"""美股信号扫描器

策略说明:
- B1: KDJ超卖 + MACD多头 + 趋势线多头 + 综合评分
- B2: MACD多头 + 趋势线多头 + 评分（不要求J值低）
- BLK: 暴力K线（大阳线 > 5%）+ 放量
- MACD_CROSS: MACD金叉 + 量能配合
- RSI_OVERSOLD: RSI超卖反弹（RSI<30后回升）
- BOLLINGER: 布林带下轨突破反弹
- S1卖出: 高位放量阴线
- TRAILING: 移动止盈（从高点回撤超过阈值）
"""
import sys
import os
import numpy as np
import pandas as pd
import duckdb
import logging
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / 'data' / 'us_stock.duckdb'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DATA_DAYS = 150


def get_stock_list():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return conn.execute("SELECT symbol, name FROM stock_info WHERE is_active = TRUE").fetchdf().to_dict('records')
    finally:
        conn.close()


def get_stock_name_map() -> dict:
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = conn.execute("SELECT symbol, name FROM stock_info").fetchdf()
        return dict(zip(df['symbol'], df['name']))
    finally:
        conn.close()


def get_stock_data(symbol: str, trading_date: str) -> pd.DataFrame:
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = conn.execute("""
            SELECT symbol, trade_date, open, high, low, close, volume
            FROM daily_price
            WHERE symbol = ? AND trade_date <= ?
            ORDER BY trade_date DESC LIMIT ?
        """, [symbol, trading_date, DATA_DAYS]).fetchdf()
        if df is None or len(df) < 60:
            return None
        return df.sort_values('trade_date').reset_index(drop=True)
    finally:
        conn.close()


def calculate_indicators(df: pd.DataFrame) -> dict:
    """计算技术指标"""
    close = df['close'].values.astype(float)
    high = df['high'].values.astype(float)
    low = df['low'].values.astype(float)
    volume = df['volume'].values.astype(float)
    open_price = df['open'].values.astype(float)

    # EMA
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    dif = ema12 - ema26
    dea = _ema(dif, 9)
    macd = (dif - dea) * 2

    # KDJ
    k, d, j = _kdj(high, low, close)

    # RSI
    rsi6 = _rsi(close, 6)
    rsi14 = _rsi(close, 14)

    # Bollinger Bands
    ma20 = _ma(close, 20)
    std20 = pd.Series(close).rolling(20).std().values
    boll_upper = ma20 + 2 * std20
    boll_lower = ma20 - 2 * std20

    # 趋势线
    ma5 = _ma(close, 5)
    ma10 = _ma(close, 10)
    ma20_val = ma20[-1] if len(ma20) > 0 else 0
    ma60 = _ma(close, 60)

    # Volume MA
    vol_ma5 = _ma(volume, 5)
    vol_ma20 = _ma(volume, 20)

    return {
        'symbol': df['symbol'].iloc[-1],
        'close': close[-1],
        'open': open_price[-1],
        'high': high[-1],
        'low': low[-1],
        'volume': volume[-1],
        'prev_close': close[-2] if len(close) > 1 else close[-1],
        'close_arr': close,
        'high_arr': high,
        'low_arr': low,
        'open_arr': open_price,
        'volume_arr': volume,
        'dif': dif[-1],
        'dea': dea[-1],
        'macd': macd[-1],
        'dif_prev': dif[-2] if len(dif) > 1 else 0,
        'dea_prev': dea[-2] if len(dea) > 1 else 0,
        'k': k[-1], 'd': d[-1], 'j': j[-1],
        'rsi6': rsi6[-1],
        'rsi14': rsi14[-1],
        'rsi14_prev': rsi14[-2] if len(rsi14) > 1 else 50,
        'ma5': ma5[-1],
        'ma10': ma10[-1],
        'ma20': ma20_val,
        'ma60': ma60[-1] if len(ma60) > 0 else 0,
        'boll_upper': boll_upper[-1] if len(boll_upper) > 0 else 0,
        'boll_lower': boll_lower[-1] if len(boll_lower) > 0 else 0,
        'vol_ma5': vol_ma5[-1] if len(vol_ma5) > 0 else 0,
        'vol_ma20': vol_ma20[-1] if len(vol_ma20) > 0 else 0,
        'pct_chg': (close[-1] - close[-2]) / close[-2] * 100 if len(close) > 1 else 0,
    }


def _ema(data, period):
    result = np.zeros_like(data, dtype=float)
    result[0] = data[0]
    multiplier = 2.0 / (period + 1)
    for i in range(1, len(data)):
        result[i] = data[i] * multiplier + result[i-1] * (1 - multiplier)
    return result


def _ma(data, period):
    return pd.Series(data).rolling(period).mean().values


def _rsi(close, period):
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean().values
    avg_loss = pd.Series(loss).rolling(period).mean().values
    rs = np.where(avg_loss != 0, avg_gain / avg_loss, 100)
    return 100 - (100 / (1 + rs))


def _kdj(high, low, close, n=9, m1=3, m2=3):
    length = len(close)
    k = np.full(length, 50.0)
    d = np.full(length, 50.0)
    j = np.full(length, 50.0)
    for i in range(n-1, length):
        hn = np.max(high[i-n+1:i+1])
        ln = np.min(low[i-n+1:i+1])
        rsv = (close[i] - ln) / (hn - ln) * 100 if hn != ln else 50
        k[i] = (m1-1)/m1 * k[i-1] + 1/m1 * rsv
        d[i] = (m2-1)/m2 * d[i-1] + 1/m2 * k[i]
        j[i] = 3 * k[i] - 2 * d[i]
    return k, d, j


# ==================== 买入策略 ====================

def signal_b1(ind: dict) -> tuple:
    """B1: J<20 + MACD多头 + MA5>MA20 + 评分"""
    score = 0
    if ind['j'] < 20: score += 3
    if ind['dif'] > 0: score += 2
    if ind['ma5'] > ind['ma20']: score += 2
    if ind['rsi14'] < 40: score += 1.5
    if ind['volume'] > ind['vol_ma5'] * 1.2: score += 1.5
    return score, (ind['j'] < 20 and ind['dif'] > 0 and ind['ma5'] > ind['ma20'] and score >= 7)


def signal_b2(ind: dict) -> tuple:
    """B2: MACD多头 + 趋势多头 + 评分（不要求J低）"""
    score = 0
    if ind['dif'] > 0: score += 2
    if ind['ma5'] > ind['ma10'] > ind['ma20']: score += 3
    if ind['close'] > ind['ma20']: score += 1.5
    if ind['rsi14'] < 60: score += 1
    if ind['volume'] > ind['vol_ma5']: score += 1.5
    if ind['pct_chg'] > 0: score += 1
    return score, (ind['dif'] > 0 and ind['ma5'] > ind['ma20'] and score >= 8)


def signal_blk(ind: dict) -> tuple:
    """BLK: 暴力K线（大阳线>5%）+ 放量"""
    pct = ind['pct_chg']
    vol_ratio = ind['volume'] / ind['vol_ma5'] if ind['vol_ma5'] > 0 else 0
    score = 0
    if pct > 5: score += 4
    elif pct > 3: score += 2
    if vol_ratio > 2: score += 3
    elif vol_ratio > 1.5: score += 1.5
    if ind['ma5'] > ind['ma20']: score += 1.5
    buy = pct > 5 and vol_ratio > 1.5 and ind['close'] > ind['ma5']
    return score, buy


def signal_macd_cross(ind: dict) -> tuple:
    """MACD金叉: DIF上穿DEA"""
    cross = ind['dif_prev'] < ind['dea_prev'] and ind['dif'] > ind['dea']
    score = 0
    if cross: score += 5
    if ind['close'] > ind['ma20']: score += 2
    if ind['volume'] > ind['vol_ma5']: score += 1.5
    if ind['rsi14'] < 60: score += 1.5
    return score, (cross and ind['close'] > ind['ma20'] and score >= 7)


def signal_rsi_oversold(ind: dict) -> tuple:
    """RSI超卖反弹: RSI14从<30回升"""
    oversold_bounce = ind['rsi14_prev'] < 30 and ind['rsi14'] > 30
    score = 0
    if oversold_bounce: score += 5
    if ind['dif'] > ind['dea']: score += 2
    if ind['volume'] > ind['vol_ma5']: score += 1.5
    if ind['close'] > ind['open']: score += 1.5
    return score, (oversold_bounce and score >= 7)


def signal_bollinger(ind: dict) -> tuple:
    """布林带下轨反弹: 前日触下轨，今日收阳"""
    prev_close = ind['close_arr'][-2] if len(ind['close_arr']) > 1 else ind['close']
    touched_lower = prev_close <= ind['boll_lower'] * 1.01
    bounce = ind['close'] > ind['open'] and ind['pct_chg'] > 0
    score = 0
    if touched_lower: score += 4
    if bounce: score += 3
    if ind['rsi14'] < 40: score += 2
    if ind['volume'] > ind['vol_ma5']: score += 1
    return score, (touched_lower and bounce and score >= 7)


# ==================== 卖出策略 ====================

def signal_s1_sell(ind: dict) -> bool:
    """S1: 高位放量阴线"""
    close_arr = ind['close_arr']
    if len(close_arr) < 60:
        return False
    at_high = ind['high'] >= np.max(ind['high_arr'][-60:]) * 0.95
    is_bearish = ind['close'] < ind['open']
    high_volume = ind['volume'] > ind['vol_ma5'] * 1.5
    big_drop = ind['pct_chg'] < -2
    return at_high and is_bearish and high_volume and big_drop


def signal_stop_loss(ind: dict, threshold: float = -5.0) -> bool:
    """止损: 跌幅超过阈值"""
    return ind['pct_chg'] < threshold


# ==================== 主扫描逻辑 ====================

def scan_single_stock(args) -> dict:
    symbol, trading_date = args
    try:
        df = get_stock_data(symbol, trading_date)
        if df is None:
            return None

        ind = calculate_indicators(df)
        name = ''

        score_b1, buy_b1 = signal_b1(ind)
        score_b2, buy_b2 = signal_b2(ind)
        score_blk, buy_blk = signal_blk(ind)
        score_macd, buy_macd = signal_macd_cross(ind)
        score_rsi, buy_rsi = signal_rsi_oversold(ind)
        score_boll, buy_boll = signal_bollinger(ind)
        sell_s1 = signal_s1_sell(ind)
        sell_stop = signal_stop_loss(ind)

        has_signal = buy_b1 or buy_b2 or buy_blk or buy_macd or buy_rsi or buy_boll or sell_s1 or sell_stop

        return {
            'date': trading_date,
            'symbol': symbol,
            'name': name,
            'open': ind['open'],
            'high': ind['high'],
            'low': ind['low'],
            'close': ind['close'],
            'volume': int(ind['volume']),
            'pct_chg': round(ind['pct_chg'], 2),
            'score_b1': round(score_b1, 2),
            'score_b2': round(score_b2, 2),
            'score_blk': round(score_blk, 2),
            'score_macd_cross': round(score_macd, 2),
            'score_rsi_oversold': round(score_rsi, 2),
            'score_bollinger': round(score_boll, 2),
            'signal_buy_b1': buy_b1,
            'signal_buy_b2': buy_b2,
            'signal_buy_blk': buy_blk,
            'signal_buy_macd_cross': buy_macd,
            'signal_buy_rsi_oversold': buy_rsi,
            'signal_buy_bollinger': buy_boll,
            'signal_sell_s1': sell_s1,
            'signal_sell_stop_loss': sell_stop,
            'signal_sell_trailing': False,
            'indicators': None,
            'has_signal': has_signal,
        }
    except Exception as e:
        return None


def scan_signals(trading_date: str = None, workers: int = 4) -> dict:
    """扫描全市场信号"""
    if not trading_date:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        latest = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
        conn.close()
        trading_date = str(latest) if latest else datetime.now().strftime('%Y-%m-%d')

    if len(trading_date) == 8:
        trading_date = f"{trading_date[:4]}-{trading_date[4:6]}-{trading_date[6:]}"

    stocks = get_stock_list()
    logger.info(f"开始扫描 {len(stocks)} 只股票, 日期: {trading_date}")

    args_list = [(s['symbol'], trading_date) for s in stocks]

    results = []
    with Pool(processes=min(workers, cpu_count())) as pool:
        for r in pool.imap(scan_single_stock, args_list, chunksize=50):
            if r is not None:
                results.append(r)

    # 写入数据库
    if results:
        # 填充股票名称
        name_map = get_stock_name_map()
        for r in results:
            r['name'] = name_map.get(r['symbol'], '')

        df = pd.DataFrame(results)
        df = df.drop(columns=['has_signal'], errors='ignore')
        conn = duckdb.connect(str(DB_PATH))
        try:
            conn.execute(f"DELETE FROM daily_signals WHERE date = '{trading_date}'")
            conn.execute("INSERT INTO daily_signals SELECT * FROM df")
        finally:
            conn.close()

    buy_count = sum(1 for r in results if any([
        r.get('signal_buy_b1'), r.get('signal_buy_b2'), r.get('signal_buy_blk'),
        r.get('signal_buy_macd_cross'), r.get('signal_buy_rsi_oversold'), r.get('signal_buy_bollinger')
    ]))

    logger.info(f"扫描完成: {len(results)} 只有效, {buy_count} 只有买入信号")
    return {'success': len(results), 'total': len(stocks), 'buy_signals': buy_count}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()
    scan_signals(args.date, args.workers)
