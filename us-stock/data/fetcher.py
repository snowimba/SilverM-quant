"""美股数据获取器 - 基于 yfinance"""
import yfinance as yf
import pandas as pd
import duckdb
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / 'data' / 'us_stock.duckdb'


def get_us_stock_list() -> pd.DataFrame:
    """获取全美股股票列表（NYSE + NASDAQ）"""
    import requests

    # 从 NASDAQ API 获取全部上市股票
    urls = [
        "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=NYSE",
        "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=NASDAQ",
    ]
    headers = {'User-Agent': 'Mozilla/5.0'}

    all_stocks = []
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            data = resp.json()
            rows = data.get('data', {}).get('table', {}).get('rows', [])
            for row in rows:
                symbol = row.get('symbol', '').strip()
                if not symbol or '/' in symbol or '^' in symbol or len(symbol) > 5:
                    continue
                all_stocks.append({
                    'symbol': symbol,
                    'name': row.get('name', ''),
                    'sector': row.get('sector', ''),
                    'industry': row.get('industry', ''),
                    'market_cap': _parse_market_cap(row.get('marketCap', '')),
                    'exchange': 'NYSE' if 'NYSE' in url else 'NASDAQ',
                })
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")

    df = pd.DataFrame(all_stocks)
    if not df.empty:
        df = df.drop_duplicates(subset='symbol')
    logger.info(f"获取到 {len(df)} 只美股")
    return df


def _parse_market_cap(val: str) -> float:
    if not val or val == '':
        return 0
    val = val.replace('$', '').replace(',', '').strip()
    try:
        return float(val)
    except:
        return 0


def update_stock_list() -> int:
    """更新股票列表到数据库"""
    df = get_us_stock_list()
    if df.empty:
        return 0

    conn = duckdb.connect(str(DB_PATH))
    try:
        conn.execute("DELETE FROM stock_info")
        conn.execute("INSERT INTO stock_info (symbol, name, sector, industry, market_cap, exchange) SELECT symbol, name, sector, industry, market_cap, exchange FROM df")
        count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        logger.info(f"股票列表更新完成: {count} 只")
        return count
    finally:
        conn.close()


def fetch_daily_batch(symbols: list, start_date: str, end_date: str) -> pd.DataFrame:
    """批量下载日线数据（yfinance 支持批量）"""
    try:
        df = yf.download(
            symbols,
            start=start_date,
            end=end_date,
            group_by='ticker',
            auto_adjust=False,
            threads=True,
            progress=False
        )
        return df
    except Exception as e:
        logger.error(f"批量下载失败: {e}")
        return pd.DataFrame()


def update_daily_price(start_date: str = None, end_date: str = None,
                       batch_size: int = 50, progress_callback=None) -> dict:
    """更新全市场日线数据

    Args:
        start_date: YYYY-MM-DD 格式
        end_date: YYYY-MM-DD 格式
        batch_size: 每批下载股票数
        progress_callback: fn(current, total) 进度回调
    """
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = '2024-01-01'

    conn = duckdb.connect(str(DB_PATH))
    symbols = [r[0] for r in conn.execute("SELECT symbol FROM stock_info WHERE is_active = TRUE").fetchall()]
    conn.close()

    if not symbols:
        logger.warning("股票列表为空，请先更新股票列表")
        return {'success': 0, 'fail': 0, 'records': 0}

    total = len(symbols)
    logger.info(f"开始下载日线数据: {total} 只股票, {start_date} ~ {end_date}")

    total_records = 0
    success_count = 0
    fail_count = 0

    for i in range(0, total, batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = i // batch_size + 1

        try:
            raw_df = fetch_daily_batch(batch, start_date, end_date)
            if raw_df.empty:
                fail_count += len(batch)
                continue

            records = _process_and_save_batch(raw_df, batch)
            total_records += records
            success_count += len(batch)

        except Exception as e:
            logger.error(f"批次 {batch_num} 失败: {e}")
            fail_count += len(batch)

        processed = min(i + batch_size, total)
        if processed % 100 == 0 or processed == total:
            logger.info(f"下载进度: {processed}/{total}")
        if progress_callback:
            progress_callback(processed, total)

    logger.info(f"日线数据更新完成: 成功{success_count}, 失败{fail_count}, 记录{total_records}")
    return {'success': success_count, 'fail': fail_count, 'records': total_records}


def _process_and_save_batch(raw_df: pd.DataFrame, symbols: list) -> int:
    """处理 yfinance 返回的批量数据并写入数据库"""
    all_rows = []

    for symbol in symbols:
        try:
            if len(symbols) == 1:
                df = raw_df.copy()
            else:
                if symbol not in raw_df.columns.get_level_values(0):
                    continue
                df = raw_df[symbol].copy()

            if df.empty or df.dropna(how='all').empty:
                continue

            df = df.dropna(subset=['Close'])
            df = df.reset_index()
            df.columns = [c.lower() if isinstance(c, str) else c for c in df.columns]

            if 'date' in df.columns:
                date_col = 'date'
            elif 'datetime' in df.columns:
                date_col = 'datetime'
            else:
                continue

            rows = pd.DataFrame({
                'symbol': symbol,
                'trade_date': pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d'),
                'open': pd.to_numeric(df.get('open'), errors='coerce'),
                'high': pd.to_numeric(df.get('high'), errors='coerce'),
                'low': pd.to_numeric(df.get('low'), errors='coerce'),
                'close': pd.to_numeric(df.get('close'), errors='coerce'),
                'volume': pd.to_numeric(df.get('volume'), errors='coerce').astype('Int64'),
                'adj_close': pd.to_numeric(df.get('adj close', df.get('adjclose')), errors='coerce'),
                'pct_chg': None,
            })
            rows['pct_chg'] = rows['close'].pct_change() * 100
            rows = rows.dropna(subset=['close'])
            all_rows.append(rows)
        except Exception:
            continue

    if not all_rows:
        return 0

    combined = pd.concat(all_rows, ignore_index=True)

    conn = duckdb.connect(str(DB_PATH))
    try:
        conn.execute("CREATE TEMPORARY TABLE temp_batch AS SELECT * FROM combined")
        conn.execute("""
            DELETE FROM daily_price WHERE EXISTS (
                SELECT 1 FROM temp_batch
                WHERE daily_price.symbol = temp_batch.symbol
                AND daily_price.trade_date = temp_batch.trade_date
            )
        """)
        conn.execute("INSERT INTO daily_price SELECT * FROM temp_batch")
        conn.execute("DROP TABLE temp_batch")
        return len(combined)
    finally:
        conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("更新股票列表...")
    count = update_stock_list()
    print(f"股票列表: {count} 只")
    print("下载日线数据...")
    result = update_daily_price(start_date='2024-06-01')
    print(f"结果: {result}")
