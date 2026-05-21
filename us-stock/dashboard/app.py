"""美股量化系统 - Web Dashboard"""
import os
import sys
import threading
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / 'data' / 'us_stock.duckdb'
FRONTEND_DIST = PROJECT_ROOT / 'frontend' / 'dist'

app = Flask(__name__)
CORS(app)

update_tasks = {}


def get_db():
    return duckdb.connect(str(DB_PATH), read_only=True)


# ==================== 页面路由 ====================

@app.route('/')
def index():
    if (FRONTEND_DIST / 'index.html').exists():
        return send_from_directory(str(FRONTEND_DIST), 'index.html')
    return jsonify({'message': 'US Stock Quant System API', 'status': 'running'})


@app.route('/<path:filename>')
def static_files(filename):
    if filename.startswith('api/'):
        return jsonify({'error': 'Not found'}), 404
    file_path = FRONTEND_DIST / filename
    if file_path.exists() and not file_path.is_dir():
        return send_from_directory(str(FRONTEND_DIST), filename)
    if (FRONTEND_DIST / 'index.html').exists():
        return send_from_directory(str(FRONTEND_DIST), 'index.html')
    return jsonify({'error': 'Not found'}), 404


# ==================== API: 概览 ====================

@app.route('/api/stats')
def api_stats():
    db = get_db()
    try:
        stock_count = db.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        price_count = db.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
        latest_date = db.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
        signal_date = db.execute("SELECT MAX(date) FROM daily_signals").fetchone()[0]

        buy_signals = 0
        if signal_date:
            buy_signals = db.execute("""
                SELECT COUNT(*) FROM daily_signals WHERE date = ?
                AND (signal_buy_b1 OR signal_buy_b2 OR signal_buy_blk
                     OR signal_buy_macd_cross OR signal_buy_rsi_oversold OR signal_buy_bollinger)
            """, [signal_date]).fetchone()[0]

        return jsonify({
            'stock_count': stock_count,
            'price_records': price_count,
            'latest_date': str(latest_date) if latest_date else None,
            'signal_date': str(signal_date) if signal_date else None,
            'buy_signals': buy_signals,
        })
    finally:
        db.close()


# ==================== API: 信号 ====================

@app.route('/api/signals')
def api_signals():
    db = get_db()
    try:
        date = request.args.get('date')
        if not date:
            date = db.execute("SELECT MAX(date) FROM daily_signals").fetchone()[0]
        if not date:
            return jsonify({'date': None, 'signals': [], 'buy_count': 0, 'sell_count': 0})

        df = db.execute("""
            SELECT * FROM daily_signals WHERE date = ?
            AND (signal_buy_b1 OR signal_buy_b2 OR signal_buy_blk
                 OR signal_buy_macd_cross OR signal_buy_rsi_oversold OR signal_buy_bollinger
                 OR signal_sell_s1 OR signal_sell_stop_loss)
            ORDER BY score_b1 + score_b2 + score_blk + score_macd_cross + score_rsi_oversold + score_bollinger DESC
        """, [str(date)]).fetchdf()

        signals = []
        for _, row in df.iterrows():
            buy_signals = []
            if row.get('signal_buy_b1'): buy_signals.append({'strategy': 'B1', 'score': row['score_b1']})
            if row.get('signal_buy_b2'): buy_signals.append({'strategy': 'B2', 'score': row['score_b2']})
            if row.get('signal_buy_blk'): buy_signals.append({'strategy': 'BLK', 'score': row['score_blk']})
            if row.get('signal_buy_macd_cross'): buy_signals.append({'strategy': 'MACD金叉', 'score': row['score_macd_cross']})
            if row.get('signal_buy_rsi_oversold'): buy_signals.append({'strategy': 'RSI超卖', 'score': row['score_rsi_oversold']})
            if row.get('signal_buy_bollinger'): buy_signals.append({'strategy': '布林反弹', 'score': row['score_bollinger']})

            sell_signals = []
            if row.get('signal_sell_s1'): sell_signals.append({'strategy': '高位放量阴线', 'score': 0})
            if row.get('signal_sell_stop_loss'): sell_signals.append({'strategy': '止损', 'score': 0})

            signals.append({
                'symbol': row['symbol'],
                'code': row['symbol'],
                'name': row.get('name', '') or row['symbol'],
                'close': float(row['close']) if pd.notna(row['close']) else 0,
                'pct_chg': float(row['pct_chg']) if pd.notna(row['pct_chg']) else 0,
                'change_pct': float(row['pct_chg']) if pd.notna(row['pct_chg']) else 0,
                'volume': int(row['volume']) if pd.notna(row['volume']) else 0,
                'buy_signals': buy_signals,
                'sell_signals': sell_signals,
            })

        buy_count = sum(1 for s in signals if s['buy_signals'])
        sell_count = sum(1 for s in signals if s['sell_signals'])

        return jsonify({
            'date': str(date),
            'signals': signals,
            'buy_count': buy_count,
            'sell_count': sell_count,
        })
    finally:
        db.close()


# ==================== API: 数据更新 ====================

@app.route('/api/data-update/status')
def api_data_status():
    db = get_db()
    try:
        stock_count = db.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        price_count = db.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
        latest = db.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
        distinct_stocks = db.execute("SELECT COUNT(DISTINCT symbol) FROM daily_price").fetchone()[0]
        return jsonify({
            'success': True,
            'data': {
                'stock_info': {'count': stock_count},
                'daily_price': {'count': price_count, 'latest': str(latest) if latest else None, 'stocks': distinct_stocks},
            }
        })
    finally:
        db.close()


@app.route('/api/data-update/update', methods=['POST'])
def api_trigger_update():
    data = request.get_json() or {}
    data_type = data.get('data_type', 'daily')
    start_date = data.get('start_date', '2024-01-01')
    end_date = data.get('end_date')

    task_id = f"update_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    update_tasks[task_id] = {'status': 'pending', 'progress': 0, 'message': '准备中...'}

    thread = threading.Thread(target=_run_update, args=(task_id, data_type, start_date, end_date))
    thread.daemon = True
    thread.start()

    return jsonify({'success': True, 'task_id': task_id})


def _run_update(task_id, data_type, start_date, end_date):
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from data.fetcher import update_stock_list, update_daily_price

        if data_type in ('all', 'stock_info'):
            update_tasks[task_id] = {'status': 'running', 'progress': 5, 'message': '更新股票列表...'}
            count = update_stock_list()
            update_tasks[task_id]['message'] = f'股票列表: {count} 只'

        if data_type in ('all', 'daily'):
            def on_progress(current, total):
                pct = int(current / total * 90) + 5
                update_tasks[task_id] = {'status': 'running', 'progress': pct, 'message': f'下载日线 {current}/{total}...'}

            update_tasks[task_id] = {'status': 'running', 'progress': 10, 'message': '下载日线数据...'}
            result = update_daily_price(start_date=start_date, end_date=end_date, progress_callback=on_progress)
            update_tasks[task_id] = {
                'status': 'completed', 'progress': 100,
                'message': f'完成: {result["records"]:,} 条记录',
                'result': result
            }
    except Exception as e:
        update_tasks[task_id] = {'status': 'error', 'progress': 0, 'message': f'失败: {str(e)}'}


@app.route('/api/data-update/task/<task_id>')
def api_task_status(task_id):
    if task_id in update_tasks:
        return jsonify({'success': True, 'data': update_tasks[task_id]})
    return jsonify({'success': False, 'error': '任务不存在'}), 404


@app.route('/api/data-update/scan-signals', methods=['POST'])
def api_scan_signals():
    data = request.get_json() or {}
    workers = data.get('workers', 4)

    task_id = f"scan_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    update_tasks[task_id] = {'status': 'pending', 'progress': 0, 'message': '准备扫描...'}

    thread = threading.Thread(target=_run_scan, args=(task_id, workers))
    thread.daemon = True
    thread.start()

    return jsonify({'success': True, 'task_id': task_id})


def _run_scan(task_id, workers):
    try:
        update_tasks[task_id] = {'status': 'running', 'progress': 10, 'message': '扫描全市场信号...'}

        result = subprocess.run(
            [sys.executable, 'signals/scanner.py', '--workers', str(workers)],
            capture_output=True, text=True, timeout=600, cwd=str(PROJECT_ROOT)
        )

        if result.returncode == 0:
            import re
            m = re.search(r'(\d+) 只有买入信号', result.stderr + result.stdout)
            buy_count = m.group(1) if m else '?'
            update_tasks[task_id] = {'status': 'completed', 'progress': 100, 'message': f'扫描完成, {buy_count} 只有买入信号'}
        else:
            update_tasks[task_id] = {'status': 'error', 'progress': 0, 'message': f'失败: {result.stderr[-200:]}'}
    except Exception as e:
        update_tasks[task_id] = {'status': 'error', 'progress': 0, 'message': f'失败: {str(e)}'}


# ==================== API: 多信号共振 ====================

@app.route('/api/multi-signal')
def api_multi_signal():
    db = get_db()
    try:
        date = request.args.get('date')
        if not date:
            date = db.execute("SELECT MAX(date) FROM daily_signals").fetchone()[0]
        if not date:
            return jsonify({'date': None, 'stocks': []})

        df = db.execute("""
            SELECT symbol, name, close, pct_chg,
                signal_buy_b1, signal_buy_b2, signal_buy_blk,
                signal_buy_macd_cross, signal_buy_rsi_oversold, signal_buy_bollinger
            FROM daily_signals WHERE date = ?
            AND (CAST(signal_buy_b1 AS INT) + CAST(signal_buy_b2 AS INT) + CAST(signal_buy_blk AS INT)
                 + CAST(signal_buy_macd_cross AS INT) + CAST(signal_buy_rsi_oversold AS INT)
                 + CAST(signal_buy_bollinger AS INT)) >= 2
            ORDER BY (CAST(signal_buy_b1 AS INT) + CAST(signal_buy_b2 AS INT) + CAST(signal_buy_blk AS INT)
                 + CAST(signal_buy_macd_cross AS INT) + CAST(signal_buy_rsi_oversold AS INT)
                 + CAST(signal_buy_bollinger AS INT)) DESC
        """, [str(date)]).fetchdf()

        strategy_names = ['B1', 'B2', 'BLK', 'MACD金叉', 'RSI超卖', '布林反弹']
        stocks = []
        for _, row in df.iterrows():
            signals = [strategy_names[i] for i, col in enumerate([
                'signal_buy_b1', 'signal_buy_b2', 'signal_buy_blk',
                'signal_buy_macd_cross', 'signal_buy_rsi_oversold', 'signal_buy_bollinger'
            ]) if row.get(col)]
            stocks.append({
                'symbol': row['symbol'],
                'name': row.get('name', ''),
                'close': float(row['close']) if pd.notna(row['close']) else 0,
                'pct_chg': float(row['pct_chg']) if pd.notna(row['pct_chg']) else 0,
                'signal_count': len(signals),
                'signals': signals,
            })

        return jsonify({'date': str(date), 'stocks': stocks, 'count': len(stocks)})
    finally:
        db.close()


# ==================== API: Agent 分析 ====================

_analysis_history = []


@app.route('/api/agent/health')
def api_agent_health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat(), 'components': {'database': 'ok', 'llm': 'ok'}})


@app.route('/api/agent/analyze', methods=['POST'])
def api_agent_analyze():
    data = request.get_json() or {}
    stock_symbol = data.get('symbol', '').upper()
    trade_date = data.get('trade_date', datetime.now().strftime('%Y-%m-%d'))

    if not stock_symbol:
        return jsonify({'success': False, 'error': '缺少symbol参数'}), 400

    try:
        # A股项目根目录必须在最前面，agent_integration 依赖其 database 模块
        a_stock_root = str(PROJECT_ROOT.parent)
        if a_stock_root not in sys.path:
            sys.path.insert(0, a_stock_root)
        # 临时切换工作目录，避免 database 模块冲突
        import os
        old_cwd = os.getcwd()
        os.chdir(a_stock_root)
        try:
            from agent_integration.api.analyzer import analyze_stock
            result = analyze_stock(stock_symbol, trade_date)
        finally:
            os.chdir(old_cwd)

        _analysis_history.insert(0, {
            'symbol': stock_symbol,
            'trade_date': trade_date,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'result': result,
        })
        if len(_analysis_history) > 50:
            _analysis_history.pop()

        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/agent/history')
def api_agent_history():
    limit = request.args.get('limit', 20, type=int)
    return jsonify({'success': True, 'data': _analysis_history[:limit]})


if __name__ == '__main__':
    from database import init_database
    init_database()
    app.run(host='0.0.0.0', port=5002, debug=False)
