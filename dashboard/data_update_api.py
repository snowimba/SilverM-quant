"""
Flask API - 数据更新接口
"""
import sys
import os
from flask import Blueprint, request, jsonify
from datetime import datetime
import threading
import subprocess
import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.updaters.fetcher_dwd import DWDFetcher
from database.db_manager import DatabaseManager

data_update_bp = Blueprint('data_update', __name__, url_prefix='/api/data-update')

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_log_path(date_str: str = None) -> str:
    """获取动态日志路径"""
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    log_dir = os.path.join(PROJECT_ROOT, 'logs', 'pipeline')
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f'fetcher_dwd_{date_str}.log')

update_tasks = {}


def run_update_task(task_id: str, data_type: str, start_date: str = None, end_date: str = None,
                   ts_code: str = None, index_code: str = None, workers: int = 4, source: str = 'tushare'):
    """后台运行更新任务"""
    try:
        update_tasks[task_id] = {'status': 'running', 'progress': 0, 'message': '开始更新...'}

        # baostock 需要在当前线程重新登录（全局 socket 不是线程安全的）
        if source == 'baostock':
            import baostock as bs
            from data.fetchers.baostock_adapter.base import BaostockBaseFetcher
            BaostockBaseFetcher._logged_in = False
            bs.login()
            BaostockBaseFetcher._logged_in = True

        fetcher = DWDFetcher(source=source)
        result = None
        
        if data_type == 'daily':
            if not start_date:
                start_date = '20240101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if source == 'baostock':
                def on_progress(current, total):
                    pct = int(current / total * 90)
                    update_tasks[task_id] = {'status': 'running', 'progress': pct, 'message': f'下载日线数据 {current}/{total}...'}
                result = fetcher.update_daily_by_stock(start_date, end_date, num_workers=workers, progress_callback=on_progress)
            else:
                result = fetcher.update_daily(start_date, end_date)
            
        elif data_type == 'daily_parallel':
            if not start_date:
                start_date = '20240101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if source == 'baostock':
                result = fetcher.update_daily_by_stock(start_date, end_date, num_workers=1)
            else:
                result = fetcher.update_daily_parallel(start_date, end_date, num_workers=workers)
            
        elif data_type == 'daily_basic':
            if source == 'baostock':
                update_tasks[task_id] = {'status': 'error', 'message': 'daily_basic 仅支持 tushare 数据源，请切换到 Tushare 或配置 TUSHARE_TOKEN'}
                return
            if not start_date:
                start_date = '20240101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            result = fetcher.update_daily_basic(start_date, end_date)

        elif data_type == 'adj_factor':
            if source == 'baostock':
                update_tasks[task_id] = {'status': 'error', 'message': 'adj_factor 仅支持 tushare 数据源，请切换到 Tushare 或配置 TUSHARE_TOKEN'}
                return
            if not start_date:
                start_date = '20200101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            result = fetcher.update_adj_factor(start_date, end_date)
            
        elif data_type == 'income':
            if ts_code:
                result = fetcher.update_income(ts_code)
            else:
                update_tasks[task_id] = {'status': 'error', 'message': '缺少ts_code参数'}
                return
                
        elif data_type == 'balancesheet':
            if ts_code:
                result = fetcher.update_balancesheet(ts_code)
            else:
                update_tasks[task_id] = {'status': 'error', 'message': '缺少ts_code参数'}
                return
                
        elif data_type == 'cashflow':
            if ts_code:
                result = fetcher.update_cashflow(ts_code)
            else:
                update_tasks[task_id] = {'status': 'error', 'message': '缺少ts_code参数'}
                return
                
        elif data_type == 'financial':
            result = fetcher.update_financial_multiprocess(num_workers=workers)
            
        elif data_type == 'index':
            if not index_code:
                index_code = '000001.SH'
            if not start_date:
                start_date = '20240101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            result = fetcher.update_index(index_code, start_date, end_date)
            
        elif data_type == 'stock_info':
            result = fetcher.update_stock_info(source=source)
            
        elif data_type == 'trade_calendar':
            if source == 'baostock':
                update_tasks[task_id] = {'status': 'error', 'message': 'trade_calendar 仅支持 tushare 数据源，请切换到 Tushare 或配置 TUSHARE_TOKEN'}
                return
            if not start_date:
                start_date = '20200101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            result = fetcher.update_trade_calendar(start_date, end_date)
            
        elif data_type == 'all':
            # 全量更新
            if not start_date:
                start_date = '20240101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')

            total_records = 0
            results = {}

            if source == 'baostock':
                # baostock 模式：只更新支持的数据
                # 1. 股票信息
                update_tasks[task_id] = {'status': 'running', 'progress': 20, 'message': '更新股票信息(baostock)...'}
                r = fetcher.update_stock_info(source='baostock')
                results['stock_info'] = r
                total_records += r.get('records', 0)

                # 2. 日线数据
                update_tasks[task_id] = {'status': 'running', 'progress': 50, 'message': '更新日线数据(baostock)...'}
                r = fetcher.update_daily_by_stock(start_date, end_date, num_workers=1)
                results['daily'] = r
                total_records += r.get('records', 0)

                results['skipped'] = ['trade_calendar', 'daily_basic', 'adj_factor', 'index', 'financial']
            else:
                # tushare 模式：全量更新
                # 1. 交易日历
                update_tasks[task_id] = {'status': 'running', 'progress': 10, 'message': '更新交易日历...'}
                r = fetcher.update_trade_calendar(start_date, end_date)
                results['trade_calendar'] = r
                total_records += r.get('records', 0)

                # 2. 股票信息
                update_tasks[task_id] = {'status': 'running', 'progress': 20, 'message': '更新股票信息...'}
                r = fetcher.update_stock_info()
                results['stock_info'] = r
                total_records += r.get('records', 0)

                # 3. 日线数据
                update_tasks[task_id] = {'status': 'running', 'progress': 40, 'message': '更新日线数据...'}
                r = fetcher.update_daily(start_date, end_date)
                results['daily'] = r
                total_records += r.get('records', 0)

                # 4. 每日指标
                update_tasks[task_id] = {'status': 'running', 'progress': 60, 'message': '更新每日指标...'}
                r = fetcher.update_daily_basic(start_date, end_date)
                results['daily_basic'] = r
                total_records += r.get('records', 0)

                # 5. 复权因子
                update_tasks[task_id] = {'status': 'running', 'progress': 70, 'message': '更新复权因子...'}
                r = fetcher.update_adj_factor(start_date, end_date)
                results['adj_factor'] = r
                total_records += r.get('records', 0)

                # 6. 指数数据
                update_tasks[task_id] = {'status': 'running', 'progress': 80, 'message': '更新指数数据...'}
                idx_records = 0
                for idx in fetcher.DEFAULT_INDICES:
                    r = fetcher.update_index(idx, start_date, end_date)
                    idx_records += r.get('records', 0)
                results['index'] = {'records': idx_records}
                total_records += idx_records

                # 7. 财务数据
                update_tasks[task_id] = {'status': 'running', 'progress': 90, 'message': '更新财务数据...'}
                r = fetcher.update_financial_multiprocess(num_workers=workers)
                results['financial'] = r
                total_records += r.get('income_records', 0) + r.get('balancesheet_records', 0) + r.get('cashflow_records', 0)

            result = {'records': total_records, 'results': results}
        
        update_tasks[task_id] = {
            'status': 'completed',
            'progress': 100,
            'message': '更新完成',
            'result': result
        }
        
    except Exception as e:
        update_tasks[task_id] = {
            'status': 'error',
            'progress': 0,
            'message': f'更新失败: {str(e)}'
        }


@data_update_bp.route('/status', methods=['GET'])
def get_status():
    """获取所有DWD表状态
    
    Returns: JSON格式表状态列表
    """
    try:
        fetcher = DWDFetcher()
        
        # 获取各表的最新日期和记录数
        db = DatabaseManager(fetcher.db_path).conn.cursor()
        try:
            tables_status = {}
            
            # 查询各表状态
            table_queries = {
                'dwd_daily_price': "SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM dwd_daily_price",
                'dwd_daily_basic': "SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM dwd_daily_basic",
                'dwd_adj_factor': "SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM dwd_adj_factor",
                'dwd_income': "SELECT COUNT(*) as cnt, MAX(end_date) as latest FROM dwd_income",
                'dwd_balancesheet': "SELECT COUNT(*) as cnt, MAX(end_date) as latest FROM dwd_balancesheet",
                'dwd_cashflow': "SELECT COUNT(*) as cnt, MAX(end_date) as latest FROM dwd_cashflow",
                'dwd_index_daily': "SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM dwd_index_daily",
                'dwd_stock_info': "SELECT COUNT(*) as cnt, MAX(list_date) as latest FROM dwd_stock_info",
                'dwd_trade_calendar': "SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM dwd_trade_calendar WHERE is_open = TRUE",
            }
            
            for table, query in table_queries.items():
                try:
                    result = db.execute(query).fetchone()
                    tables_status[table] = {
                        'count': result[0] if result else 0,
                        'latest': result[1].strftime('%Y-%m-%d') if result and result[1] else None
                    }
                except Exception as e:
                    tables_status[table] = {'count': 0, 'latest': None, 'error': str(e)}
            
            return jsonify({
                'success': True,
                'data': tables_status,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        finally:
            db.close()
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@data_update_bp.route('/update', methods=['POST'])
def trigger_update():
    """触发数据更新
    
    POST /api/data-update/update
    Body: {
        "data_type": "daily",
        "start_date": "20240101",  // 可选
        "end_date": "20260406",    // 可选
        "ts_code": "600519.SH",     // 财务数据必需
        "index_code": "000001.SH", // 指数数据可选
        "workers": 4               // 并行进程数
    }
    
    Returns: JSON格式任务ID
    """
    try:
        data = request.get_json() or {}
        
        data_type = data.get('data_type')
        if not data_type:
            return jsonify({'success': False, 'error': '缺少data_type参数'}), 400
        
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        ts_code = data.get('ts_code')
        index_code = data.get('index_code')
        workers = data.get('workers', 4)
        source = data.get('source', 'tushare')
        
        # 生成任务ID
        task_id = f"update_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # 后台启动更新任务
        thread = threading.Thread(
            target=run_update_task,
            args=(task_id, data_type, start_date, end_date, ts_code, index_code, workers, source)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': f'更新任务已启动: {data_type}'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@data_update_bp.route('/task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """获取更新任务状态
    
    GET /api/data-update/task/{task_id}
    
    Returns: JSON格式任务状态
    """
    if task_id in update_tasks:
        return jsonify({
            'success': True,
            'data': update_tasks[task_id]
        })
    else:
        return jsonify({
            'success': False,
            'error': '任务不存在或已过期'
        }), 404


@data_update_bp.route('/log', methods=['GET'])
def get_update_log():
    """获取更新日志最后N行
    
    GET /api/data-update/log?lines=100&date=20260407
    
    Returns: JSON格式日志内容
    """
    try:
        lines = int(request.args.get('lines', 100))
        date_str = request.args.get('date', datetime.now().strftime('%Y%m%d'))
        log_path = get_log_path(date_str)
        
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                log_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
                content = ''.join(log_lines)
        else:
            content = f'日志文件不存在: {log_path}'
        
        return jsonify({
            'success': True,
            'log': content,
            'lines': len(log_lines) if os.path.exists(log_path) else 0,
            'log_path': log_path
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@data_update_bp.route('/indices', methods=['GET'])
def get_default_indices():
    """获取默认指数列表
    
    GET /api/data-update/indices
    
    Returns: JSON格式指数列表
    """
    return jsonify({
        'success': True,
        'data': DWDFetcher.DEFAULT_INDICES
    })


@data_update_bp.route('/calendar/<table_name>', methods=['GET'])
def get_table_calendar(table_name):
    """获取指定表的数据日历统计"""
    try:
        date_col_map = {
            'dwd_daily_price': 'trade_date',
            'dwd_daily_basic': 'trade_date',
            'dwd_adj_factor': 'trade_date',
            'dwd_income': 'end_date',
            'dwd_balancesheet': 'end_date',
            'dwd_cashflow': 'end_date',
            'dwd_index_daily': 'trade_date',
            'dwd_trade_calendar': 'trade_date',
            'dwd_stock_info': 'list_date',
        }
        
        date_col = date_col_map.get(table_name, 'trade_date')
        
        fetcher = DWDFetcher()
        db = DatabaseManager(fetcher.db_path).conn.cursor()
        
        try:
            result = db.execute(f"""
                SELECT {date_col}, COUNT(*) as cnt 
                FROM {table_name} 
                GROUP BY {date_col} 
                ORDER BY {date_col}
            """).fetchall()
            
            year_stats = {}
            month_stats = {}
            day_stats = {}
            
            for row in result:
                date_str = row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0])
                cnt = row[1]
                
                year = date_str[:4]
                month = date_str[:7]
                
                if year not in year_stats:
                    year_stats[year] = 0
                year_stats[year] += cnt
                
                if month not in month_stats:
                    month_stats[month] = 0
                month_stats[month] += cnt
                
                day_stats[date_str] = cnt
            
            table_names = {
                'dwd_daily_price': '日线行情',
                'dwd_daily_basic': '每日指标',
                'dwd_adj_factor': '复权因子',
                'dwd_income': '利润表',
                'dwd_balancesheet': '资产负债表',
                'dwd_cashflow': '现金流量表',
                'dwd_index_daily': '指数日线',
                'dwd_stock_info': '股票基础信息',
                'dwd_trade_calendar': '交易日历',
            }
            
            return jsonify({
                'success': True,
                'data': {
                    'table_name': table_name,
                    'table_desc': table_names.get(table_name, table_name),
                    'date_col': date_col,
                    'year_stats': [{'period': k, 'count': v} for k, v in sorted(year_stats.items(), reverse=True)],
                    'month_stats': [{'period': k, 'count': v} for k, v in sorted(month_stats.items(), reverse=True)],
                    'day_stats': [{'period': k, 'count': v} for k, v in sorted(day_stats.items(), reverse=True)],
                }
            })
        finally:
            db.close()

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@data_update_bp.route('/scan-signals', methods=['POST'])
def trigger_scan_signals():
    """触发信号扫描"""
    try:
        data = request.get_json() or {}
        date = data.get('date')
        workers = data.get('workers', 4)

        task_id = f"scan_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        update_tasks[task_id] = {'status': 'pending', 'progress': 0, 'message': '准备扫描信号...'}

        thread = threading.Thread(
            target=run_scan_signals_task,
            args=(task_id, date, workers)
        )
        thread.daemon = True
        thread.start()

        return jsonify({'success': True, 'task_id': task_id, 'message': '信号扫描已启动'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def run_scan_signals_task(task_id: str, date: str = None, workers: int = 4):
    """后台运行信号扫描（通过子进程，临时释放DB锁）"""
    try:
        update_tasks[task_id] = {'status': 'running', 'progress': 10, 'message': '正在扫描全市场信号...'}

        # 临时关闭主进程的 DB 连接，让子进程能访问
        from database.db_manager import DatabaseManager
        db_mgr = DatabaseManager()
        db_mgr.conn.close()

        import subprocess
        cmd = ['python', 'signals/scan_signals_v2.py', '--workers', str(workers)]
        if date:
            cmd.extend(['--date', date])

        result = subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=600,
            cwd=PROJECT_ROOT
        )

        # 重新打开 DB 连接
        import duckdb
        db_mgr.conn = duckdb.connect(str(db_mgr.db_path))

        if result.returncode == 0:
            output = result.stdout + result.stderr
            import re
            m = re.search(r'成功[：:]\s*(\d+)', output)
            success_count = m.group(1) if m else '?'
            update_tasks[task_id] = {
                'status': 'completed',
                'progress': 100,
                'message': f'扫描完成: {success_count} 只股票'
            }
        else:
            update_tasks[task_id] = {
                'status': 'error',
                'progress': 0,
                'message': f'扫描失败: {result.stderr[-200:] if result.stderr else "未知错误"}'
            }
    except subprocess.TimeoutExpired:
        update_tasks[task_id] = {
            'status': 'error',
            'progress': 0,
            'message': '扫描超时（超过10分钟）'
        }
    except Exception as e:
        update_tasks[task_id] = {
            'status': 'error',
            'progress': 0,
            'message': f'扫描失败: {str(e)}'
        }