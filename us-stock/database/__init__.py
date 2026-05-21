"""美股数据库初始化"""
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / 'data' / 'us_stock.duckdb'


def init_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    conn.execute("""
    CREATE TABLE IF NOT EXISTS stock_info (
        symbol VARCHAR PRIMARY KEY,
        name VARCHAR,
        sector VARCHAR,
        industry VARCHAR,
        market_cap DOUBLE,
        exchange VARCHAR,
        is_active BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS daily_price (
        symbol VARCHAR NOT NULL,
        trade_date DATE NOT NULL,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        adj_close DOUBLE,
        pct_chg DOUBLE,
        PRIMARY KEY (symbol, trade_date)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS daily_signals (
        date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        name VARCHAR,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        pct_chg DOUBLE,
        score_b1 DOUBLE,
        score_b2 DOUBLE,
        score_blk DOUBLE,
        score_macd_cross DOUBLE,
        score_rsi_oversold DOUBLE,
        score_bollinger DOUBLE,
        signal_buy_b1 BOOLEAN DEFAULT FALSE,
        signal_buy_b2 BOOLEAN DEFAULT FALSE,
        signal_buy_blk BOOLEAN DEFAULT FALSE,
        signal_buy_macd_cross BOOLEAN DEFAULT FALSE,
        signal_buy_rsi_oversold BOOLEAN DEFAULT FALSE,
        signal_buy_bollinger BOOLEAN DEFAULT FALSE,
        signal_sell_s1 BOOLEAN DEFAULT FALSE,
        signal_sell_stop_loss BOOLEAN DEFAULT FALSE,
        signal_sell_trailing BOOLEAN DEFAULT FALSE,
        indicators JSON,
        PRIMARY KEY (date, symbol)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY,
        symbol VARCHAR,
        name VARCHAR,
        strategy VARCHAR,
        buy_date DATE,
        shares INTEGER,
        buy_price DOUBLE,
        current_price DOUBLE,
        stop_loss_pct DOUBLE DEFAULT 0.05,
        status VARCHAR DEFAULT 'holding',
        sell_date DATE,
        sell_price DOUBLE,
        sell_reason VARCHAR,
        profit_loss DOUBLE,
        profit_pct DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.execute("CHECKPOINT")
    conn.close()
    print(f"US stock database initialized: {DB_PATH}")


if __name__ == '__main__':
    init_database()
