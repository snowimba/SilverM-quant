"""美股量化系统配置"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / '.env')
except ImportError:
    pass


class Settings:
    PROJECT_ROOT = Path(__file__).parent.parent
    DATABASE_PATH = PROJECT_ROOT / 'data' / 'us_stock.duckdb'
    LOG_DIR = PROJECT_ROOT / 'logs'

    # yfinance 不需要 API key
    DATA_SOURCE = 'yfinance'
    DOWNLOAD_BATCH_SIZE = 50
    DOWNLOAD_WORKERS = 8

    # LLM
    LLM_PROVIDER = os.getenv('LLM_PROVIDER', 'deepseek')
    LLM_MODEL = os.getenv('LLM_MODEL', 'gpt-5.5')
    DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY')
    DEEPSEEK_BASE_URL = os.getenv('DEEPSEEK_BASE_URL')

    # 回测
    DEFAULT_INITIAL_CASH = 100000.0
    DEFAULT_COMMISSION = 0.001
    DEFAULT_STOP_LOSS = 0.05

    @classmethod
    def ensure_directories(cls):
        cls.DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)


Settings.ensure_directories()
