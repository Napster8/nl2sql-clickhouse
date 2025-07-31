# src/config.py
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration settings for the application"""
    
    # MindsDB settings
    MINDSDB_HOST = os.getenv('MINDSDB_HOST')
    MINDSDB_PORT = int(os.getenv('MINDSDB_PORT'))
    MINDSDB_USER = os.getenv('MINDSDB_USER')
    MINDSDB_PASSWORD = os.getenv('MINDSDB_PASSWORD')
    
    # Construct full MindsDB URL
    MINDSDB_URL = f'http://{MINDSDB_HOST}:{MINDSDB_PORT}'
    
    # Gemini API settings
    GEMINI_API_KEY = os.getenv('GOOGLE_API_KEY')
    GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')  # Alias for compatibility
    GOOGLE_CLOUD_QUOTA_PROJECT = os.getenv('GOOGLE_CLOUD_QUOTA_PROJECT')
    
    # Knowledge base settings
    KB_NAME = 'clickhouse_metadata_kb'
    KB_DATA_TABLE = f'{KB_NAME}_data'
    
    # File paths
    METADATA_FILE = 'outputs/clickhouse_metadata.csv'
    
    # LLM Model tiers from .env
    TIER_1_MODEL = os.getenv('tier_1')
    TIER_2_MODEL = os.getenv('tier_2')
    TIER_3_MODEL = os.getenv('tier_3')
    
    # DSPy settings
    DSPY_MODEL = TIER_2_MODEL
    
    # Query processing settings
    CONFIDENCE_THRESHOLD = 0.7
    TOP_K_RESULTS = 10
    
    # ClickHouse settings - match .env variable names
    CLICKHOUSE_HOST = os.getenv('ch_host')
    CLICKHOUSE_PORT = int(os.getenv('ch_port'))
    CLICKHOUSE_USER = os.getenv('ch_username')
    CLICKHOUSE_PASSWORD = os.getenv('ch_password')
    CLICKHOUSE_DATABASE = os.getenv('ch_database')
    
    @classmethod
    def get_mindsdb_connection_params(cls) -> Dict[str, Any]:
        """Get MindsDB connection parameters"""
        return {
            'url': cls.MINDSDB_URL
        }
    
    @classmethod
    def get_clickhouse_connection_params(cls) -> Dict[str, Any]:
        """Get ClickHouse connection parameters"""
        return {
            'host': cls.CLICKHOUSE_HOST,
            'port': cls.CLICKHOUSE_PORT,
            'user': cls.CLICKHOUSE_USER,
            'password': cls.CLICKHOUSE_PASSWORD,
            'database': cls.CLICKHOUSE_DATABASE
        }
    
    @classmethod
    def validate_mindsdb_config(cls) -> bool:
        """Validate MindsDB configuration parameters"""
        required_params = [
            cls.MINDSDB_HOST,
            cls.MINDSDB_PORT
        ]
        
        if not all(required_params):
            missing = []
            if not cls.MINDSDB_HOST:
                missing.append('MINDSDB_HOST')
            if not cls.MINDSDB_PORT:
                missing.append('MINDSDB_PORT')
            
            print(f"❌ Missing MindsDB configuration: {', '.join(missing)}")
            return False
        
        return True