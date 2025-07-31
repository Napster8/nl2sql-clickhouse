# src/sql_executor.py
import clickhouse_connect
from typing import List, Dict, Any, Optional
from src.config import Config
import logging

logger = logging.getLogger(__name__)

class SQLExecutor:
    """Executes SQL queries against ClickHouse"""
    
    def __init__(self):
        self.client = None
        self._connect()
    
    def _connect(self):
        """Connect to ClickHouse"""
        try:
            connection_params = Config.get_clickhouse_connection_params()
            self.client = clickhouse_connect.get_client(**connection_params)
            logger.info("✅ Connected to ClickHouse")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            raise
    
    def execute_query(self, sql_query: str) -> Optional[List[Dict[str, Any]]]:
        """Execute SQL query and return results"""
        try:
            if not self.client:
                self._connect()
            
            logger.info(f"Executing query: {sql_query}")
            result = self.client.query(sql_query)
            
            # Convert to list of dictionaries
            if result.result_rows:
                columns = result.column_names
                rows = []
                for row in result.result_rows:
                    row_dict = dict(zip(columns, row))
                    rows.append(row_dict)
                return rows
            else:
                return []
                
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test ClickHouse connection"""
        try:
            if not self.client:
                self._connect()
            
            result = self.client.query("SELECT 1")
            return len(result.result_rows) > 0
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False