# src/sql_executor.py
import clickhouse_connect
from typing import List, Any, Dict
from config import Config

class SQLExecutor:
    """Executes SQL queries against ClickHouse"""
    
    def __init__(self):
        self.client = None
        self.connection_params = Config.get_clickhouse_connection_params()
    
    def connect(self) -> None:
        """Connect to ClickHouse"""
        if not self.client:
            self.client = clickhouse_connect.get_client(**self.connection_params)
    
    def execute_query(self, sql_query: str) -> List[Any]:
        """Execute a SQL query and return results"""
        self.connect()
        
        try:
            result = self.client.query(sql_query)
            return result.result_rows
        except Exception as e:
            print(f"Error executing query: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get schema information for a table"""
        self.connect()
        
        try:
            result = self.client.query(f"DESCRIBE TABLE {table_name}")
            return {row[0]: row[1] for row in result.result_rows}
        except Exception as e:
            print(f"Error getting table schema: {e}")
            return {}