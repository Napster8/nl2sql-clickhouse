# src/vector_store.py
import pandas as pd
import mindsdb_sdk
import os
from typing import Optional
from src.config import Config

class VectorStore:
    """Handles vector storage and retrieval using MindsDB"""
    
    def __init__(self, verbose: bool = True):
        self.connection = None
        self.kb_name = Config.KB_NAME
        self.verbose = verbose
    
    def connect(self) -> None:
        """Connect to MindsDB"""
        if not self.connection:
            # Validate configuration first
            if not Config.validate_mindsdb_config():
                raise ValueError("Invalid MindsDB configuration")
            
            params = Config.get_mindsdb_connection_params()
            if self.verbose:
                print(f"🔌 Connecting to MindsDB at {Config.MINDSDB_URL}")
            self.connection = mindsdb_sdk.connect(**params)
            if self.verbose:
                print("✅ Connected to MindsDB successfully")
    
    def create_knowledge_base(self, csv_path: str = Config.METADATA_FILE) -> str:
        """Create MindsDB Knowledge Base from enriched metadata CSV"""
        self.connect()
        
        # Load metadata
        df = pd.read_csv(csv_path)
        
        # Drop existing KB if it exists
        try:
            self.connection.knowledge_bases.drop(self.kb_name)
        except Exception:
            pass  # Ignore if it doesn't exist
        
        # Create KB with proper params (add embedding model from config)
        kb = self.connection.knowledge_bases.create(
            name=self.kb_name,
            metadata_columns=['table_name', 'data_type', 'cardinality', 'cardinality_level', 'total_rows', 'primary_key', 'distinct_values', 'neighbouring_columns', 'table_description'],
            content_columns=['column_description'],  # Embed this column for semantic search
            id_column='column_name',  # Use a unique column as ID
            params={
                'embedding_model': {
                    'provider': 'google',
                    'model_name': 'text-embedding-004',
                    'api_key': Config.GOOGLE_API_KEY
                }
            }
        )
        
        # Insert data directly from DataFrame
        kb.insert(df)
        
        print(f"✅ Knowledge Base '{self.kb_name}' created and populated")
        return self.kb_name
    
    def search(self, query: str, top_k: int = Config.TOP_K_RESULTS) -> list:
        """Search the knowledge base for relevant metadata"""
        if self.verbose:
            print(f"🔍 Searching for: {query}")
        
        # Ensure we're connected before searching
        self.connect()
        
        kb = self.connection.knowledge_bases.get(self.kb_name)
        results = kb.find(query=query, limit=top_k).fetch()
        # Format results as list of dicts
        return results.to_dict(orient='records')
    
    def create_learnings_knowledge_base(self) -> str:
        """Create a separate knowledge base for query learnings from successful_queries.md"""
        learnings_kb_name = f"{self.kb_name}_learnings"
        
        try:
            self.connect()
            
            # Check if learnings file exists
            learnings_file = "data/successful_queries.md"
            if not os.path.exists(learnings_file):
                if self.verbose:
                    print("No successful queries file found, skipping learnings KB creation")
                return learnings_kb_name
            
            # Read and parse learnings file
            with open(learnings_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse learnings into structured data
            learnings_data = []
            # Split by ### headers (each query entry)
            entries = content.split('### ')[1:]  # Skip any content before first ###
            
            for i, entry in enumerate(entries):
                if not entry.strip():
                    continue
                
                lines = entry.strip().split('\n')
                if not lines:
                    continue
                
                # First line is the query pattern (title)
                query_pattern = lines[0].strip()
                
                learning_record = {
                    'id': f"learning_{i}",
                    'query_pattern': query_pattern,
                    'sql_solution': '',
                    'tables_involved': '',
                    'learning': '',
                    'user_feedback': ''
                }
                
                # Parse the rest of the lines
                for line in lines[1:]:
                    if line.startswith('**SQL:**'):
                        # Extract SQL from markdown code format
                        sql_part = line.replace('**SQL:**', '').strip()
                        if sql_part.startswith('`') and sql_part.endswith('`'):
                            learning_record['sql_solution'] = sql_part[1:-1]  # Remove backticks
                        else:
                            learning_record['sql_solution'] = sql_part
                    elif line.startswith('**Tables:**'):
                        learning_record['tables_involved'] = line.replace('**Tables:**', '').strip()
                    elif line.startswith('**Learning:**'):
                        learning_record['learning'] = line.replace('**Learning:**', '').strip()
                    elif line.startswith('**Key Insight:**'):
                        learning_record['user_feedback'] = line.replace('**Key Insight:**', '').strip()
                
                if learning_record['query_pattern']:  # Only add if we have a query pattern
                    learnings_data.append(learning_record)
            
            if not learnings_data:
                if self.verbose:
                    print("No valid learnings data found")
                return learnings_kb_name
            
            # Create DataFrame
            import pandas as pd
            df = pd.DataFrame(learnings_data)
            
            # Drop existing learnings KB if it exists
            try:
                self.connection.knowledge_bases.drop(learnings_kb_name)
            except Exception:
                pass
            
            # Create learnings KB
            kb = self.connection.knowledge_bases.create(
                name=learnings_kb_name,
                metadata_columns=['tables_involved', 'user_feedback'],
                content_columns=['query_pattern', 'sql_solution', 'learning'],
                id_column='id',
                params={
                    'embedding_model': {
                        'provider': 'google',
                        'model_name': 'text-embedding-004',
                        'api_key': Config.GOOGLE_API_KEY
                    }
                }
            )
            
            # Insert learnings data
            kb.insert(df)
            
            if self.verbose:
                print(f"✅ Learnings Knowledge Base '{learnings_kb_name}' created with {len(learnings_data)} entries")
            
            return learnings_kb_name
            
        except Exception as e:
            if self.verbose:
                print(f"Warning: Could not create learnings KB: {e}")
            return learnings_kb_name
    
    def search_learnings(self, query: str, top_k: int = 5) -> list:
        """Search the learnings knowledge base for relevant query patterns"""
        learnings_kb_name = f"{self.kb_name}_learnings"
        
        try:
            self.connect()
            kb = self.connection.knowledge_bases.get(learnings_kb_name)
            results = kb.find(query=query, limit=top_k).fetch()
            return results.to_dict(orient='records')
        except Exception as e:
            if self.verbose:
                print(f"Warning: Could not search learnings KB: {e}")
            return []