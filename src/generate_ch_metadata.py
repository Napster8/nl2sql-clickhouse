import clickhouse_connect
import os
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Configure logging only if running as main script
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('outputs/metadata_generation.log'),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)

# Establish connection to clickhouse
logger.info("🔌 Establishing connection to ClickHouse...")
try:
    client = clickhouse_connect.get_client(
        host=os.getenv("ch_host"),   
        username=os.getenv("ch_username"),           
        password=os.getenv("ch_password"),
        secure=True
    )
    logger.info("✅ Successfully connected to ClickHouse")
except Exception as e:
    logger.error(f"❌ Failed to connect to ClickHouse: {e}")
    raise

database = os.getenv("ch_database")
logger.info(f"🗄️  Target database: {database}")

def classify_cardinality(cardinality, total_rows=None):
    """
    Classify column cardinality as High or Low
    
    Args:
        cardinality: Number of distinct values
        total_rows: Total number of rows in the table (optional, unused)
    
    Returns:
        str: 'High' or 'Low'
    """
    if isinstance(cardinality, str):  # Handle error cases
        return 'Unknown'
    
    return 'Low' if cardinality < 30 else 'High'

def get_comprehensive_database_metadata(client, database, output_file=None, test_mode=None):
    if output_file is None:
        # Get the directory containing this script, then go up one level to project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        output_file = os.path.join(project_root, 'outputs', 'clickhouse_metadata.csv')
    
    logger.info("🚀 Starting comprehensive database metadata extraction")
    start_time = datetime.now()
    
    # Step 1: Get all table names in the database
    logger.info("📋 Step 1/4: Fetching table names from database")
    table_query = f"SELECT name FROM system.tables WHERE database = '{database}'"
    try:
        table_names = [row[0] for row in client.query(table_query).result_rows]
        logger.info(f"✅ Found {len(table_names)} tables: {', '.join(table_names)}")
    except Exception as e:
        logger.error(f"❌ Failed to fetch table names: {e}")
        raise
    
    metadata = []
    total_tables = len(table_names)
    processed_columns = 0
    
    for table_idx, table in enumerate(table_names, 1):
        table_progress = f"({table_idx}/{total_tables})"
        logger.info(f"🔍 Step 2/4: Processing table {table_progress}: {table}")
        
        # Get total row count for the table
        try:
            logger.debug(f"   📊 Getting row count for {table}")
            row_count_query = f"SELECT count(*) FROM `{database}`.`{table}`"
            total_rows = client.query(row_count_query).result_rows[0][0]
            logger.debug(f"   ✅ Table {table} has {total_rows:,} rows")
        except Exception as e:
            logger.warning(f"   ⚠️  Error getting row count for {table}: {e}")
            total_rows = None
        
        # Get column information including data types and primary key status
        logger.debug(f"   🏗️  Fetching column metadata for {table}")
        column_query = f"""
        SELECT name, type, is_in_primary_key 
        FROM system.columns 
        WHERE database = '{database}' AND table = '{table}'
        """
        try:
            columns = client.query(column_query).result_rows
            logger.debug(f"   ✅ Found {len(columns)} columns in {table}")
            
            # Get all column names for neighbouring_columns
            all_column_names = [col[0] for col in columns]
        except Exception as e:
            logger.error(f"   ❌ Failed to get columns for {table}: {e}")
            continue
        
        for col_idx, (col_name, col_type, is_pk) in enumerate(columns, 1):
            processed_columns += 1
            col_progress = f"({col_idx}/{len(columns)})"
            logger.debug(f"     🔧 Step 3/4: Processing column {col_progress}: {table}.{col_name}")
            
            try:
                # Calculate cardinality (distinct count)
                logger.debug(f"       📈 Calculating cardinality for {col_name}")
                cardinality_query = f"SELECT count(DISTINCT `{col_name}`) FROM `{database}`.`{table}`"
                cardinality = client.query(cardinality_query).result_rows[0][0]
                
                # Get distinct values based on cardinality
                logger.debug(f"       🎯 Fetching sample values for {col_name} (cardinality: {cardinality})")
                if cardinality < 30:
                    distinct_query = f"SELECT DISTINCT `{col_name}` FROM `{database}`.`{table}` LIMIT 3"
                else:
                    distinct_query = f"SELECT DISTINCT `{col_name}` FROM `{database}`.`{table}` LIMIT 30"
                
                distinct_results = client.query(distinct_query).result_rows
                distinct_values = [str(row[0]) for row in distinct_results]
                
                # Classify cardinality level
                cardinality_level = classify_cardinality(cardinality, total_rows)
                
                # Get neighbouring columns (all columns except current one)
                neighbouring_columns = [col for col in all_column_names if col != col_name]
                
                # Create metadata record
                metadata.append({
                    'table_name': table,
                    'column_name': col_name,
                    'data_type': col_type,
                    'cardinality': cardinality,
                    'cardinality_level': cardinality_level,
                    'total_rows': total_rows,
                    'primary_key': 'Yes' if is_pk else 'No',
                    'distinct_values': ', '.join(distinct_values) if distinct_values else '',
                    'neighbouring_columns': ', '.join(neighbouring_columns)
                })
                
                logger.debug(f"       ✅ Successfully processed {col_name} - {cardinality_level} cardinality ({cardinality:,} distinct)")
                
            except Exception as e:
                logger.error(f"       ❌ Error processing {table}.{col_name}: {e}")
                # Get neighbouring columns (all columns except current one)
                neighbouring_columns = [col for col in all_column_names if col != col_name]
                
                # Add record with error info
                metadata.append({
                    'table_name': table,
                    'column_name': col_name,
                    'data_type': col_type,
                    'cardinality': 'Error',
                    'cardinality_level': 'Unknown',
                    'total_rows': total_rows,
                    'primary_key': 'Yes' if is_pk else 'No',
                    'distinct_values': f'Error: {str(e)}',
                    'neighbouring_columns': ', '.join(neighbouring_columns)
                })
        
        # Progress update after each table
        progress_pct = (table_idx / total_tables) * 100
        logger.info(f"   ✅ Completed table {table} - Progress: {progress_pct:.1f}% ({table_idx}/{total_tables} tables)")
    
    # Step 4: Create DataFrame and save to CSV
    logger.info("💾 Step 4/4: Saving metadata to CSV file")
    try:
        df = pd.DataFrame(metadata)
        
        # Apply test mode if specified (limit to first N rows)
        if test_mode:
            df = df.head(test_mode)
            logger.info(f"🧪 Test mode: Saving only first {test_mode} rows to CSV")
        
        df.to_csv(output_file, index=False)
        
        # Summary statistics
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("🎉 Metadata extraction completed successfully!")
        logger.info(f"📊 Summary:")
        logger.info(f"   • Tables processed: {total_tables}")
        logger.info(f"   • Columns processed: {processed_columns}")
        logger.info(f"   • Total records: {len(metadata)}")
        logger.info(f"   • Duration: {duration}")
        logger.info(f"   • Output file: {output_file}")
        
        # Cardinality breakdown
        if len(metadata) > 0:
            cardinality_counts = df['cardinality_level'].value_counts()
            logger.info(f"📈 Cardinality breakdown:")
            for level, count in cardinality_counts.items():
                pct = (count / len(metadata)) * 100
                logger.info(f"   • {level}: {count} columns ({pct:.1f}%)")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to save metadata: {e}")
        raise

if __name__ == "__main__":
    # Ensure outputs directory exists
    os.makedirs('outputs', exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("🚀 CLICKHOUSE METADATA EXTRACTION STARTED")
    logger.info("=" * 60)
    
    try:
        df_metadata = get_comprehensive_database_metadata(client, database)
        logger.info("=" * 60)
        logger.info("✅ METADATA EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ METADATA EXTRACTION FAILED")
        logger.error(f"Error: {e}")
        logger.error("=" * 60)
        raise