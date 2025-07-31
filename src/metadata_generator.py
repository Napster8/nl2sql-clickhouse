import os
import pandas as pd
import dspy
from typing import List, Dict
from dotenv import load_dotenv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Load environment variables
load_dotenv()

# Configure logging only if running as main script
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

logger = logging.getLogger(__name__)

# Configure Google API for DSPy
import google.generativeai as genai
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Validate API key first
if not os.getenv("GOOGLE_API_KEY"):
    logger.warning("⚠️  GOOGLE_API_KEY not found in environment variables")

class GeminiFlash(dspy.LM):
    """DSPy module for Gemini Flash 2.5"""
    def __init__(self, model="gemini-2.5-flash", thinking_budget=0, **kwargs):
        super().__init__(model=model, **kwargs)
        self.thinking_budget = thinking_budget

# Initialize models using proper DSPy Gemini integration
try:
    column_model = GeminiFlash(model=os.getenv("tier_3"), thinking_budget=0)
    table_model = GeminiFlash(model=os.getenv("tier_2"), thinking_budget=0)
    logger.info("✅ Successfully initialized Gemini models")
except Exception as e:
    logger.error(f"❌ Failed to initialize models: {e}")
    raise

# Configure DSPy
dspy.settings.configure(lm=column_model)

# DSPy Signatures
class GenerateColumnDescription(dspy.Signature):
    """Generate concise column description without filler words. Rules:
    - No filler words like "This column contains"
    - For datetime: specify format like "YYYY-MM-DD HH:MM:SS"
    - For IDs: State the "ID with high cardinality. Likely primary key." Also state the pattern of ID if there is any.
    - Be direct and factual
    - Under 450 words"""
    table_name = dspy.InputField(desc="Name of the table")
    column_name = dspy.InputField(desc="Name of the column")
    data_type = dspy.InputField(desc="Data type of the column")
    cardinality = dspy.InputField(desc="Number of distinct values")
    cardinality_level = dspy.InputField(desc="High/Low cardinality level")
    total_rows = dspy.InputField(desc="Total number of rows")
    primary_key = dspy.InputField(desc="Whether this is a primary key")
    distinct_values = dspy.InputField(desc="Sample distinct values")
    neighboring_columns = dspy.InputField(desc="Names of neighboring columns")
    description = dspy.OutputField(desc="Concise column description without filler words, under 1000 characters")

class GenerateTableDescription(dspy.Signature):
    """Generate concise table description without filler words. Rules:
    - No filler words like "This table contains" or "This table stores"
    - Be direct about what the table represents
    - Focus on business purpose
    - Under 50 words"""
    table_name = dspy.InputField(desc="Name of the table")
    total_rows = dspy.InputField(desc="Total number of rows")
    columns_summary = dspy.InputField(desc="Summary of key columns and their relationships")
    description = dspy.OutputField(desc="Concise table description without filler words, under 1000 characters")

# DSPy Modules
class ColumnDescriber(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generate = dspy.Predict(GenerateColumnDescription)
        
    def forward(self, table_name: str, column_name: str, data_type: str,
                cardinality: int, cardinality_level: str, total_rows: int,
                primary_key: str, distinct_values: int, neighboring_columns: str) -> str:
        
        try:
            result = self.generate(
                table_name=table_name,
                column_name=column_name,
                data_type=data_type,
                cardinality=cardinality,
                cardinality_level=cardinality_level,
                total_rows=total_rows,
                primary_key=primary_key,
                distinct_values=distinct_values,
                neighboring_columns=neighboring_columns
            )
            
            description = result.description.strip()
            return description[:997] + "..." if len(description) > 1000 else description
            
        except Exception as e:
            logger.error(f"❌ Error generating description for {column_name}: {e}")
            return "Description generation failed"

class TableDescriber(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generate = dspy.Predict(GenerateTableDescription)
        
    def forward(self, table_name: str, total_rows: int, columns_summary: str) -> str:
        
        try:
            result = self.generate(
                table_name=table_name,
                total_rows=total_rows,
                columns_summary=columns_summary
            )
            
            description = result.description.strip()
            return description[:997] + "..." if len(description) > 1000 else description
            
        except Exception as e:
            logger.error(f"❌ Error generating description for {table_name}: {e}")
            return "Description generation failed"

class MetadataEnricher:
    """Metadata enricher using DSPy for structured generation"""
    
    def __init__(self):
        self.column_describer = ColumnDescriber()
        self.table_describer = TableDescriber()
        
        # Configure models for each task
        self.column_describer.lm = column_model
        self.table_describer.lm = table_model
        
    def _create_columns_summary(self, columns: List[Dict]) -> str:
        """Create a summary of columns for table description"""
        summary_parts = []
        
        # Add key columns
        summary_parts.append("Key Columns:")
        for col in columns[:8]:
            summary_parts.append(f"- {col['column_name']} ({col['data_type']})")
        
        # Add neighboring relationships
        summary_parts.append("\nColumn Relationships:")
        for col in columns[:5]:
            if col['neighbouring_columns'] and col['neighbouring_columns'] != '':
                summary_parts.append(f"- {col['column_name']} neighbors: {col['neighbouring_columns']}")
        
        return "\n".join(summary_parts)
    
    def enrich_metadata(self, input_file: str, output_file: str, max_workers: int = 3, test_mode: int = None) -> pd.DataFrame:
        """Main enrichment process using DSPy"""
        logger.info(f"🤖 Starting metadata enrichment: {input_file} -> {output_file}")
        
        # Load metadata
        df_full = pd.read_csv(input_file)
        
        # Apply test mode if specified
        if test_mode:
            df_to_process = df_full.head(test_mode)
            logger.info(f"🧪 Test mode: Processing first {test_mode} columns only (preserving all {len(df_full)} rows)")
        else:
            df_to_process = df_full
        
        logger.info(f"📊 Loaded {len(df_full)} total rows, processing {len(df_to_process)} rows")
        
        # Group by table (only for rows to process)
        tables = {}
        for _, row in df_to_process.iterrows():
            table_name = row['table_name']
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(row.to_dict())
        
        logger.info(f"🗂️  Found {len(tables)} unique tables")
        
        # Process tables
        all_results = []
        
        for table_idx, (table_name, columns) in enumerate(tables.items(), 1):
            progress = (table_idx / len(tables)) * 100
            logger.info(f"🔍 Processing table {table_idx}/{len(tables)} ({progress:.0f}%): {table_name}")
            
            # Generate table description
            columns_summary = self._create_columns_summary(columns)
            table_description = self.table_describer(
                table_name=table_name,
                total_rows=columns[0]['total_rows'],
                columns_summary=columns_summary
            )
            
            # Generate column descriptions in parallel
            column_descriptions = {}
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                
                for col in columns:
                    future = executor.submit(
                        self.column_describer,
                        table_name=col['table_name'],
                        column_name=col['column_name'],
                        data_type=col['data_type'],
                        cardinality=col['cardinality'],
                        cardinality_level=col['cardinality_level'],
                        total_rows=col['total_rows'],
                        primary_key=col['primary_key'],
                        distinct_values=col['distinct_values'],
                        neighboring_columns=col['neighbouring_columns']
                    )
                    futures[future] = col['column_name']
                
                completed_columns = 0
                total_columns = len(columns)
                
                for future in as_completed(futures):
                    col_name = futures[future]
                    completed_columns += 1
                    
                    try:
                        description = future.result()
                        column_descriptions[col_name] = description
                        
                        # Show column progress in verbose mode
                        col_progress = (completed_columns / total_columns) * 100
                        logger.debug(f"     ✅ Column {completed_columns}/{total_columns} ({col_progress:.0f}%): {col_name}")
                        
                        time.sleep(0.1)  # Rate limiting
                    except Exception as e:
                        logger.error(f"❌ Failed to process {col_name}: {e}")
                        column_descriptions[col_name] = "Description generation failed"
            
            # Combine results
            for col in columns:
                result_row = col.copy()
                result_row['column_description'] = column_descriptions[col['column_name']]
                result_row['table_description'] = table_description
                all_results.append(result_row)
        
        # In test mode, merge enriched results with original data
        if test_mode:
            # Create enriched dataframe from processed results
            enriched_df = pd.DataFrame(all_results)
            
            # Add description columns to full dataframe if they don't exist
            if 'column_description' not in df_full.columns:
                df_full['column_description'] = ''
            if 'table_description' not in df_full.columns:
                df_full['table_description'] = ''
            
            # Update only the processed rows with enriched data
            for _, enriched_row in enriched_df.iterrows():
                mask = (df_full['table_name'] == enriched_row['table_name']) & \
                       (df_full['column_name'] == enriched_row['column_name'])
                df_full.loc[mask, 'column_description'] = enriched_row['column_description']
                df_full.loc[mask, 'table_description'] = enriched_row['table_description']
            
            # Save the full dataframe with partial enrichment
            df_full.to_csv(output_file, index=False)
            final_df = df_full
        else:
            # Normal mode: save all enriched results
            enriched_df = pd.DataFrame(all_results)
            enriched_df.to_csv(output_file, index=False)
            final_df = enriched_df
        
        # Report
        logger.info("🎉 Enrichment complete!")
        logger.info(f"📊 Processed {len(tables)} tables, {len(all_results)} columns")
        
        # Verify character limits
        max_col_len = enriched_df['column_description'].str.len().max()
        max_table_len = enriched_df['table_description'].str.len().max()
        
        logger.info(f"📏 Max column description length: {max_col_len} characters")
        logger.info(f"📏 Max table description length: {max_table_len} characters")
        
        if max_col_len <= 1000 and max_table_len <= 1000:
            logger.info("✅ All descriptions within 1000 character limit!")
        else:
            logger.warning("⚠️  Some descriptions exceed 1000 character limit!")
        
        return final_df

def main():
    """Command-line interface"""
    enricher = MetadataEnricher()
    
    input_file = "outputs/clickhouse_metadata.csv"
    output_file = "outputs/clickhouse_metadata_enriched.csv"
    
    try:
        enriched_df = enricher.enrich_metadata(input_file, output_file)
        
        print("\n" + "="*50)
        print("ENRICHMENT SUMMARY")
        print("="*50)
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print(f"Total rows processed: {len(enriched_df)}")
        print(f"Unique tables: {enriched_df['table_name'].nunique()}")
        
        # Show sample
        print("\nSAMPLE OUTPUT:")
        sample = enriched_df.iloc[0]
        print(f"Table: {sample['table_name']}")
        print(f"Column: {sample['column_name']}")
        print(f"Neighboring Columns: {sample['neighbouring_columns']}")
        print(f"Column Description: {sample['column_description'][:100]}...")
        print(f"Table Description: {sample['table_description'][:100]}...")
        
        print("\n✅ All done! Check the output file for results.")
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()