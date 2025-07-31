#!/usr/bin/env python3
"""
ClickHouse Metadata Extraction Orchestrator

Simple CLI driver for running ClickHouse metadata extraction.
"""

import argparse
import sys
import os
import logging

# Configuration
MAX_WORKERS = 3  # Number of parallel workers for AI enrichment

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from generate_ch_metadata import get_comprehensive_database_metadata, client, database
from metadata_generator import MetadataEnricher

def setup_logging(verbose=False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="ClickHouse Metadata Extraction Orchestrator",
        epilog="""
Examples:
  uv run orchestrator.py --metadata        # Extract metadata only
  uv run orchestrator.py --enrich          # Enrich existing metadata only
  uv run orchestrator.py --metadata --enrich  # Extract + enrich in one go
  uv run orchestrator.py --enrich --test-mode 5  # Test enrichment with first 5 rows
  uv run orchestrator.py -m -v             # Metadata with verbose logging
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '-m', '--metadata',
        action='store_true',
        help='Extract database metadata'
    )
    
    parser.add_argument(
        '-e', '--enrich',
        action='store_true',
        help='Enable AI-powered metadata enrichment (requires GOOGLE_API_KEY)'
    )
    
    parser.add_argument(
        '--test-mode',
        type=int,
        metavar='N',
        help='Test mode: process only first N columns (useful for testing)'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    try:
        # Ensure outputs directory exists
        os.makedirs('outputs', exist_ok=True)
        
        # Check if any action is requested
        if not args.metadata and not args.enrich:
            logger.error("❌ No action specified. Use --metadata or --enrich")
            parser.print_help()
            sys.exit(1)
        
        basic_output_file = 'outputs/clickhouse_metadata.csv'
        
        # Step 1: Run metadata extraction if requested
        if args.metadata:
            logger.info("🎯 Starting metadata extraction")
            df_metadata = get_comprehensive_database_metadata(
                client=client, 
                database=database,
                output_file=basic_output_file,
                test_mode=args.test_mode
            )
            logger.info("🎉 Extraction completed")
            logger.info(f"📄 Output file: {basic_output_file}")
        
        # Step 2: Run enrichment if requested
        if args.enrich:
            # Check if metadata file exists (unless we just created it)
            if not args.metadata and not os.path.exists(basic_output_file):
                logger.error(f"❌ Metadata file not found: {basic_output_file}")
                logger.error("Run with --metadata first to generate the metadata file")
                sys.exit(1)
            
            # Check for API key
            if not os.getenv("GOOGLE_API_KEY"):
                logger.error("❌ GOOGLE_API_KEY not found in environment variables")
                sys.exit(1)
            
            logger.info("🤖 Starting AI enrichment")
            enricher = MetadataEnricher()
            
            enriched_df = enricher.enrich_metadata(
                input_file=basic_output_file,
                output_file=basic_output_file,
                max_workers=MAX_WORKERS,
                test_mode=args.test_mode
            )
            
            logger.info("🎉 Enrichment completed")
            logger.info(f"📄 Output file: {basic_output_file}")
        
    except ImportError as e:
        logger.error(f"❌ Missing dependencies: {e}")
        logger.error("Please ensure all required packages are installed")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()