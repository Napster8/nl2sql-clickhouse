#!/usr/bin/env python3
"""
ClickHouse Query Processing Orchestrator
Simple CLI driver for running the query processing system.
"""
import argparse
import sys
import os
import logging
import warnings

# Suppress Google Cloud SDK authentication warnings
warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
warnings.filterwarnings("ignore", category=UserWarning, module="google.auth._default")

# Suppress LiteLLM verbose logging
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("litellm").setLevel(logging.WARNING)

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_store import VectorStore
from src.query_processor import QueryProcessor
from src.sql_executor import SQLExecutor
from src.config import Config

def setup_logging(verbose=False, interactive=False):
    """Setup logging configuration"""
    if interactive:
        # In interactive mode, suppress all logging to console
        logging.basicConfig(
            level=logging.CRITICAL,  # Only show critical errors
            format='%(message)s',
            handlers=[logging.FileHandler('debug.log')] if verbose else [logging.NullHandler()]
        )
        # Suppress all third-party loggers
        logging.getLogger().setLevel(logging.CRITICAL)
        for logger_name in ['LiteLLM', 'litellm', 'httpx', 'openai', 'anthropic']:
            logging.getLogger(logger_name).setLevel(logging.CRITICAL)
    else:
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="ClickHouse Query Processing Orchestrator",
        epilog="""
Examples:
  uv run query_orchestrator.py --create-kb        # Create knowledge base from metadata
  uv run query_orchestrator.py --interactive      # Start interactive query mode
  uv run query_orchestrator.py --create-kb --interactive  # Create KB and then start interactive mode
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--create-kb',
        action='store_true',
        help='Create the knowledge base from the metadata file'
    )
    
    parser.add_argument(
        '-i', '--interactive',
        action='store_true',
        help='Start interactive query mode'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose, args.interactive)
    logger = logging.getLogger(__name__)
    
    try:
        # Check if any action is requested
        if not args.create_kb and not args.interactive:
            logger.error("❌ No action specified. Use --create-kb or --interactive")
            parser.print_help()
            sys.exit(1)
        
        # Step 1: Create knowledge base if requested
        if args.create_kb:
            if not os.path.exists(Config.METADATA_FILE):
                logger.error(f"❌ Metadata file not found: {Config.METADATA_FILE}")
                logger.error("Run the metadata extraction orchestrator first")
                sys.exit(1)
            
            logger.info("🧠 Creating knowledge base")
            vector_store = VectorStore()
            vector_store.create_knowledge_base()
            
            # Also create learnings knowledge base if learnings exist
            logger.info("🧠 Creating learnings knowledge base")
            vector_store.create_learnings_knowledge_base()
            
            logger.info("✅ Knowledge bases created successfully")
        
        # Step 2: Start interactive mode if requested
        if args.interactive:
            # Clean startup for interactive mode
            if not args.verbose:
                print("🤖 ClickHouse Query Assistant")
                print("Type your questions in natural language. Commands: 'exit', 'history', 'help'")
            else:
                logger.info("🚀 Starting interactive query mode")
            
            query_processor = QueryProcessor(verbose=args.verbose)
            sql_executor = SQLExecutor()
            
            while True:
                try:
                    user_query = input("\n💬 ")
                    if user_query.lower() in ['exit', 'quit']:
                        print("👋 Goodbye!")
                        break
                    
                    if user_query.lower() == 'history':
                        query_processor.show_conversation_history()
                        continue
                    
                    if user_query.lower() == 'help':
                        print("\n📖 Available commands:")
                        print("  • Type natural language questions about your data")
                        print("  • 'history' - Show conversation history")
                        print("  • 'clear' - Clear conversation history")
                        print("  • 'exit' or 'quit' - Exit the assistant")
                        continue
                    
                    if user_query.lower() == 'clear':
                        query_processor.conversation_history.clear()
                        print("🧹 Conversation history cleared!")
                        continue
                    
                    # Process the query
                    result = query_processor.process_query(user_query)
                    
                    # Display results in clean format
                    if result['sql_query']:
                        # Pre-execution refinement loop
                        sql_query = result['sql_query']
                        user_approved = False
                        tried_queries = set()  # Track queries we've already tried
                        tried_queries.add(sql_query.strip())
                        
                        while True:
                            print(f"\n🔍 Generated SQL:")
                            print(f"```sql\n{sql_query}\n```")
                            
                            refinement = input("\n🤔 Is this what you want? (yes/no/modify/regenerate): ").lower().strip()
                            
                            if refinement == 'yes':
                                user_approved = True
                                break
                            elif refinement == 'no':
                                print("❌ Query cancelled.")
                                break
                            elif refinement == 'modify':
                                # Get specific modification feedback with better prompts
                                print("\n🔧 What type of modification do you need?")
                                print("  Examples:")
                                print("  • 'Add a filter for status = active'")
                                print("  • 'Group by month instead of day'") 
                                print("  • 'Include product names in the results'")
                                print("  • 'Sort by revenue descending'")
                                
                                modification = input("\n💭 Describe the modification: ").strip()
                                if modification:
                                    print(f"🔄 Modifying query: {modification}")
                                    
                                    # MODIFY context: Keep original query as base, add modification
                                    context = query_processor.retrieve_relevant_context(user_query)
                                    
                                    # Create modification-specific prompt
                                    modification_query = f"""
Original request: {user_query}

Current SQL that needs modification:
{sql_query}

MODIFICATION NEEDED: {modification}

Please modify the above SQL query to incorporate the requested change. Keep the core logic the same but apply the modification.
"""
                                    
                                    rephrased = query_processor.rephrase_query(modification_query, context)
                                    
                                    # Generate modified SQL with specific context
                                    modification_context = f"""
TASK: MODIFY existing SQL query
Original SQL: {sql_query}
Modification requested: {modification}

{context}

Instructions: Take the existing SQL and apply ONLY the requested modification. Do not completely rewrite the query.
"""
                                    
                                    new_sql = query_processor.generate_sql(
                                        rephrased, modification_context, f"MODIFY: {modification}", ""
                                    )
                                    
                                    sql_query = new_sql
                                    tried_queries.add(new_sql.strip())
                                    
                                    # Store the modification learning immediately
                                    query_processor.add_to_conversation_history(
                                        user_query, sql_query, f"User requested modification: {modification}", False
                                    )
                                else:
                                    print("❌ No modification specified.")
                            elif refinement == 'regenerate':
                                # Get regeneration feedback with different prompts
                                print("\n🔄 Why do you want to regenerate?")
                                print("  Examples:")
                                print("  • 'This approach is completely wrong'")
                                print("  • 'Need a different table/approach'")
                                print("  • 'Missing key business logic'")
                                print("  • 'Wrong aggregation method'")
                                
                                regeneration_reason = input("\n💭 What's wrong with this approach? ").strip()
                                if regeneration_reason:
                                    print(f"🔄 Regenerating from scratch: {regeneration_reason}")
                                    
                                    # REGENERATE context: Start fresh, avoid previous approach
                                    context = query_processor.retrieve_relevant_context(user_query)
                                    
                                    # Create regeneration-specific prompt
                                    regeneration_query = f"""
Original request: {user_query}

FAILED APPROACH that should be AVOIDED:
{sql_query}

PROBLEM WITH FAILED APPROACH: {regeneration_reason}

Please generate a COMPLETELY DIFFERENT SQL approach to solve this request. Do not use the same tables, joins, or logic as the failed approach above.
"""
                                    
                                    rephrased = query_processor.rephrase_query(regeneration_query, context)
                                    
                                    # Generate new SQL with regeneration context
                                    regeneration_context = f"""
TASK: COMPLETELY REGENERATE SQL query
Failed approach to avoid: {sql_query}
Reason for failure: {regeneration_reason}

{context}

Instructions: Generate a COMPLETELY DIFFERENT approach. Use different tables, different joins, different logic. Avoid the patterns from the failed query.
"""
                                    
                                    previous_failures = "\n".join([f"Failed Query {i+1}: {q}" for i, q in enumerate(tried_queries)])
                                    
                                    new_sql = query_processor.generate_sql(
                                        rephrased, regeneration_context, f"REGENERATE: {regeneration_reason}", previous_failures
                                    )
                                    
                                    sql_query = new_sql
                                    tried_queries.add(new_sql.strip())
                                    
                                    # Store the regeneration learning immediately
                                    query_processor.add_to_conversation_history(
                                        user_query, sql_query, f"Previous approach failed: {regeneration_reason}", False
                                    )
                                else:
                                    print("❌ No regeneration reason provided.")
                            else:
                                print("❓ Please answer 'yes', 'no', 'modify', or 'regenerate'")
                        
                        # Only proceed if user approved the query
                        if user_approved:
                            # Now check safety and execute
                            safety_result = query_processor.check_sql_safety(sql_query)
                            
                            if safety_result['is_safe']:
                                execute = input("\n▶️  Execute this query? (yes/no): ").lower()
                                if execute == 'yes':
                                    query_result = sql_executor.execute_query(sql_query)
                                    
                                    if query_result:
                                        print(f"\n📊 Results ({len(query_result)} rows):")
                                        for i, row in enumerate(query_result[:10]):  # Show first 10 rows
                                            print(f"  {i+1}. {row}")
                                        if len(query_result) > 10:
                                            print(f"  ... and {len(query_result) - 10} more rows")
                                        
                                        # Auto-store successful query (user already approved it pre-execution)
                                        print("✅ Query executed successfully! Storing for future reference.")
                                        query_processor.store_successful_query(
                                            user_query, sql_query, "User approved and executed successfully", query_result
                                        )
                                    else:
                                        print("\n❌ No results returned")
                            else:
                                print(f"\n⚠️  Safety warning: {safety_result['reason']}")
                                execute = input("Execute anyway? (yes/no): ").lower()
                                if execute == 'yes':
                                    query_result = sql_executor.execute_query(sql_query)
                                    
                                    if query_result:
                                        print(f"\n📊 Results ({len(query_result)} rows):")
                                        for i, row in enumerate(query_result[:10]):
                                            print(f"  {i+1}. {row}")
                                        if len(query_result) > 10:
                                            print(f"  ... and {len(query_result) - 10} more rows")
                                        
                                        # Auto-store successful query (user already approved it pre-execution)
                                        print("✅ Query executed successfully! Storing for future reference.")
                                        query_processor.store_successful_query(
                                            user_query, sql_query, "User approved and executed successfully", query_result
                                        )
                                    else:
                                        print("\n❌ No results returned")
                    else:
                        print(f"\n❌ {result['reason']}")
                        
                except KeyboardInterrupt:
                    print("\n👋 Goodbye!")
                    break
                except Exception as e:
                    if args.verbose:
                        print(f"\n❌ Error: {e}")
                    else:
                        print("\n❌ Something went wrong. Try rephrasing your question.")
        
    except ImportError as e:
        logger.error(f"❌ Missing dependencies: {e}")
        logger.error("Please ensure all required packages are installed")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()