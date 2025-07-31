# src/query_processor.py
from typing import Dict, Any, Optional
import os
from datetime import datetime
from src.dspy_modules import QueryRephrasingModule, SQLGenerationModule, SQLSafetyCheckModule
from src.vector_store import VectorStore
from src.config import Config
import re

class QueryProcessor:
    """Processes user queries and generates SQL"""
    
    def __init__(self, verbose: bool = True):
        self.vector_store = VectorStore(verbose=verbose)
        self.rephrase_module = QueryRephrasingModule()
        self.sql_module = SQLGenerationModule()
        self.safety_module = SQLSafetyCheckModule()
        
        # Conversation history - store last 5 conversations
        self.conversation_history = []
        self.max_history = 5
    
    def add_to_conversation_history(self, user_query: str, sql_query: str, user_feedback: str = "", was_successful: bool = True):
        """Add a conversation to history, maintaining max_history limit"""
        conversation = {
            'user_query': user_query,
            'sql_query': sql_query,
            'user_feedback': user_feedback,
            'was_successful': was_successful,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self.conversation_history.append(conversation)
        
        # Keep only the last max_history conversations
        if len(self.conversation_history) > self.max_history:
            self.conversation_history.pop(0)
    
    def get_conversation_context(self) -> str:
        """Get formatted conversation history as context"""
        if not self.conversation_history:
            return ""
        
        context = "\n--- Previous Conversation History ---\n"
        for i, conv in enumerate(self.conversation_history, 1):
            context += f"\nConversation {i}:\n"
            context += f"User Query: {conv['user_query']}\n"
            context += f"Generated SQL: {conv['sql_query']}\n"
            if conv['user_feedback']:
                context += f"User Feedback: {conv['user_feedback']}\n"
            context += f"Status: {'Successful' if conv['was_successful'] else 'Not completed'}\n"
            context += f"Time: {conv['timestamp']}\n"
        
        context += "\n--- End of Previous Conversations ---\n\n"
        return context
    
    def show_conversation_history(self):
        """Display conversation history to the user"""
        if not self.conversation_history:
            print("No conversation history available.")
            return
        
        print("\n" + "="*60)
        print("CONVERSATION HISTORY (Last 5 conversations)")
        print("="*60)
        
        for i, conv in enumerate(self.conversation_history, 1):
            print(f"\n--- Conversation {i} ({conv['timestamp']}) ---")
            print(f"User Query: {conv['user_query']}")
            print(f"Generated SQL: {conv['sql_query']}")
            if conv['user_feedback']:
                print(f"User Feedback: {conv['user_feedback']}")
            print(f"Status: {'✅ Successful' if conv['was_successful'] else '❌ Not completed'}")
        
        print("\n" + "="*60)
    
    def rephrase_query(self, user_query: str, context: str = "") -> str:
        """Rephrase user query into a complete SQL problem with schema context"""
        # Include conversation history in context
        full_context = context
        conversation_context = self.get_conversation_context()
        if conversation_context:
            full_context = conversation_context + context
        
        result = self.rephrase_module(user_query=user_query, context=full_context)
        return result['rephrased_query']
    

    
    def retrieve_relevant_context(self, query: str) -> str:
        """Retrieve relevant context from knowledge base - top 20 tables with all their columns plus query learnings"""
        # First get relevant columns to identify tables
        context_data = self.vector_store.search(query, top_k=50)
        
        # Get unique table names from search results, ranked by relevance
        table_relevance = {}
        for item in context_data:
            table_name = item['metadata']['table_name']
            if table_name not in table_relevance:
                table_relevance[table_name] = item['relevance']
            else:
                # Keep the highest relevance score for each table
                table_relevance[table_name] = max(table_relevance[table_name], item['relevance'])
        
        # Sort tables by relevance and take top 20
        top_tables = sorted(table_relevance.items(), key=lambda x: x[1], reverse=True)[:20]
        top_table_names = [table[0] for table in top_tables]
        
        # Now get ALL columns for these top 20 tables from the metadata file
        import pandas as pd
        from src.config import Config
        
        df = pd.read_csv(Config.METADATA_FILE)
        
        context_str = "Available Database Schema (Top 20 most relevant tables):\n\n"
        
        for table_name in top_table_names:
            table_data = df[df['table_name'] == table_name]
            if not table_data.empty:
                # Get table description
                table_desc = table_data.iloc[0]['table_description']
                
                context_str += f"Table: {table_name}\n"
                context_str += f"Description: {table_desc}\n"
                context_str += "Columns:\n"
                
                # Add all columns for this table
                for _, row in table_data.iterrows():
                    col_name = row['column_name']
                    col_type = row['data_type']
                    col_desc = row['column_description']
                    context_str += f"  - {col_name} ({col_type}): {col_desc}\n"
                
                context_str += "\n"
        
        # Add relevant query learnings
        query_learnings = self._get_relevant_query_learnings(query)
        if query_learnings:
            context_str += "\n--- Previous Successful Query Patterns ---\n"
            context_str += query_learnings
            context_str += "\n--- End Query Patterns ---\n\n"
        
        return context_str
    
    def generate_sql(self, rephrased_query: str, context: str, user_feedback: str = "", previous_queries: str = "") -> str:
        """Generate SQL from query and context, optionally with user feedback"""
        # Include conversation history in context
        full_context = context
        conversation_context = self.get_conversation_context()
        if conversation_context:
            full_context = conversation_context + context
        
        result = self.sql_module(
            rephrased_query=rephrased_query, 
            context=full_context,
            user_feedback=user_feedback,
            previous_queries=previous_queries
        )
        
        # Clean the SQL before returning
        return self.clean_sql(result.sql_query)
    
    def check_sql_safety(self, sql_query: str) -> Dict[str, Any]:
        """Check SQL for dangerous operations"""
        result = self.safety_module(sql_query=sql_query)
        is_safe = result.is_safe.lower() == "true"
        reason = result.reason if not is_safe else "Query is safe"
        
        return {
            'is_safe': is_safe,
            'reason': reason
        }
    
    def store_successful_query(self, user_query: str, sql_query: str, user_feedback: str = "", query_results: list = None) -> None:
        """Store a successful query with learnings and add to RAG system"""
        successful_queries_file = "data/successful_queries.md"
        
        # Extract tables used from SQL
        tables_used = self._extract_tables_from_sql(sql_query)
        
        # Generate learning insights from the conversation
        learning_insights = self._extract_learning_insights(user_query, sql_query, user_feedback)
        
        # Create the entry with essential information only
        entry = f"""
### {user_query.strip()}
**SQL:** `{sql_query.strip()}`
**Tables:** {', '.join(tables_used) if tables_used else 'N/A'}
"""
        
        if user_feedback:
            entry += f"**Learning:** {user_feedback}\n"
        
        if learning_insights:
            entry += f"**Key Insight:** {learning_insights}\n"
        
        entry += "\n---\n"
        
        # Append to file
        with open(successful_queries_file, 'a', encoding='utf-8') as f:
            f.write(entry)
        
        # Add to RAG system for future queries
        self._add_to_knowledge_base(user_query, sql_query, tables_used, learning_insights, user_feedback)
    
    def _generate_simple_title(self, user_query: str) -> str:
        """Generate a simple title from the user query"""
        # Clean and capitalize the query, limit length
        title = user_query.strip()
        if len(title) > 60:
            title = title[:57] + "..."
        return title.capitalize()
    
    def _extract_tables_from_sql(self, sql_query: str) -> list:
        """Extract table names from SQL query"""
        # Simple regex to find table names after FROM and JOIN
        pattern = r'(?:FROM|JOIN)\s+(\w+)'
        matches = re.findall(pattern, sql_query, re.IGNORECASE)
        
        # Remove duplicates and return
        return list(set(matches))
    
    def _extract_learning_insights(self, user_query: str, sql_query: str, user_feedback: str) -> str:
        """Extract key learning insights from the successful query interaction"""
        insights = []
        
        # Analyze query patterns
        if "top" in user_query.lower() and "limit" in sql_query.lower():
            insights.append("Uses LIMIT for top N queries")
        
        if "revenue" in user_query.lower() and "sum" in sql_query.lower():
            insights.append("Revenue queries typically use SUM aggregation")
        
        if len(self._extract_tables_from_sql(sql_query)) > 1:
            insights.append("Multi-table joins required for this type of query")
        
        # Add user feedback insights
        if user_feedback:
            if "join" in user_feedback.lower():
                insights.append("User needed clarification on table relationships")
            if "group" in user_feedback.lower():
                insights.append("Grouping logic was important for this query")
        
        return "; ".join(insights) if insights else ""
    
    def _get_relevant_query_learnings(self, query: str) -> str:
        """Get relevant query learnings from successful queries using vector search"""
        try:
            # First try to search learnings knowledge base
            learnings_results = self.vector_store.search_learnings(query, top_k=3)
            
            if learnings_results:
                relevant_learnings = []
                for result in learnings_results:
                    query_pattern = result.get('query_pattern', '')
                    learning = result.get('learning', '')
                    if query_pattern and learning:
                        relevant_learnings.append(f"Similar query '{query_pattern}': {learning}")
                
                if relevant_learnings:
                    return "\n".join(relevant_learnings)
            
            # Fallback to file-based search using successful_queries.md
            if not os.path.exists("data/successful_queries.md"):
                return ""
            
            with open("data/successful_queries.md", 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract relevant patterns based on query keywords
            query_lower = query.lower()
            relevant_learnings = []
            
            # Split into individual query examples
            examples = content.split("### ")[1:]  # Skip header
            
            for example in examples:
                lines = example.strip().split('\n')
                if not lines:
                    continue
                
                query_title = lines[0].strip()
                
                # Check if this example is relevant to current query
                if any(keyword in query_title.lower() for keyword in query_lower.split() if len(keyword) > 2):
                    # Extract insights from Learning or Key Insight lines
                    for line in lines:
                        if line.startswith("**Learning:**"):
                            insight = line.replace("**Learning:**", "").strip()
                            relevant_learnings.append(f"Similar query '{query_title}': {insight}")
                            break
                        elif line.startswith("**Key Insight:**"):
                            insight = line.replace("**Key Insight:**", "").strip()
                            relevant_learnings.append(f"Similar query '{query_title}': {insight}")
                            break
            
            return "\n".join(relevant_learnings[:3])  # Limit to top 3 most relevant
            
        except Exception as e:
            return ""
    
    def _add_to_knowledge_base(self, user_query: str, sql_query: str, tables_used: list, learning_insights: str, user_feedback: str) -> None:
        """Add successful query learnings to the knowledge base for future reference"""
        try:
            # Only add to conversation history for immediate use
            # The .md file already contains all the learning information needed
            self.add_to_conversation_history(user_query, sql_query, user_feedback, True)
            
        except Exception as e:
            if hasattr(self, 'verbose') and self.verbose:
                print(f"Warning: Could not add to knowledge base: {e}")
    
    def clean_sql(self, sql_query: str) -> str:
        """Clean SQL query by removing markdown formatting, comments, and extra whitespace"""
        if not sql_query:
            return sql_query
        
        # Remove markdown code blocks (```sql and ```)
        cleaned = re.sub(r'```sql\s*', '', sql_query)
        cleaned = re.sub(r'```\s*', '', cleaned)
        
        # Remove any remaining backticks
        cleaned = re.sub(r'`+', '', cleaned)
        
        # Remove SQL comments (-- style and /* */ style)
        cleaned = re.sub(r'--.*?(?=\n|$)', '', cleaned)  # Single line comments
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)  # Multi-line comments
        
        # Remove extra whitespace and normalize line breaks
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.strip()
        
        return cleaned
    

    
    def collect_post_execution_feedback(self, user_query: str, sql_query: str, user_feedback: str = "", query_results: list = None) -> bool:
        """Collect feedback after query execution with 3-path system"""
        print("\n" + "="*50)
        feedback = input("Rate this query: (g)ood / (f)ix / (w)rong: ").lower().strip()
        
        if feedback == 'g':
            print("✅ Great! Storing this successful query for future reference.")
            self.store_successful_query(user_query, sql_query, user_feedback, query_results)
            return True
        elif feedback == 'f':
            refinement = input("What needs fixing? ").strip()
            print(f"🔧 Noted: '{refinement}' - For now, please ask a new refined question.")
            print("💡 Tip: Try asking something like: 'Previous query but also add [your refinement]'")
            # Store the refinement feedback for future learning
            refinement_feedback = f"Query needed refinement: {refinement}"
            self.store_successful_query(user_query, sql_query, refinement_feedback, query_results)
            return False
        elif feedback == 'w':
            failure_reason = input("What went wrong? (optional): ").strip()
            print("🔄 Thanks for the feedback. Please try rephrasing your question.")
            if failure_reason:
                print(f"💡 I'll remember that: {failure_reason}")
                # Store failure context for learning
                failure_feedback = f"Query failed because: {failure_reason}"
                # Don't store as successful, but could log for learning
            return False
        else:
            print("Please enter 'g', 'f', or 'w'")
            return self.collect_post_execution_feedback(user_query, sql_query, user_feedback, query_results)
    
    def process_query_with_refinement(self, user_query: str, refinement_context: str = "", max_iterations: int = 2) -> Dict[str, Any]:
        """Process query with iterative refinement support"""
        for iteration in range(max_iterations):
            # Build context with refinement if provided
            full_query = user_query
            if refinement_context:
                full_query = f"{user_query}\n\nRefinement needed: {refinement_context}"
            
            # Process the query
            result = self.process_query(full_query)
            if not result.get("success"):
                return result
                
            return result  # Return for orchestrator to handle execution and feedback
        
        return {"success": False, "reason": "Max refinement iterations reached"}

    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a user query through the pipeline - simplified for orchestrator handling"""
        # Step 1: Retrieve relevant context
        context = self.retrieve_relevant_context(user_query)
        if not context:
            return {
                'rephrased_query': user_query,
                'sql_query': None,
                'is_safe': False,
                'reason': "No relevant database context found"
            }
        
        # Step 2: Rephrase query with schema context
        rephrased = self.rephrase_query(user_query, context)
        
        # Step 3: Generate initial SQL
        sql_query = self.generate_sql(rephrased, context)
        
        return {
            'rephrased_query': rephrased,
            'sql_query': sql_query,
            'is_safe': True,  # Safety check will be done in orchestrator
            'reason': "Query generated successfully"
        }