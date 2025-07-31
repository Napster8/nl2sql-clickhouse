# src/dspy_modules.py
import dspy
import os
from src.config import Config
import google.generativeai as genai

# Configure Google API
genai.configure(api_key=Config.GEMINI_API_KEY)

class GeminiFlash(dspy.LM):
    """DSPy module for Gemini Flash 2.5"""
    def __init__(self, model="gemini-2.5-flash", thinking_budget=0, **kwargs):
        super().__init__(model=model, **kwargs)
        self.thinking_budget = thinking_budget

# Initialize model using proper DSPy Gemini integration with thinking budget
lm = GeminiFlash(
    model=Config.DSPY_MODEL, 
    thinking_budget=10000,  # Enable thinking mode with 10k tokens for reasoning
    max_tokens=12000,       # Increased output tokens
    temperature=0.1         # Slightly increase temperature to avoid repetition
)
dspy.settings.configure(lm=lm)

class QueryAnalysisAndRephrasing(dspy.Signature):
    """
    Analyze user query and rephrase into a structured ClickHouse SQL problem with intent classification.
    """
    user_query = dspy.InputField(desc="User's natural language query about ClickHouse data")
    context = dspy.InputField(desc="Available database schema with tables and columns", default="")
    
    # Analysis outputs
    query_intent = dspy.OutputField(desc="Intent classification: analytical, operational, exploratory, or diagnostic")
    data_entities = dspy.OutputField(desc="List of tables, columns, or metrics mentioned or implied")
    time_dimension = dspy.OutputField(desc="Time range, aggregation period, or temporal aspect if any")
    aggregation_type = dspy.OutputField(desc="Required aggregations: count, sum, avg, percentiles, etc.")
    filters_conditions = dspy.OutputField(desc="Filtering conditions or WHERE clause requirements")
    
    # Enhanced rephrasing
    rephrased_query = dspy.OutputField(desc="Complete, unambiguous SQL problem statement with ClickHouse-specific considerations using actual table and column names from the schema")
    complexity_level = dspy.OutputField(desc="Query complexity: simple, moderate, or complex")
    suggested_optimizations = dspy.OutputField(desc="ClickHouse-specific optimization hints for the query")

class SQLGeneration(dspy.Signature):
    """Generate SQL from query and context, optionally incorporating user feedback.
    
    Think step by step:
    1. Analyze the required data and user feedback
    2. Identify all necessary tables and their relationships
    3. Plan the JOINs needed to connect tables
    4. Consider user preferences (names vs IDs, formatting, etc.)
    5. Generate the complete SQL with proper JOINs
    6. IMPORTANT: If previous_queries are provided, generate a DIFFERENT query that addresses the feedback
    """
    rephrased_query = dspy.InputField(desc="Rephrased SQL problem statement")
    context = dspy.InputField(desc="Relevant database schema context with table relationships")
    user_feedback = dspy.InputField(desc="User feedback for improving the SQL query", default="")
    previous_queries = dspy.InputField(desc="Previously tried SQL queries that should be avoided", default="")
    sql_query = dspy.OutputField(desc="Complete SQL query with proper JOINs that addresses user feedback. MUST be different from previous_queries if provided. If user wants readable names instead of IDs, find the appropriate lookup tables and JOIN them.")

class SQLSafetyCheck(dspy.Signature):
    """Check SQL for dangerous operations."""
    sql_query = dspy.InputField(desc="SQL query to check")
    is_safe = dspy.OutputField(desc="Boolean indicating if query is safe")
    reason = dspy.OutputField(desc="Explanation if unsafe")

class QueryRephrasingModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.analyze_and_rephrase = dspy.ChainOfThought(QueryAnalysisAndRephrasing)  # Use ChainOfThought for better analysis
    
    def forward(self, user_query, context=""):
        result = self.analyze_and_rephrase(user_query=user_query, context=context)
        
        # Return structured analysis along with rephrased query
        return {
            'rephrased_query': result.rephrased_query,
            'intent': result.query_intent,
            'entities': result.data_entities,
            'time_dimension': result.time_dimension,
            'aggregations': result.aggregation_type,
            'filters': result.filters_conditions,
            'complexity': result.complexity_level,
            'optimizations': result.suggested_optimizations
        }

class SQLGenerationModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generate = dspy.ChainOfThought(SQLGeneration)  # Use ChainOfThought for better reasoning
    
    def forward(self, rephrased_query, context, user_feedback="", previous_queries=""):
        return self.generate(rephrased_query=rephrased_query, context=context, user_feedback=user_feedback, previous_queries=previous_queries)

class SQLSafetyCheckModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.check = dspy.Predict(SQLSafetyCheck)
    
    def forward(self, sql_query):
        return self.check(sql_query=sql_query)