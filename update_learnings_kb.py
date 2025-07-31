#!/usr/bin/env python3
"""
Script to update the learnings knowledge base with new successful queries
Run this periodically to keep the RAG system updated with latest learnings
"""
import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_store import VectorStore

def main():
    """Update the learnings knowledge base"""
    print("🔄 Updating learnings knowledge base...")
    
    try:
        vector_store = VectorStore(verbose=True)
        vector_store.create_learnings_knowledge_base()
        print("✅ Learnings knowledge base updated successfully!")
        
    except Exception as e:
        print(f"❌ Failed to update learnings KB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()