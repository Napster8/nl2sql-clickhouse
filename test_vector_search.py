#!/usr/bin/env python3

from src.vector_store import VectorStore

def test_search():
    vs = VectorStore()
    
    try:
        # Test search
        query = "revenue sales amount payment"
        results = vs.search(query, top_k=5)
        
        print(f"Search results type: {type(results)}")
        print(f"Number of results: {len(results) if results else 0}")
        
        if results:
            print("\nFirst result:")
            print(results[0])
            
            print("\nAll results:")
            for i, result in enumerate(results):
                print(f"{i+1}. {result}")
        else:
            print("No results returned")
            
    except Exception as e:
        print(f"Error during search: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_search()