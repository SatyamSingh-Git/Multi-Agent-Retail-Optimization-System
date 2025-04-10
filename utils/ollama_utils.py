# utils/ollama_utils.py
import ollama
import sys
import os

# Add project root to path to import config (adjust if your structure differs)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from config import settings # Import settings

# Initialize Ollama client (consider making this a singleton if used heavily)
try:
    client = ollama.Client(host=settings.OLLAMA_BASE_URL)
    print(f"Ollama client initialized for host: {settings.OLLAMA_BASE_URL}")
    # Optional: Check connection by listing models
    # client.list()
except Exception as e:
    print(f"Failed to initialize Ollama client: {e}. Ensure Ollama is running.")
    client = None # Set client to None if initialization fails


def get_ollama_completion(prompt: str, model: str = settings.OLLAMA_LLM_MODEL, temperature: float = 0.1) -> str | None:
    """Gets a completion from the specified Ollama LLM."""
    if not client:
        print("Error: Ollama client not available.")
        return None
    try:
        print(f"\n--- Sending Prompt to Ollama ({model}) ---")
        # print(f"Prompt: {prompt[:200]}...") # Optional: Log truncated prompt
        response = client.chat(
            model=model,
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': temperature}
        )
        completion = response['message']['content'].strip()
        print(f"--- Ollama Response Received ---")
        # print(f"Completion: {completion[:200]}...") # Optional: Log truncated response
        return completion
    except Exception as e:
        print(f"Error getting Ollama completion: {e}")
        return None

def get_ollama_embedding(text: str, model: str = settings.OLLAMA_EMBEDDING_MODEL) -> list[float] | None:
    """Gets embeddings from the specified Ollama embedding model."""
    if not client:
        print("Error: Ollama client not available.")
        return None
    try:
        # print(f"--- Getting Embedding from Ollama ({model}) ---") # Can be verbose
        response = client.embeddings(model=model, prompt=text)
        # print("--- Embedding Received ---")
        return response['embedding']
    except Exception as e:
        print(f"Error getting Ollama embedding for text '{text[:50]}...': {e}")
        return None

# Example usage (for testing connection)
if __name__ == '__main__':
    print("Testing Ollama Utilities...")
    test_prompt = "Explain the concept of supply chain optimization in one sentence."
    completion = get_ollama_completion(test_prompt)
    if completion:
        print(f"\nTest Completion for '{test_prompt}':\n{completion}")
    else:
        print("\nFailed to get test completion.")

    test_text = "Retail inventory management"
    embedding = get_ollama_embedding(test_text)
    if embedding:
        print(f"\nTest Embedding for '{test_text}':\n{embedding[:5]}... (vector length: {len(embedding)})")
    else:
        print("\nFailed to get test embedding.")