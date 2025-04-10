# config/settings.py
import os
from dotenv import load_dotenv

load_dotenv() # Optional: Load .env file if you use one for secrets

# --- Ollama Configuration ---
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434") # Default if not set in env
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "deepseek-r1:8b") # Or "mistral:latest", etc.
OLLAMA_EMBEDDING_MODEL = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text:latest")

# --- Other Settings ---
# Example: Web scraping target (use placeholders, highly site-specific)
COMPETITOR_PRICE_URL_TEMPLATE = "https://example-competitor.com/products/{product_sku}" # Placeholder!