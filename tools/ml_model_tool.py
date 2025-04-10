# tools/ml_model_tool.py
from datetime import datetime, timedelta

import pandas as pd
# --- REMOVE sqlite3, os, datetime if only used by moved functions ---
# import sqlite3
# import os
# from datetime import datetime, timedelta
import joblib

# Add project root to sys.path to find utils
import sys
import os # Keep os for path joining
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)


# --- Updated Import ---
# Import helper functions from the new utility file
from utils.data_utils import connect_db, get_historical_data, simple_average_forecast


MODEL_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

# --- load_model function remains the same ---
def load_model(model_filename):
    # ... (implementation as before) ...
    path = os.path.join(MODEL_DIR, model_filename)
    if os.path.exists(path):
        try:
            model = joblib.load(path)
            print(f"Loaded model from {path}")
            return model
        except Exception as e:
            print(f"Error loading model from {path}: {e}")
            return None
    else:
        # print(f"Model file not found: {path}") # Less verbose
        return None


# --- Tool function ---
def predict_demand(product_id: int, store_id: int, forecast_days: int = 7, history_days: int = 90) -> list[tuple[str, int]] | None:
    """
    Predicts demand using the best available method.
    Currently uses simple average, placeholder for real models.
    Now uses helper functions from data_utils.
    """
    print(f"--- ML Tool: Predicting demand for P:{product_id} S:{store_id} for {forecast_days} days ---")

    # --- Placeholder: Try loading a specific model ---
    model_filename = f"forecast_model_p{product_id}_s{store_id}.joblib"
    trained_model = load_model(model_filename)
    if trained_model:
        # --- Logic to use the loaded trained_model ---
        print("--- ML Tool: Found trained model (using it - Placeholder Logic) ---")
        # Example: Returning dummy data for now, replace with actual prediction
        # This part needs implementation based on your model's expected input/output
        dummy_forecast_value = 10 # Placeholder
        forecasts = [( (datetime.now() + timedelta(days=i+1)).strftime('%Y-%m-%d'), dummy_forecast_value ) for i in range(forecast_days)]
        return forecasts
        # pass # Implement actual model usage here


    # --- Fallback to simple average if no model or model fails ---
    print("--- ML Tool: No trained model found/loaded. Using fallback simple average forecast ---")
    conn = connect_db() # Use imported function
    if not conn:
        print("--- ML Tool: DB connection failed for fallback ---")
        return None

    # Use imported functions
    historical_data = get_historical_data(conn, product_id, store_id, history_days=history_days)
    forecasts = simple_average_forecast(historical_data, forecast_days=forecast_days)

    if conn:
        conn.close()

    return forecasts