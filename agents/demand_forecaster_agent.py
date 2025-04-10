# agents/demand_forecaster_agent.py
import pandas as pd
import sqlite3
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from datetime import datetime, timedelta

from utils.ollama_utils import get_ollama_completion # Import LLM util
from tools.ml_model_tool import predict_demand # Import the new tool
from utils.data_utils import connect_db # Import connect_db from utils

# Database path (relative)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'retail_data.db')

def connect_db():
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database in forecaster agent: {e}")
        return None


def store_forecast_results(conn, product_id, store_id, forecasts, model_name="SimpleAvg"):
    """Stores the generated forecasts in the database."""
    if not conn or not forecasts:
        print(f"Warning: Cannot store forecast for P:{product_id} S:{store_id}. Connection or forecasts missing.")
        return False
    cursor = conn.cursor()
    inserted_count = 0
    try:
        for target_date, qty in forecasts:
            # Use INSERT OR REPLACE to update if a forecast for this combo already exists
            cursor.execute("""
            INSERT OR REPLACE INTO forecast_results
            (ProductID, StoreID, ForecastTargetDate, ForecastedQuantity, ForecastModel, ForecastGeneratedDate)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (product_id, store_id, target_date, qty, model_name))
        conn.commit()
        inserted_count = len(forecasts)
        if inserted_count > 0: # Avoid printing if nothing was inserted
             print(f"Stored {inserted_count} forecast records for P:{product_id} S:{store_id} (Model: {model_name}).")
        return True
    except sqlite3.Error as e:
        print(f"Database error storing forecast for P:{product_id} S:{store_id}: {e}")
        conn.rollback() # Rollback changes on error
        return False
    finally:
        if cursor:
            cursor.close()


# --- run_demand_forecasting remains largely the same, calling predict_demand ---
def run_demand_forecasting(items_to_forecast):
    """
    Runs the demand forecasting process for a list of product/store combinations.
    Uses ML tool for baseline forecast and optionally adjusts with LLM based on context.
    """
    results = {'processed_count': 0, 'success_count': 0, 'failed_items': []}
    all_forecasts_generated = {} # Store forecasts keyed by (pid, sid)

    for product_id, store_id in items_to_forecast:
        print(f"\n--- Forecasting for Product: {product_id}, Store: {store_id} ---")
        results['processed_count'] += 1
        final_forecasts = None
        store_success = False
        model_used = "ML_Tool_Default" # Default model name

        try:
            # --- Call the ML Tool to get baseline forecast ---
            # predict_demand now uses helpers from data_utils internally
            generated_forecasts = predict_demand(product_id, store_id, forecast_days=7)

            if generated_forecasts:
                # Assume simple average was fallback if no trained model used yet
                # (The tool's internal logic determines this)
                # We'll refine model_used name based on actual tool behavior later if needed
                model_used = "ML_Tool_SimpleAvg"

                # --- Optional LLM Adjustment Logic ---
                # Example: Fetch context factors (needs implementation or hardcoding)
                # context_factors = get_context_factors_from_db(connect_db(), product_id, store_id)
                context_factors = f"ExternalFactors: Economic-Stable, DemandTrend: Increasing, Seasonality: None" # Hardcoded example

                current_avg_forecast = 0
                if generated_forecasts: # Ensure list is not empty
                     try:
                         current_avg_forecast = sum(qty for _, qty in generated_forecasts) / len(generated_forecasts)
                     except ZeroDivisionError:
                         current_avg_forecast = 0


                prompt = f"""
                Analyze the potential impact of these factors on future demand for Product ID {product_id} at Store ID {store_id}.
                Baseline forecast (next 7 days avg): {current_avg_forecast:.1f} units/day.
                Contextual factors: {context_factors}

                Based *only* on the contextual factors provided, should the baseline forecast be slightly adjusted up or down?
                Respond with only ONE of these words: INCREASE, DECREASE, or NONE. Do not explain your reasoning.
                """
                adjustment_suggestion = get_ollama_completion(prompt, temperature=0.0)

                adjustment_factor = 1.0
                if adjustment_suggestion == "INCREASE":
                    adjustment_factor = 1.1 # Simple 10% increase
                    model_used += "+LLM_Incr"
                    print(f"  LLM suggested: INCREASE forecast (Factor: {adjustment_factor:.2f})")
                elif adjustment_suggestion == "DECREASE":
                    adjustment_factor = 0.9 # Simple 10% decrease
                    model_used += "+LLM_Decr"
                    print(f"  LLM suggested: DECREASE forecast (Factor: {adjustment_factor:.2f})")
                else:
                     print(f"  LLM suggested: NO adjustment (Raw Response: '{adjustment_suggestion}')")
                     model_used += "+LLM_None"

                # Apply adjustment to the generated forecast list
                final_forecasts = [(date, max(0, int(round(qty * adjustment_factor)))) for date, qty in generated_forecasts]
                # --- End Optional LLM Adjustment ---

            else: # If predict_demand returned None or empty list
                 print(f"  ML Tool failed to generate baseline forecast for P:{product_id} S:{store_id}")
                 final_forecasts = None


            # --- Store the final forecast results ---
            if final_forecasts:
                conn_store = connect_db() # Get DB connection for storing
                if conn_store:
                    store_success = store_forecast_results(conn_store, product_id, store_id, final_forecasts, model_name=model_used)
                    conn_store.close() # Close connection after storing
                else:
                     print(f"  Error: DB connection failed, cannot store forecast results for P:{product_id} S:{store_id}.")
                     store_success = False # Mark as failed if cannot connect to store

                if store_success:
                    results['success_count'] += 1
                    all_forecasts_generated[(product_id, store_id)] = final_forecasts
                else:
                    results['failed_items'].append((product_id, store_id))
                    all_forecasts_generated[(product_id, store_id)] = None # Mark as failed
            else:
                 # Handle case where no forecast could be generated or adjusted
                 print(f"  Failed to generate final forecast for P:{product_id} S:{store_id}")
                 results['failed_items'].append((product_id, store_id))
                 all_forecasts_generated[(product_id, store_id)] = None # Mark as failed

        except Exception as e:
             print(f"!! Unhandled Error in forecasting loop for P:{product_id} S:{store_id}: {e}")
             import traceback
             traceback.print_exc() # Print detailed traceback for debugging
             results['failed_items'].append((product_id, store_id))
             all_forecasts_generated[(product_id, store_id)] = None # Mark as failed


    results['forecasts'] = all_forecasts_generated # Add the actual forecasts to the result dict
    print(f"\nForecasting finished. Processed: {results['processed_count']}, Succeeded: {results['success_count']}")
    return results

# Example usage (for testing)
if __name__ == '__main__':
     print("Running Demand Forecaster Agent directly...")
     # Example: Forecast for Product 4277 / Store 1 and Product 5540 / Store 10
     # You might need to adjust these IDs based on your actual data
     # Need to get valid ProductID/StoreID pairs from your data
     items = [(4277, 1), (5540, 10)] # Replace with valid pairs from your DB
     if not items:
         print("Please provide valid (ProductID, StoreID) pairs for testing.")
     else:
        forecasting_results = run_demand_forecasting(items)
        print("\nDirect Run Results:")
        import json
        # Avoid printing the potentially large 'forecasts' part in summary
        summary_results = {k: v for k, v in forecasting_results.items() if k != 'forecasts'}
        print(json.dumps(summary_results, indent=2))