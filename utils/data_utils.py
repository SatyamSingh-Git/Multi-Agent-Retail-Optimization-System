# utils/data_utils.py
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta

# Database path (relative to this utils directory)
# Adjust if your utils directory is elsewhere relative to database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'retail_data.db')

def connect_db():
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database in data_utils: {e}")
        return None

def get_historical_data(conn, product_id, store_id, history_days=90):
    """Retrieves recent historical sales data for a specific product and store."""
    if not conn:
        print("DB connection not provided to get_historical_data")
        return None
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=history_days)).strftime('%Y-%m-%d')

        query = """
        SELECT Date, SalesQuantity
        FROM demand_forecast
        WHERE ProductID = ? AND StoreID = ? AND Date BETWEEN ? AND ?
        ORDER BY Date DESC;
        """
        df = pd.read_sql_query(query, conn, params=(product_id, store_id, start_date, end_date))
        df['Date'] = pd.to_datetime(df['Date'])
        # Print only if data is found for less verbosity
        if not df.empty:
            print(f"Retrieved {len(df)} historical sales records for Product {product_id}, Store {store_id}.")
        return df
    except Exception as e:
        print(f"Error fetching historical data for P:{product_id} S:{store_id}: {e}")
        return None

def simple_average_forecast(historical_df, forecast_days=7):
    """Generates a simple forecast based on the average of historical sales."""
    if historical_df is None or historical_df.empty:
        # print("No historical data available for forecasting.") # Less verbose
        # Return a default forecast (e.g., 0 or a predefined baseline)
        return [( (datetime.now() + timedelta(days=i+1)).strftime('%Y-%m-%d'), 0 ) for i in range(forecast_days)]

    historical_df['SalesQuantity'] = pd.to_numeric(historical_df['SalesQuantity'], errors='coerce')
    average_sales = historical_df['SalesQuantity'].mean()

    if pd.isna(average_sales):
        # print("Warning: Average sales calculation resulted in NaN. Using 0.")
        average_sales = 0
    else:
         average_sales = int(round(average_sales))

    # print(f"Calculated average daily sales: {average_sales}") # Less verbose

    forecasts = []
    last_historical_date = historical_df['Date'].max() if not historical_df.empty else datetime.now()

    for i in range(forecast_days):
        target_date = (last_historical_date + timedelta(days=i + 1)).strftime('%Y-%m-%d')
        forecasts.append((target_date, average_sales))

    return forecasts