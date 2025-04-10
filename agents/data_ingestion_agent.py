# agents/data_ingestion_agent.py
import pandas as pd
import sqlite3
import os

# Database path (relative to the main script location)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'retail_data.db')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

def connect_db():
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database in agent: {e}")
        return None

def clean_date(date_str):
    """Attempts to convert various date formats to YYYY-MM-DD."""
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        # Handle potential Excel date serial numbers if necessary
        if isinstance(date_str, (int, float)):
             # Assuming standard Excel epoch, might need adjustment for Mac Excel
             return pd.to_datetime('1899-12-30') + pd.to_timedelta(date_str, 'D')
        # Otherwise, proceed with robust string parsing
        dt = pd.to_datetime(date_str, errors='coerce')
        return dt.strftime('%Y-%m-%d') if pd.notna(dt) else None
    except Exception as e:
        print(f"Warning: Could not parse date '{date_str}'. Error: {e}")
        return None

def load_and_insert_data(conn, csv_filename, table_name, column_mapping=None, date_columns=None):
    """Loads data from a CSV, cleans it, and inserts into the specified table."""
    csv_path = os.path.join(DATA_DIR, csv_filename)
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return False

    try:
        # Specify dtype={'Customer Reviews': str} or similar if needed for specific columns
        df = pd.read_csv(csv_path)
        print(f"Read {len(df)} rows from {csv_filename}")

        # --- Basic Cleaning ---
        # 1. Strip whitespace from column names FIRST
        original_columns = df.columns.tolist()
        df.columns = df.columns.str.strip()
        cleaned_columns = df.columns.tolist()
        if original_columns != cleaned_columns:
            print(f"Cleaned column names: {cleaned_columns}")


        # 2. Rename columns based on the provided mapping *after* stripping
        if column_mapping:
            df.rename(columns=column_mapping, inplace=True)
            print(f"Columns after mapping for {csv_filename}: {df.columns.tolist()}")
        else:
             print(f"No column mapping provided for {csv_filename}. Columns: {df.columns.tolist()}")


        # --- Data Type Conversion and Specific Cleaning ---

        # Get target table columns to ensure required ones exist *after* renaming
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        # Store as dictionary {name: type} for easier lookup
        table_column_info = {info[1]: info[2] for info in cursor.fetchall()}
        table_columns = list(table_column_info.keys())
        cursor.close()
        print(f"Target table '{table_name}' columns: {table_columns}")

        # Check if essential columns (like PKs, FKs, NOT NULL) exist in the DataFrame *after* mapping
        required_cols = ['ProductID', 'StoreID'] # Add other critical NOT NULL columns if needed
        missing_required = [col for col in required_cols if col in table_columns and col not in df.columns]
        if missing_required:
            print(f"CRITICAL ERROR: Required column(s) {missing_required} not found in DataFrame for {csv_filename} after mapping. Check CSV headers and column_mapping.")
            print(f"Available DataFrame columns: {df.columns.tolist()}")
            return False # Stop processing this file

        # Format date columns specifically
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    print(f"Cleaning date column: {col}")
                    df[col] = df[col].apply(clean_date)
                    # Ensure dates that couldn't be parsed are None (NULL in DB)
                    df[col] = df[col].where(pd.notna(df[col]), None)
                else:
                     print(f"Warning: Date column '{col}' specified but not found in DataFrame for {csv_filename}.")

        # Handle potential missing values for non-date columns
        # Iterate through columns expected by the DB table
        for col_name, col_type in table_column_info.items():
            if col_name in df.columns: # Process only if column exists in DF
                # Fill numeric columns
                if df[col_name].dtype in ['int64', 'float64'] or 'INT' in col_type.upper() or 'REAL' in col_type.upper():
                    # Decide fill value: 0 is common, but None (NULL) might be better if 0 has meaning
                    df.fillna({col_name:0}, inplace=True) # Or use df[col_name] = df[col_name].where(pd.notna(df[col_name]), None)
                # Fill text columns (but avoid date columns already processed)
                elif df[col_name].dtype == 'object' and (not date_columns or col_name not in date_columns):
                    # Replace NaN/None with 'Unknown' or None (for NULL in DB)
                    df.fillna({col_name:'Unknown'}, inplace=True)# Or use df[col_name] = df[col_name].where(pd.notna(df[col_name]), None)
            # else: Column exists in DB but not DF - will be handled by DB default or raise error if NOT NULL


        # **** MODIFICATION ****
        # 1. REMOVE the explicit DataFrame column filtering step.
        # df = df[[col for col in df.columns if col in table_columns]] # <-- REMOVED/COMMENTED OUT

        # 2. Change if_exists to 'replace' to avoid UNIQUE constraint errors on re-runs during dev.
        #    WARNING: This deletes all previous data in the table before inserting.
        #    Use 'append' and handle conflicts (e.g., INSERT OR IGNORE) for production/incremental loads.
        insert_behavior = 'replace' # Or 'append' if you implement conflict handling
        print(f"Using if_exists='{insert_behavior}' for table '{table_name}'")

        # Keep only columns that actually exist in the target table before inserting
        # This prevents errors if the CSV has extra columns pandas tries to map
        df_to_insert = df[[col for col in df.columns if col in table_columns]].copy()
        print(f"Final columns being inserted into '{table_name}': {df_to_insert.columns.tolist()}")


        # Check for required columns again just before insert
        missing_required_final = [col for col in required_cols if col in table_columns and col not in df_to_insert.columns]
        if missing_required_final:
            print(f"CRITICAL ERROR: Required column(s) {missing_required_final} are missing JUST BEFORE INSERT. This should not happen.")
            return False


        df_to_insert.to_sql(table_name, conn, if_exists=insert_behavior, index=False)

        print(f"Data from {csv_filename} inserted/replaced into table '{table_name}'.")
        return True

    except pd.errors.EmptyDataError:
        print(f"Warning: CSV file {csv_filename} is empty.")
        return False
    except FileNotFoundError:
        print(f"Error: File not found {csv_path}")
        return False
    except sqlite3.IntegrityError as e:
         print(f"DATABASE ERROR during insert into {table_name}: {e}")
         print("This might be due to NOT NULL constraints (check if required columns like ProductID/StoreID exist and have values)")
         print(f"DataFrame columns at time of error: {df.columns.tolist()}") # Print DF columns before final filtering
         if 'df_to_insert' in locals():
             print(f"Columns attempted to insert: {df_to_insert.columns.tolist()}") # Columns actually sent to SQL
             # print(df_to_insert.isnull().sum()) # Check for nulls in critical columns
         import traceback
         traceback.print_exc()
         return False
    except Exception as e:
        print(f"UNEXPECTED ERROR processing file {csv_filename}: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging
        return False


def run_data_ingestion():
    """Main function to run the data ingestion process."""
    conn = connect_db()
    if not conn:
        return {"status": "Failed", "error": "Database connection failed"}

    ingestion_status = {}
    success = True

    try:
        # --- Demand Forecast ---
        # ** CRITICAL FIX **: Keys MUST match the exact headers from the CSV file
        #    as shown in the error log: ['Product ID', 'Date', 'Store ID', ...]
        map_demand = {
            'Product ID': 'ProductID',        # Use 'Product ID' (with space) as the key
            'Store ID': 'StoreID',          # Use 'Store ID' (with space) as the key
            'Date': 'Date',
            'Sales Quantity': 'SalesQuantity', # Use 'Sales Quantity' (with space)
            'Price': 'Price',
            'Promotion': 'Promotion',        # Assuming the CSV header is 'Promotion' after stripping space
            'Seasonality Factors': 'Seasonality', # Use 'Seasonality Factors' (with space)
            'External Factors': 'ExternalFactors', # Use 'External Factors' (with space)
            'Demand Trend': 'DemandTrend',     # Use 'Demand Trend' (with space)
            'Customer Segments': 'CustomerSegment' # Use 'Customer Segments' (plural, with space)
            # Add any other necessary mappings here if headers differ from DB columns
        }
        date_cols_demand = ['Date']
        print(f"\n--- Processing demand_forcast.csv ---") # Added separator for clarity
        status_demand = load_and_insert_data(conn,
                                             'demand_forcast.csv',
                                             'demand_forecast',
                                             column_mapping=map_demand,
                                             date_columns=date_cols_demand)
        ingestion_status['demand_forecast'] = status_demand
        if not status_demand: success = False

        # --- Inventory Monitoring ---
        # Assuming these mappings are correct based on successful run previously
        map_inventory = {
            'Product ID': 'ProductID',
            'Store ID': 'StoreID',
            'Stock Levels': 'StockLevel',
            'Supplier Lead Time (days)': 'SupplierLeadTimeDays',
            'Stockout Frequency': 'StockoutFrequency',
            'Reorder Point': 'ReorderPoint',
            'Expiry Date': 'ExpiryDate',
            'Warehouse Capacity': 'WarehouseCapacity',
            'Order Fulfillment Time (days)': 'OrderFulfillmentTimeDays'
        }
        date_cols_inventory = ['ExpiryDate']
        print(f"\n--- Processing inventory_monitoring.csv ---") # Added separator
        status_inventory = load_and_insert_data(conn,
                                                'inventory_monitoring.csv',
                                                'inventory_monitoring',
                                                column_mapping=map_inventory,
                                                date_columns=date_cols_inventory)
        ingestion_status['inventory_monitoring'] = status_inventory
        if not status_inventory: success = False # Though it succeeded last time, keep check

        # --- Pricing Optimization ---
         # Assuming these mappings are correct based on successful run previously
        map_pricing = {
            'Product ID': 'ProductID',
            'Store ID': 'StoreID',
            'Price': 'Price', # Added explicit mapping for Price if header is 'Price'
            'Competitor Prices': 'CompetitorPrice',
            'Discounts': 'DiscountPercentage',
            'Sales Volume': 'SalesVolume',
            'Customer Reviews': 'CustomerReviews',
            'Return Rate (%)': 'ReturnRatePercentage',
            'Storage Cost': 'StorageCost',
            'Elasticity Index': 'ElasticityIndex'
        }
        print(f"\n--- Processing pricing_optimization.csv ---") # Added separator
        status_pricing = load_and_insert_data(conn,
                                              'pricing_optimization.csv',
                                              'pricing_optimization',
                                              column_mapping=map_pricing)
        ingestion_status['pricing_optimization'] = status_pricing
        if not status_pricing: success = False # Though it succeeded last time, keep check

    finally:
        if conn:
            conn.close()

    final_status = "Success" if success else "Partial or Failed"
    print(f"\nData ingestion process completed with status: {final_status}")
    return {"status": final_status, "details": ingestion_status}



if __name__ == '__main__':
     # Allow running this agent directly for testing
     print("Running Data Ingestion Agent directly...")
     run_data_ingestion()