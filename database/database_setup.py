# database/database_setup.py
import sqlite3
import os

# Define the path for the database file relative to this script
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, 'retail_data.db')

def create_connection():
    """Creates a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        print(f"SQLite connection established to {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def create_tables(conn):
    """Creates all tables (existing + new) in the SQLite database."""
    if conn is None:
        print("Cannot create tables without a database connection.")
        return

    cursor = conn.cursor()

    try:
        # --- Demand Forecast Table (From Phase 1/2) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS demand_forecast (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ProductID INTEGER NOT NULL,
            StoreID INTEGER NOT NULL,
            Date TEXT NOT NULL,
            SalesQuantity INTEGER,
            Price REAL,
            Promotion TEXT,
            Seasonality TEXT,
            ExternalFactors TEXT,
            DemandTrend TEXT,
            CustomerSegment TEXT,
            UNIQUE(ProductID, StoreID, Date)
        );
        """)
        print("Table 'demand_forecast' checked/created.")

        # --- Inventory Monitoring Table (From Phase 1/2) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory_monitoring (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ProductID INTEGER NOT NULL,
            StoreID INTEGER NOT NULL,
            StockLevel INTEGER,
            SupplierLeadTimeDays INTEGER,
            StockoutFrequency INTEGER,
            ReorderPoint INTEGER,
            ExpiryDate TEXT,
            WarehouseCapacity INTEGER,
            OrderFulfillmentTimeDays INTEGER,
            LastUpdated TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ProductID, StoreID)
        );
        """)
        print("Table 'inventory_monitoring' checked/created.")

        # --- Pricing Optimization Table (From Phase 1/2) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pricing_optimization (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ProductID INTEGER NOT NULL,
            StoreID INTEGER NOT NULL,
            Price REAL,
            CompetitorPrice REAL,
            DiscountPercentage REAL,
            SalesVolume INTEGER,
            CustomerReviews TEXT,
            ReturnRatePercentage REAL,
            StorageCost REAL,
            ElasticityIndex REAL,
            LastUpdated TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ProductID, StoreID)
        );
        """)
        print("Table 'pricing_optimization' checked/created.")

        # --- Forecast Results Table (From Phase 2) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS forecast_results (
            ForecastID INTEGER PRIMARY KEY AUTOINCREMENT,
            ProductID INTEGER NOT NULL,
            StoreID INTEGER NOT NULL,
            ForecastGeneratedDate TEXT DEFAULT CURRENT_TIMESTAMP,
            ForecastTargetDate TEXT NOT NULL,
            ForecastedQuantity INTEGER NOT NULL,
            ForecastModel TEXT,
            ConfidenceIntervalLower INTEGER,
            ConfidenceIntervalUpper INTEGER,
            UNIQUE(ProductID, StoreID, ForecastTargetDate, ForecastModel)
        );
        """)
        print("Table 'forecast_results' checked/created.")


        # --- *** NEW: Orders Table (For Phase 3) *** ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            OrderID INTEGER PRIMARY KEY AUTOINCREMENT,
            ProductID INTEGER NOT NULL,
            StoreID INTEGER NOT NULL,
            QuantityOrdered INTEGER NOT NULL,
            OrderDate TEXT DEFAULT CURRENT_TIMESTAMP, -- When the recommendation/order was generated
            ExpectedDeliveryDate TEXT,           -- Calculated based on lead time
            ActualDeliveryDate TEXT,             -- To be updated later (e.g., manually or by another process)
            SupplierID INTEGER,                  -- Optional: Link to a suppliers table later
            Status TEXT NOT NULL                 -- e.g., 'Proposed', 'Placed', 'Confirmed', 'Shipped', 'Delivered', 'Cancelled'
        );
        """)
        print("Table 'orders' checked/created.")


        conn.commit()
        print("Tables committed.")

    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            cursor.close()

def initialize_database():
    """Initializes the database by creating connection and tables."""
    conn = create_connection()
    if conn:
        create_tables(conn)
        conn.close()
        print("Database initialization complete.")
    else:
        print("Database initialization failed.")

if __name__ == '__main__':
    # This allows running the script directly to set up the DB
    print("Initializing database schema...")
    initialize_database()