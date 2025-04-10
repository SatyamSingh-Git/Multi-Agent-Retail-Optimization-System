# agents/inventory_monitoring_agent.py
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta

# Database path (relative)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'retail_data.db')

def connect_db():
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database in monitor agent: {e}")
        return None

def get_inventory_data(conn, product_id, store_id):
    """Retrieves current inventory details for a specific product and store."""
    # *** Add WarehouseCapacity to the SELECT statement ***
    if not conn: return None
    try:
        query = """
        SELECT ProductID, StoreID, StockLevel, ReorderPoint, ExpiryDate, WarehouseCapacity
        FROM inventory_monitoring
        WHERE ProductID = ? AND StoreID = ?;
        """
        df = pd.read_sql_query(query, conn, params=(product_id, store_id))
        if df.empty:
            print(f"No inventory record found for P:{product_id} S:{store_id}")
            return None
        else:
             return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Error fetching inventory data for P:{product_id} S:{store_id}: {e}")
        return None

def check_inventory_status(inventory_data, expiry_threshold_days=30, excess_stock_multiplier=2.0):
    """Analyzes inventory data and returns status flags, including EXCESS_STOCK."""
    if not inventory_data:
        return {"status": "NO_DATA", "flags": ["NO_DATA"], "details": "Inventory record missing."}

    status_flags = set()
    details = {}

    stock_level = pd.to_numeric(inventory_data.get('StockLevel'), errors='coerce')
    reorder_point = pd.to_numeric(inventory_data.get('ReorderPoint'), errors='coerce')
    expiry_date_str = inventory_data.get('ExpiryDate')

    # --- Stock Level Check ---
    can_check_low_stock = not pd.isna(stock_level) and not pd.isna(reorder_point)
    can_check_excess_stock = can_check_low_stock and reorder_point > 0 # Avoid division by zero or meaningless check

    if pd.isna(stock_level):
        status_flags.add("STOCK_LEVEL_UNKNOWN")
        details["stock_check"] = "Stock level data missing or invalid."
    else:
        is_low = False
        is_excess = False
        if can_check_low_stock and stock_level <= reorder_point:
            status_flags.add("LOW_STOCK")
            details["stock_check"] = f"Stock ({stock_level}) is at or below reorder point ({reorder_point})."
            is_low = True
        # *** NEW: Excess Stock Check ***
        if can_check_excess_stock and stock_level > (reorder_point * excess_stock_multiplier):
             status_flags.add("EXCESS_STOCK")
             details["excess_check"] = f"Stock ({stock_level}) is significantly above reorder point ({reorder_point} * {excess_stock_multiplier})."
             is_excess = True

        if not is_low and not is_excess:
             status_flags.add("OK")
             details["stock_check"] = f"Stock level ({stock_level}) appears normal relative to reorder point ({reorder_point})."
        elif not is_low and is_excess:
            # If it's excess but not low, primary status might lean towards OK unless action needed
            status_flags.add("OK") # Still functionally 'OK' but flagged as excess

    # --- Expiry Date Check (Keep existing logic) ---
    if expiry_date_str and expiry_date_str != 'Unknown':
        try:
            expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d')
            threshold_date = datetime.now() + timedelta(days=expiry_threshold_days)
            if expiry_date <= datetime.now():
                status_flags.add("EXPIRED")
                details["expiry_check"] = f"Item expired on {expiry_date_str}."
                status_flags.discard("OK") # Expired overrides OK
            elif expiry_date <= threshold_date:
                status_flags.add("NEAR_EXPIRY")
                details["expiry_check"] = f"Item expiring soon ({expiry_date_str})."
            else:
                 details["expiry_check"] = f"Expiry date ({expiry_date_str}) is OK."
        except ValueError:
            details["expiry_check"] = f"Invalid expiry date format: {expiry_date_str}."
            status_flags.add("INVALID_EXPIRY_DATE")
    else:
        details["expiry_check"] = "No expiry date information."


    # --- Determine Primary Status (Adjust priority if needed) ---
    primary_status = "UNKNOWN"
    if "NO_DATA" in status_flags: primary_status = "NO_DATA"
    elif "STOCK_LEVEL_UNKNOWN" in status_flags: primary_status = "UNKNOWN"
    elif "EXPIRED" in status_flags: primary_status = "EXPIRED" # Highest priority actionable status
    elif "LOW_STOCK" in status_flags: primary_status = "LOW_STOCK" # Next priority
    elif "NEAR_EXPIRY" in status_flags: primary_status = "NEAR_EXPIRY"
    elif "EXCESS_STOCK" in status_flags: primary_status = "EXCESS_STOCK" # Lower priority than near expiry? Debatable.
    elif "OK" in status_flags: primary_status = "OK" # Lowest priority


    return {
        "status": primary_status,
        "flags": sorted(list(status_flags)), # Return sorted list for consistency
        "details": details,
        "product_id": inventory_data.get('ProductID'),
        "store_id": inventory_data.get('StoreID'),
        # Pass essential data onwards
        "stock_level": stock_level if not pd.isna(stock_level) else None,
        "reorder_point": reorder_point if not pd.isna(reorder_point) else None,
        "warehouse_capacity": pd.to_numeric(inventory_data.get('WarehouseCapacity'), errors='coerce')
        }


def run_inventory_monitoring(items_to_monitor):
    """
    Runs the inventory monitoring process for a list of product/store combinations.

    Args:
        items_to_monitor (list): A list of tuples, where each tuple is (product_id, store_id).

    Returns:
        dict: A dictionary where keys are (product_id, store_id) tuples and
              values are the status dictionaries from check_inventory_status.
              Includes a summary count.
    """
    conn = connect_db()
    if not conn:
        return {"error": "Database connection failed"}

    all_statuses = {}
    processed_count = 0

    try:
        for product_id, store_id in items_to_monitor:
            print(f"\n--- Monitoring Inventory for Product: {product_id}, Store: {store_id} ---")
            processed_count +=1
            inventory_data = get_inventory_data(conn, product_id, store_id)
            status_result = check_inventory_status(inventory_data)
            all_statuses[(product_id, store_id)] = status_result
            print(f"Status: {status_result.get('status')}, Flags: {status_result.get('flags')}")

    finally:
        if conn:
            conn.close()

    print(f"\nInventory monitoring finished. Processed: {processed_count}")
    all_statuses['summary'] = {'processed_count': processed_count}
    return all_statuses


# Example usage (for testing)
if __name__ == '__main__':
     print("Running Inventory Monitoring Agent directly...")
     # Use the same items as forecasting or other valid pairs
     items = [(4277, 1), (5540, 10), (9286, 16)] # Replace with valid pairs from your DB
     if not items:
          print("Please provide valid (ProductID, StoreID) pairs for testing.")
     else:
        monitoring_results = run_inventory_monitoring(items)
        print("\nDirect Run Results:")
        import json
        print(json.dumps(monitoring_results, indent=2))