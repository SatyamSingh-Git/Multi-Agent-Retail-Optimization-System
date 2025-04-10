# agents/replenishment_agent.py
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
        print(f"Error connecting to database in replenishment agent: {e}")
        return None

def get_forecast_data(conn, product_id, store_id, start_date, end_date):
    """Retrieves forecasted quantities within a date range."""
    if not conn: return 0
    try:
        query = """
        SELECT SUM(ForecastedQuantity) as TotalForecast
        FROM forecast_results
        WHERE ProductID = ?
          AND StoreID = ?
          AND ForecastTargetDate BETWEEN ? AND ?;
        """
        cursor = conn.cursor()
        cursor.execute(query, (product_id, store_id, start_date, end_date))
        result = cursor.fetchone()
        cursor.close()
        # Handle case where no forecast exists for the period
        total_forecast = result[0] if result and result[0] is not None else 0
        # print(f"Forecast P:{product_id} S:{store_id} between {start_date}-{end_date}: {total_forecast}")
        return total_forecast
    except Exception as e:
        print(f"Error fetching forecast data for P:{product_id} S:{store_id}: {e}")
        return 0 # Return 0 forecast on error

def get_lead_time(conn, product_id, store_id):
     """Retrieves supplier lead time."""
     if not conn: return 0
     try:
        # Assuming lead time is stored per product/store in inventory_monitoring
        query = "SELECT SupplierLeadTimeDays FROM inventory_monitoring WHERE ProductID = ? AND StoreID = ?"
        cursor = conn.cursor()
        cursor.execute(query, (product_id, store_id))
        result = cursor.fetchone()
        cursor.close()
        lead_time = int(result[0]) if result and result[0] is not None else 0 # Default to 0 if missing
        # print(f"Lead time for P:{product_id} S:{store_id}: {lead_time}")
        return lead_time
     except Exception as e:
        print(f"Error fetching lead time for P:{product_id} S:{store_id}: {e}")
        return 0

def calculate_replenishment(conn, inventory_status_item):
    """Calculates replenishment quantity based on status, forecast, and lead time."""
    proposal = None # Default to no order proposal
    pid = inventory_status_item.get('product_id')
    sid = inventory_status_item.get('store_id')
    flags = inventory_status_item.get('flags', [])
    stock = inventory_status_item.get('stock_level')
    rop = inventory_status_item.get('reorder_point')
    # capacity = inventory_status_item.get('warehouse_capacity') # Use later if needed

    # --- Trigger condition: Only propose orders for LOW_STOCK items ---
    if "LOW_STOCK" not in flags or stock is None or rop is None:
        # print(f"Replenishment skipped for P:{pid} S:{sid}. Status not LOW_STOCK or data missing.")
        return proposal # No order needed or possible

    print(f"Calculating replenishment for LOW_STOCK item P:{pid} S:{sid} (Stock:{stock}, ROP:{rop})")

    # --- Calculate Demand during Lead Time ---
    lead_time_days = get_lead_time(conn, pid, sid)
    if lead_time_days <= 0:
        print(f"Warning: Lead time missing or zero for P:{pid} S:{sid}. Cannot calculate demand during lead time accurately.")
        # Could default to ordering ROP - stock, but safer to skip or use simple logic
        order_qty = max(0, rop - stock) # Simple fallback: order minimum to reach ROP
    else:
        forecast_start_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        forecast_end_date = (datetime.now() + timedelta(days=lead_time_days)).strftime('%Y-%m-%d')
        demand_during_lead_time = get_forecast_data(conn, pid, sid, forecast_start_date, forecast_end_date)

        # --- Simple Replenishment Logic: Order up to ROP + Demand during Lead Time ---
        # Target Stock = Reorder Point + Forecasted Demand during Lead Time
        # Order Quantity = Target Stock - Current Stock
        # Ensure order quantity is not negative
        target_stock = rop + demand_during_lead_time
        order_qty = max(0, int(round(target_stock - stock)))

    if order_qty > 0:
        print(f"  Lead Time: {lead_time_days} days")
        print(f"  Demand during Lead Time (Forecasted): {demand_during_lead_time}")
        print(f"  Target Stock (ROP + Lead Time Demand): {target_stock}")
        print(f"  Calculated Order Quantity: {order_qty}")

        # --- Add Capacity Check (Optional for now) ---
        # available_capacity = capacity - stock if capacity is not None else float('inf')
        # if order_qty > available_capacity:
        #    print(f"  Warning: Order Qty ({order_qty}) exceeds available capacity ({available_capacity}). Adjusting.")
        #    order_qty = max(0, int(available_capacity))

        if order_qty > 0:
             proposal = {
                 "ProductID": pid,
                 "StoreID": sid,
                 "QuantityOrdered": order_qty,
                 "OrderDate": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                 "Status": "Proposed", # Initial status
                 "LeadTimeDays": lead_time_days # Include for supplier agent
             }
             print(f"  Proposed Order: {proposal}")
        else:
             print(f"  Order quantity is zero after adjustments. No order proposed.")

    else:
         print(f"  Calculated order quantity is zero. No order proposed.")


    return proposal


def run_replenishment_calculation(inventory_status_dict):
    """
    Runs replenishment calculations for items based on their inventory status.

    Args:
        inventory_status_dict (dict): The dictionary output from the inventory monitor node.

    Returns:
        dict: Contains 'proposed_orders' (list of order dicts) and summary counts.
    """
    conn = connect_db()
    if not conn:
        return {"error": "Database connection failed", "proposed_orders": []}

    proposed_orders = []
    processed_count = 0
    proposal_count = 0

    # Remove the summary key before iterating
    status_items = {k: v for k, v in inventory_status_dict.items() if k != 'summary'}

    try:
        for item_key, status_info in status_items.items():
            processed_count += 1
            # Ensure status_info is a dictionary before proceeding
            if isinstance(status_info, dict):
                proposal = calculate_replenishment(conn, status_info)
                if proposal:
                    proposed_orders.append(proposal)
                    proposal_count += 1
            else:
                 print(f"Warning: Skipping item {item_key} due to unexpected status format: {status_info}")


    finally:
        if conn:
            conn.close()

    print(f"\nReplenishment calculation finished. Processed: {processed_count}, Proposed Orders: {proposal_count}")
    return {
        "processed_count": processed_count,
        "proposal_count": proposal_count,
        "proposed_orders": proposed_orders
        }

# Example usage (for testing)
if __name__ == '__main__':
     print("Running Replenishment Agent directly...")
     # Example: Simulate inventory status for testing
     # You would normally get this from the inventory monitor agent output
     test_inventory_status = {
         (9286, 16): {'status': 'EXPIRED', 'flags': ['EXPIRED'], 'product_id': 9286, 'store_id': 16, 'stock_level': 0, 'reorder_point': 132},
         (2605, 60): {'status': 'LOW_STOCK', 'flags': ['LOW_STOCK', 'EXPIRED'], 'product_id': 2605, 'store_id': 60, 'stock_level': 50, 'reorder_point': 127}, # Simulate LOW_STOCK
         (2859, 55): {'status': 'EXCESS_STOCK', 'flags': ['EXCESS_STOCK', 'LOW_STOCK', 'EXPIRED'], 'product_id': 2859, 'store_id': 55, 'stock_level': 50, 'reorder_point': 192}, # Simulate LOW_STOCK
         (2374, 24): {'status': 'OK', 'flags': ['OK'], 'product_id': 2374, 'store_id': 24, 'stock_level': 30, 'reorder_point': 19}, # OK stock
         'summary': {'processed_count': 4}
     }
     # Assume forecasts exist for Product 2605/Store 60 and 2859/55
     # You might need to manually add forecast data or adjust the test data

     replenishment_results = run_replenishment_calculation(test_inventory_status)
     print("\nDirect Run Results:")
     import json
     print(json.dumps(replenishment_results, indent=2))