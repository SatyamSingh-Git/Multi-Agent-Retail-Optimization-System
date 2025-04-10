# agents/supplier_agent.py
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
        print(f"Error connecting to database in supplier agent: {e}")
        return None

def place_simulated_order(conn, order_proposal):
    """Simulates placing an order by adding it to the orders table."""
    if not conn or not order_proposal:
        return None

    pid = order_proposal.get("ProductID")
    sid = order_proposal.get("StoreID")
    qty = order_proposal.get("QuantityOrdered")
    lead_time = order_proposal.get("LeadTimeDays", 0) # Get lead time passed from replenishment

    if not all([pid, sid, qty]):
        print(f"Error: Missing required fields in order proposal: {order_proposal}")
        return None

    cursor = conn.cursor()
    try:
        order_date_dt = datetime.now()
        expected_delivery_dt = order_date_dt + timedelta(days=lead_time)
        order_date_str = order_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        expected_delivery_str = expected_delivery_dt.strftime('%Y-%m-%d')

        cursor.execute("""
        INSERT INTO orders (ProductID, StoreID, QuantityOrdered, OrderDate, ExpectedDeliveryDate, Status)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (pid, sid, qty, order_date_str, expected_delivery_str, "Placed")) # Status is now 'Placed'

        order_id = cursor.lastrowid # Get the ID of the inserted order
        conn.commit()
        print(f"Simulated order placed for P:{pid} S:{sid}, Qty:{qty}. OrderID: {order_id}, Expected Delivery: {expected_delivery_str}")
        return {
            "OrderID": order_id,
            "ProductID": pid,
            "StoreID": sid,
            "QuantityOrdered": qty,
            "ExpectedDeliveryDate": expected_delivery_str,
            "Status": "Placed"
            }
    except sqlite3.Error as e:
        print(f"Database error placing order for P:{pid} S:{sid}: {e}")
        conn.rollback()
        return None
    finally:
        if cursor:
            cursor.close()


def run_supplier_interaction(proposed_orders):
    """
    Simulates interaction with suppliers for proposed orders.

    Args:
        proposed_orders (list): A list of order dictionaries from the replenishment agent.

    Returns:
        dict: Contains 'placed_orders' (list of confirmed placed order dicts) and summary.
    """
    conn = connect_db()
    if not conn:
        return {"error": "Database connection failed", "placed_orders": []}

    placed_orders = []
    processed_count = 0
    placed_count = 0

    try:
        for proposal in proposed_orders:
            processed_count += 1
            placement_result = place_simulated_order(conn, proposal)
            if placement_result:
                placed_orders.append(placement_result)
                placed_count += 1
    finally:
        if conn:
            conn.close()

    print(f"\nSupplier interaction simulation finished. Processed proposals: {processed_count}, Orders Placed: {placed_count}")
    return {
        "processed_count": processed_count,
        "placed_count": placed_count,
        "placed_orders": placed_orders # Return details of orders placed
        }

# Example usage (for testing)
if __name__ == '__main__':
     print("Running Supplier Interaction Agent directly...")
     # Example: Simulate proposed orders
     test_proposed_orders = [
         {'ProductID': 2605, 'StoreID': 60, 'QuantityOrdered': 100, 'Status': 'Proposed', 'LeadTimeDays': 11},
         {'ProductID': 2859, 'StoreID': 55, 'QuantityOrdered': 150, 'Status': 'Proposed', 'LeadTimeDays': 25}
     ]

     supplier_results = run_supplier_interaction(test_proposed_orders)
     print("\nDirect Run Results:")
     import json
     print(json.dumps(supplier_results, indent=2))