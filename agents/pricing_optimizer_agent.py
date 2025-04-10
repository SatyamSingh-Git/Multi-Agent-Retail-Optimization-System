# agents/pricing_optimizer_agent.py
import pandas as pd
import sqlite3
import os
from datetime import datetime
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Add project root

from utils.ollama_utils import get_ollama_completion # Import LLM util
from tools.web_scraper_tool import get_competitor_price # Import scraper tool

# Database path (relative)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'retail_data.db')

def connect_db():
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database in pricing agent: {e}")
        return None

def get_current_pricing(conn, product_id, store_id):
    """Retrieves current pricing details including reviews.""" # Modified docstring
    if not conn: return None
    try:
        # Add CustomerReviews to SELECT
        query = """
        SELECT ProductID, StoreID, Price, CompetitorPrice, StorageCost, ElasticityIndex, CustomerReviews
        FROM pricing_optimization
        WHERE ProductID = ? AND StoreID = ?;
        """
        df = pd.read_sql_query(query, conn, params=(product_id, store_id))
        if df.empty:
            print(f"No pricing record found for P:{product_id} S:{store_id}")
            return None
        else:
             return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Error fetching pricing data for P:{product_id} S:{store_id}: {e}")
        return None


def recommend_pricing_action(inventory_status_item, pricing_data):
    """Recommends pricing actions using inventory status, competitor prices, and review sentiment."""
    proposal = None
    pid = inventory_status_item.get('product_id')
    sid = inventory_status_item.get('store_id')
    flags = inventory_status_item.get('flags', [])

    if not pricing_data:
        # print(f"Pricing action skipped for P:{pid} S:{sid}. Pricing data missing.") # Already printed in caller
        return proposal

    current_price = pd.to_numeric(pricing_data.get('Price'), errors='coerce')
    customer_reviews = pricing_data.get('CustomerReviews', '') # Get reviews text

    if pd.isna(current_price) or current_price <= 0:
         print(f"Pricing action skipped for P:{pid} S:{sid}. Invalid current price ({current_price}).")
         return proposal

    discount_percentage = 0.0
    price_change_factor = 1.0 # Use factor for increase/decrease
    reason = []
    llm_sentiment = "NEUTRAL" # Default sentiment

    # --- Trigger Conditions: NEAR_EXPIRY or EXCESS_STOCK ---
    trigger_flags = {"NEAR_EXPIRY", "EXCESS_STOCK"}
    if trigger_flags.intersection(flags):
        if "NEAR_EXPIRY" in flags:
            discount_percentage = max(discount_percentage, 0.15) # Higher discount for expiry
            reason.append("Near Expiry")
            print(f"  Pricing trigger: NEAR_EXPIRY for P:{pid} S:{sid}")
        if "EXCESS_STOCK" in flags:
            discount_percentage = max(discount_percentage, 0.10) # Base discount for excess
            reason.append("Excess Stock")
            print(f"  Pricing trigger: EXCESS_STOCK for P:{pid} S:{sid}")
    else:
         # print(f"No primary trigger flags for P:{pid} S:{sid}")
         pass # No discount needed based on core inventory flags

    # --- LLM Sentiment Analysis ---
    if customer_reviews and isinstance(customer_reviews, str) and customer_reviews != 'Unknown' and len(customer_reviews) > 10: # Avoid analyzing short/default text
         prompt = f"""
         Analyze the sentiment expressed ONLY regarding the PRICE or VALUE of the product in the following customer reviews.
         Ignore comments about quality, shipping, etc. if they don't relate to price/value.
         Reviews: "{customer_reviews}"

         Respond with only ONE word: POSITIVE, NEGATIVE, or NEUTRAL.
         """
         sentiment_result = get_ollama_completion(prompt, temperature=0.0)
         if sentiment_result in ["POSITIVE", "NEGATIVE", "NEUTRAL"]:
             llm_sentiment = sentiment_result
             print(f"  LLM Review Sentiment (Price/Value): {llm_sentiment}")
             reason.append(f"ReviewSentiment:{llm_sentiment}")
         else:
              print(f"  LLM Review Sentiment: Unexpected response - {sentiment_result}")
    else:
        print("  LLM Review Sentiment: No valid reviews to analyze.")


    # --- Competitor Price Check (Tool) ---
    # Assume product_identifier could be SKU or name from another table later
    competitor_price = get_competitor_price(f"product_{pid}") # Use mock tool
    if competitor_price:
         print(f"  Competitor Price Found: {competitor_price}")
         reason.append(f"CompPrice:{competitor_price}")
         # Basic competitive pricing logic:
         if current_price > competitor_price * 1.1: # If we are >10% higher
             print("  Action: Potential discount suggested due to higher price than competitor.")
             discount_percentage = max(discount_percentage, 0.05) # Suggest at least 5% off
         elif competitor_price > current_price * 1.1: # If competitor is >10% higher
              # Could potentially INCREASE price if sentiment is not negative and no discount needed
              if discount_percentage == 0 and llm_sentiment != "NEGATIVE":
                  print("  Action: Potential price increase suggested due to lower price than competitor.")
                  price_change_factor = 1.05 # Suggest 5% increase
                  reason.append("PriceIncrease")


    # --- Combine Factors ---
    final_price = current_price
    if price_change_factor > 1.0: # Handle increase
         final_price = round(current_price * price_change_factor, 2)
         print(f"  Applying price increase factor {price_change_factor}")
    elif discount_percentage > 0: # Handle discount
         # Modify discount based on sentiment?
         if llm_sentiment == "NEGATIVE":
             discount_percentage *= 1.2 # Increase discount if price sentiment is negative
         final_price = round(current_price * (1 - discount_percentage), 2)
         print(f"  Applying discount percentage {discount_percentage*100:.1f}%")


    # --- Propose Action if Price Changed ---
    if final_price != current_price:
        proposal = {
            "ProductID": pid,
            "StoreID": sid,
            "CurrentPrice": current_price,
            "RecommendedPrice": final_price,
            "Reason": ", ".join(reason),
            "GeneratedDate": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "Status": "Proposed"
        }
        print(f"  Proposed Pricing Action: Recommend changing price to {final_price} (Reason: {proposal['Reason']})")
    else:
        print(f"  No price change recommended for P:{pid} S:{sid}.")


    return proposal


# --- Update run_pricing_optimization ---
def run_pricing_optimization(inventory_status_dict):
    """Runs pricing optimization using inventory status, LLM sentiment, and competitor prices."""
    conn = connect_db()
    if not conn:
        return {"error": "Database connection failed", "proposed_actions": []}

    proposed_actions = []
    processed_count = 0
    proposal_count = 0

    status_items = {k: v for k, v in inventory_status_dict.items() if k != 'summary'}

    try:
        for item_key, status_info in status_items.items():
            processed_count += 1
            if isinstance(status_info, dict) and 'product_id' in status_info and 'store_id' in status_info:
                 pid = status_info['product_id']
                 sid = status_info['store_id']
                 print(f"\n--- Optimizing Pricing for P:{pid} S:{sid} ---")
                 pricing_data = get_current_pricing(conn, pid, sid) # Fetches reviews too now
                 proposal = recommend_pricing_action(status_info, pricing_data) # Uses LLM, scraper tool
                 if proposal:
                    proposed_actions.append(proposal)
                    proposal_count += 1
            else:
                print(f"Warning: Skipping pricing check for item {item_key} due to unexpected status format: {status_info}")
    finally:
        if conn:
            conn.close()

    print(f"\nPricing optimization finished. Processed: {processed_count}, Proposed Actions: {proposal_count}")
    return { "processed_count": processed_count, "proposal_count": proposal_count, "proposed_actions": proposed_actions }

# Example usage (for testing)
if __name__ == '__main__':
     print("Running Pricing Optimizer Agent directly...")
     # Use the same test inventory status as replenishment agent
     test_inventory_status = {
         (9286, 16): {'status': 'EXPIRED', 'flags': ['EXPIRED'], 'product_id': 9286, 'store_id': 16},
         (2605, 60): {'status': 'LOW_STOCK', 'flags': ['LOW_STOCK', 'EXPIRED'], 'product_id': 2605, 'store_id': 60},
         (2859, 55): {'status': 'EXCESS_STOCK', 'flags': ['EXCESS_STOCK', 'NEAR_EXPIRY'], 'product_id': 2859, 'store_id': 55}, # Simulate Excess & Near Expiry
         (2374, 24): {'status': 'OK', 'flags': ['OK', 'EXCESS_STOCK'], 'product_id': 2374, 'store_id': 24}, # Simulate OK but Excess
         'summary': {'processed_count': 4}
     }
     # Assume pricing data exists for products 2859/55 and 2374/24
     # You might need to manually add pricing data or adjust test data

     pricing_results = run_pricing_optimization(test_inventory_status)
     print("\nDirect Run Results:")
     import json
     print(json.dumps(pricing_results, indent=2))