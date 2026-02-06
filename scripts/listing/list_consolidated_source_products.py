
import pandas as pd
from sqlalchemy import create_engine
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.db_connector import get_db_engine

def list_consolidated_products():
    """
    Reads the 'all_product_master_Table' from the local Hub database
    and lists all products in a single table format in the terminal.
    """
    try:
        hub_engine = get_db_engine('hub')
        
        print("\n🚀 FETCHING CONSOLIDATED PRODUCTS FROM LOCAL DATABASE...")
        
        with hub_engine.connect() as conn:
            # We created this table in the last step of the standardization pipeline
            query = "SELECT source_product_name, assign_parent_name, \"Has parent?\" FROM \"all_product_master_Table\" ORDER BY source_product_name"
            df = pd.read_sql(query, conn)
            
        if df.empty:
            print("⚠️ No consolidated products found. Please run pipeline/standardization.py first.")
            return

        print("\n" + "="*80)
        print(f"📦 CONSOLIDATED PRODUCT LIST (Total: {len(df)})")
        print("="*80)
        
        # Format for terminal
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', 50)
        pd.set_option('display.width', 1000)
        
        print(df.to_string(index=False))
        
        print("="*80)
        print(f"✅ Displayed {len(df)} products.")
        print("   Status Column: ✅ = In Mapping JSON, ❌ = Not yet in Mapping JSON")
        print("="*80)

    except Exception as e:
        print(f"❌ Error listing products: {e}")

if __name__ == "__main__":
    list_consolidated_products()
