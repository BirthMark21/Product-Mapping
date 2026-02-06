"""
List ALL products from Supabase products table (No filtering, no uniqueness)
"""
import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from utils.db_connector import get_db_engine

def list_supabase_products():
    """List ALL product names from Supabase products table"""
    print("\n🌐 FETCHING ALL PRODUCTS FROM SUPABASE (Full List)...")
    
    engine = get_db_engine('supabase')
    
    try:
        with engine.connect() as conn:
            # Fetch all names from the products table
            df = pd.read_sql('SELECT name FROM products ORDER BY name', conn)
            
        if df.empty:
            print("❌ No products found in Supabase.")
            return

        product_list = df['name'].tolist()

        print(f"✅ Found {len(product_list)} total rows in Supabase products table.\n")
        
        print("=" * 60)
        print(f"{'#':<4} | {'PRODUCT NAME'}")
        print("-" * 60)
        
        for i, name in enumerate(product_list, 1):
            print(f"{i:<4} | {name}")
        
        print("=" * 60)
        print(f"📊 TOTAL: {len(product_list)} Products listed.")
        print("-" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    list_supabase_products()
