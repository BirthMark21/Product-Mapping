import os
import sys
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

# Add the root directory to the path so we can import utils
# Script is in scripts/listing/, so go up two levels
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def list_all_local_products():
    print("Listing all product names from local pgAdmin tables...")
    engine = get_db_engine('hub')
    
    if not engine:
        print("❌ Failed to connect to local PostgreSQL.")
        return

    # Verified Table mapping from local hub
    table_configs = {
        "products": "name",
        "clickhouse_product_names": "name",
        "clickhouse_products": "name"
    }
    
    try:
        with engine.connect() as conn:
            for table_name, name_col in table_configs.items():
                print(f"\n" + "=" * 60)
                print(f"📦 TABLE: {table_name}")
                print("=" * 60)
                
                try:
                    query = f'SELECT "{name_col}" FROM public."{table_name}" ORDER BY "{name_col}"'
                    df = pd.read_sql(query, conn)
                    
                    if not df.empty:
                        for i, name in enumerate(df[name_col], 1):
                            # Handle potential None/NaN values
                            display_name = str(name) if name is not None else "N/A"
                            print(f"{i:4d}. {display_name}")
                        print(f"\n✅ Total: {len(df)} entries in {table_name}")
                    else:
                        print("⚠️ No data in table.")
                except Exception as e:
                    print(f"❌ Error reading {table_name}: {e}")
            
    except Exception as e:
        print(f"❌ Connection error: {e}")

if __name__ == "__main__":
    list_all_local_products()
