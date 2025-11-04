#!/usr/bin/env python3
"""
Export parent products from sunday_market_master table
"""

import sys
import os
import pandas as pd
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def export_parent_products():
    """Export parent products from sunday_market_master table"""
    
    print("Exporting parent products from sunday_market_master table")
    print("=" * 60)
    
    try:
        # Get database engine
        engine = get_db_engine('hub')
        
        with engine.connect() as conn:
            # Query to get parent products
            query = """
            SELECT 
                parent_product_id,
                parent_product_name
            FROM public.sunday_market_master
            ORDER BY parent_product_name
            """
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("ERROR: No parent products found in sunday_market_master table!")
                return
            
            print(f"SUCCESS: Found {len(df)} parent products")
            
            # Create exports folder if it doesn't exist
            exports_folder = "exports"
            os.makedirs(exports_folder, exist_ok=True)
            
            # Export to CSV
            output_file = os.path.join(exports_folder, "sunday_market_parent_products.csv")
            df.to_csv(output_file, index=False)
            print(f"SUCCESS: Exported to {output_file}")
            
            # Display sample data
            print("\nSample parent products:")
            print("-" * 40)
            for i, row in df.head(10).iterrows():
                print(f"ID: {row['parent_product_id']} | Name: {row['parent_product_name']}")
            
            if len(df) > 10:
                print(f"... and {len(df) - 10} more")
                
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    export_parent_products()
