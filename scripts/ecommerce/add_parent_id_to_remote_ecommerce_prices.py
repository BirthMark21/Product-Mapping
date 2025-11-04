#!/usr/bin/env python3
"""
Add parent_product_id to REMOTE ecommerce_prices table in Supabase
Using parent IDs from ecommerce_master table
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

def get_ecommerce_master_data():
    """Get parent-child relationships from ecommerce_master table"""
    
    print("Fetching parent-child relationships from ecommerce_master...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_names
            FROM public.ecommerce_master
            ORDER BY parent_product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("ERROR: No data found in ecommerce_master table!")
                return {}
            
            print(f"SUCCESS: Found {len(df)} parent products in ecommerce_master")
            
            # Create mapping dictionary
            parent_mapping = {}
            for _, row in df.iterrows():
                parent_name = row['parent_product_name']
                parent_id = row['parent_product_id']
                
                # Add parent itself
                parent_mapping[parent_name.lower()] = parent_id
                
                # Add children if they exist
                if row['child_product_names']:
                    try:
                        children = eval(row['child_product_names']) if isinstance(row['child_product_names'], str) else row['child_product_names']
                        for child in children:
                            if child and child.strip():
                                parent_mapping[child.lower().strip()] = parent_id
                    except:
                        pass
            
            return parent_mapping
            
    except Exception as e:
        print(f"ERROR: Error fetching ecommerce_master data: {e}")
        return {}

def add_parent_product_id_to_remote_table(engine, table_name, parent_mapping):
    """Add parent_product_id column to the remote Supabase table"""
    
    print(f"Adding parent_product_id column to remote {table_name}...")
    
    try:
        with engine.connect() as conn:
            # Add the column
            add_column_query = f"ALTER TABLE {table_name} ADD COLUMN parent_product_id TEXT"
            conn.execute(add_column_query)
            conn.commit()
            print(f"  SUCCESS: Added parent_product_id column to {table_name}")
            
            # Get all products from the table
            get_products_query = f"SELECT id, product_name FROM {table_name}"
            products_df = pd.read_sql(get_products_query, conn)
            
            if products_df.empty:
                print(f"  WARNING: No products found in {table_name}")
                return
            
            print(f"  Updating {len(products_df)} products with parent IDs...")
            
            # Update each product with parent ID
            updated_count = 0
            for _, row in products_df.iterrows():
                product_name = row['product_name']
                product_id = row['id']
                
                # Find parent ID for this product
                parent_id = None
                for mapped_name, mapped_id in parent_mapping.items():
                    if product_name.lower().strip() == mapped_name:
                        parent_id = mapped_id
                        break
                
                if parent_id:
                    update_query = f"""
                    UPDATE {table_name} 
                    SET parent_product_id = %s 
                    WHERE id = %s
                    """
                    conn.execute(update_query, (parent_id, product_id))
                    updated_count += 1
                else:
                    print(f"    WARNING: No parent found for: {product_name}")
            
            conn.commit()
            print(f"  SUCCESS: Updated {updated_count} products with parent IDs")
            
    except Exception as e:
        print(f"  ERROR: Error adding parent_product_id to {table_name}: {e}")

def verify_parent_assignments(engine, table_name):
    """Verify parent_product_id assignments"""
    
    print(f"Verifying parent_product_id assignments in {table_name}...")
    
    try:
        with engine.connect() as conn:
            # Get sample of products with parent assignments
            query = f"""
            SELECT product_name, parent_product_id 
            FROM {table_name} 
            WHERE parent_product_id IS NOT NULL 
            LIMIT 10
            """
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                print(f"  Sample of {table_name} products with parent assignments:")
                for _, row in df.iterrows():
                    print(f"    YES {row['product_name']} -> {row['parent_product_id']}")
            
            # Get summary statistics
            summary_query = f"""
            SELECT 
                COUNT(*) as total_products,
                COUNT(parent_product_id) as with_parent_id,
                COUNT(*) - COUNT(parent_product_id) as without_parent_id
            FROM {table_name}
            """
            summary_df = pd.read_sql(summary_query, conn)
            
            if not summary_df.empty:
                row = summary_df.iloc[0]
                print(f"  Summary:")
                print(f"    Total products: {row['total_products']}")
                print(f"    With parent ID: {row['with_parent_id']}")
                print(f"    Without parent ID: {row['without_parent_id']}")
                
    except Exception as e:
        print(f"  ERROR: Error verifying parent_product_id assignments: {e}")

def main():
    """Main function"""
    print("Adding parent_product_id to REMOTE ecommerce_prices table in Supabase")
    print("USING PARENT IDs FROM ECOMMERCE_MASTER TABLE")
    print("=" * 60)
    
    try:
        # Step 1: Get parent-child relationships from ecommerce_master
        parent_mapping = get_ecommerce_master_data()
        if not parent_mapping:
            print("No parent mapping found. Cannot proceed.")
            return
        
        # Step 2: Get Supabase engine
        supabase_engine = get_db_engine('supabase')
        
        # Step 3: Add parent_product_id to remote table
        add_parent_product_id_to_remote_table(supabase_engine, 'ecommerce_prices', parent_mapping)
        
        # Step 4: Verify assignments
        print("\nVerification Results:")
        print("=" * 40)
        verify_parent_assignments(supabase_engine, 'ecommerce_prices')
        
        print("\nSUCCESS: Parent product ID assignment completed for remote ecommerce_prices!")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()
