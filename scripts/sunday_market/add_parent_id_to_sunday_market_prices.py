#!/usr/bin/env python3
"""
Script to add parent_product_id column to local sunday_market_prices table
Uses parent IDs from canonical_products_master table to ensure consistency
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys
import uuid
import re

# Add the parent directory to the path so we can import config and utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

# Use the same namespace UUID as canonical master
NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

def get_sunday_market_master_data():
    """Get parent-child relationships from sunday_market_master table"""
    
    print("Fetching parent-child relationships from sunday_market_master...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_names
            FROM public.sunday_market_master
            ORDER BY parent_product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("ERROR: No data found in sunday_market_master table!")
                return {}
            
            print(f"SUCCESS: Found {len(df)} parent products in sunday_market_master")
            
            # Create mapping dictionary
            parent_mapping = {}
            for _, row in df.iterrows():
                parent_name = row['parent_product_name']
                parent_id = row['parent_product_id']
                
                # Add parent itself
                parent_mapping[parent_name.lower().strip()] = {
                    'parent_name': parent_name,
                    'parent_id': parent_id
                }
                
                # Add all child names
                if row['child_product_names']:
                    try:
                        import ast
                        child_names = ast.literal_eval(row['child_product_names'])
                        for child_name in child_names:
                            parent_mapping[child_name.lower().strip()] = {
                                'parent_name': parent_name,
                                'parent_id': parent_id
                            }
                    except:
                        pass
            
            return parent_mapping
            
    except Exception as e:
        print(f"ERROR: Error fetching sunday_market_master data: {e}")
        return {}

def add_parent_product_id_to_table(engine, table_name, parent_mapping):
    """Add parent_product_id column to the specified table"""
    
    print(f"🔧 Adding parent_product_id column to {table_name}...")
    
    try:
        with engine.connect() as conn:
            # Check if column already exists
            check_column_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}' 
            AND column_name = 'parent_product_id'
            """
            result = pd.read_sql(check_column_query, conn)
            
            if not result.empty:
                print(f"  ⚠️  parent_product_id column already exists in {table_name}")
                # Drop existing column first
                drop_column_query = f"ALTER TABLE public.{table_name} DROP COLUMN IF EXISTS parent_product_id"
                conn.execute(text(drop_column_query))
                conn.commit()
                print(f"  🗑️  Dropped existing parent_product_id column")
            
            # Add parent_product_id column
            add_column_query = f"ALTER TABLE public.{table_name} ADD COLUMN parent_product_id TEXT"
            conn.execute(text(add_column_query))
            conn.commit()
            print(f"  SUCCESS: Added parent_product_id column to {table_name}")
            
            # Get all products from the table
            get_products_query = f"SELECT id, product_name FROM public.{table_name}"
            products_df = pd.read_sql(get_products_query, conn)
            
            if products_df.empty:
                print(f"  ⚠️  No products found in {table_name}")
                return True
            
            print(f"  📝 Updating {len(products_df)} products with parent IDs...")
            
            # Update each product with parent_product_id
            updated_count = 0
            for _, row in products_df.iterrows():
                product_name = row['product_name']
                product_id = row['id']
                
                # Clean product name for matching
                cleaned_name = re.sub(r'\s+', ' ', str(product_name)).strip().lower()
                
                # Find parent mapping
                if cleaned_name in parent_mapping:
                    parent_info = parent_mapping[cleaned_name]
                    parent_id = parent_info['parent_id']
                    
                    # Update the record
                    update_query = text(f"""
                        UPDATE public.{table_name} 
                        SET parent_product_id = :parent_id 
                        WHERE id = :product_id
                    """)
                    
                    conn.execute(update_query, {
                        'parent_id': parent_id,
                        'product_id': product_id
                    })
                    conn.commit()
                    updated_count += 1
                else:
                    print(f"    WARNING: No parent found for: {product_name}")
            
            print(f"  SUCCESS: Updated {updated_count} products with parent IDs")
            return True
            
    except Exception as e:
        print(f"  ERROR: Error adding parent_product_id to {table_name}: {e}")
        return False

def verify_parent_product_ids(engine, table_name):
    """Verify parent_product_id assignments"""
    
    print(f"🔍 Verifying parent_product_id assignments in {table_name}...")
    
    try:
        with engine.connect() as conn:
            # Get products with their parent IDs
            verify_query = f"""
            SELECT 
                product_name,
                parent_product_id,
                CASE 
                    WHEN parent_product_id IS NOT NULL THEN 'YES' 
                    ELSE 'NO' 
                END as has_parent
            FROM public.{table_name}
            ORDER BY product_name
            LIMIT 10
            """
            verify_df = pd.read_sql(verify_query, conn)
            
            if not verify_df.empty:
                print(f"  Sample of {table_name} products with parent assignments:")
                for _, row in verify_df.iterrows():
                    print(f"    {row['has_parent']} {row['product_name']} -> {row['parent_product_id']}")
            
            # Count products with and without parent IDs
            count_query = f"""
            SELECT 
                COUNT(*) as total_products,
                COUNT(parent_product_id) as products_with_parent,
                COUNT(*) - COUNT(parent_product_id) as products_without_parent
            FROM public.{table_name}
            """
            count_df = pd.read_sql(count_query, conn)
            
            if not count_df.empty:
                row = count_df.iloc[0]
                print(f"  Summary:")
                print(f"    Total products: {row['total_products']}")
                print(f"    With parent ID: {row['products_with_parent']}")
                print(f"    Without parent ID: {row['products_without_parent']}")
            
    except Exception as e:
        print(f"  ERROR: Error verifying parent_product_id assignments: {e}")

def main():
    """Main function"""
    print("Adding parent_product_id to LOCAL sunday_market_prices table")
    print("USING PARENT IDs FROM SUNDAY_MARKET_MASTER TABLE")
    print("=" * 60)
    
    try:
        # Step 1: Get parent-child relationships from sunday_market_master
        parent_mapping = get_sunday_market_master_data()
        if not parent_mapping:
            print("No parent mapping found. Cannot proceed.")
            return
        
        # Step 2: Get local database engine
        local_engine = get_db_engine('hub')
        
        # Step 3: Add parent_product_id to sunday_market_prices table
        if add_parent_product_id_to_table(local_engine, 'sunday_market_prices', parent_mapping):
            print("\nVerification Results:")
            print("=" * 40)
            verify_parent_product_ids(local_engine, 'sunday_market_prices')
            print("\nSUCCESS: Parent product ID assignment completed for sunday_market_prices!")
        else:
            print("\nERROR: Failed to add parent_product_id to sunday_market_prices table.")
        
    except Exception as e:
        print(f"\nERROR: Error in main process: {e}")

if __name__ == "__main__":
    main()
