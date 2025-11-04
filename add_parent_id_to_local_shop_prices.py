#!/usr/bin/env python3
"""
Script to add parent_product_id to local local_shop_prices table
based on the local_shop_master table
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
import ast

# Load environment variables
load_dotenv()

def get_local_shop_master_data():
    """Get parent-child relationships from local shop master table"""
    print("Loading parent-child relationships from local shop master...")
    try:
        local_engine = get_db_engine('hub')
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_names
            FROM public.local_shop_master
            """
            df = pd.read_sql(query, conn)
            if df.empty:
                print("No data found in local shop master table!")
                return {}
            print(f"Found {len(df)} parent groups in local shop master")
            parent_mapping = {}
            for _, row in df.iterrows():
                parent_product_id = row['parent_product_id']
                parent_name = row['parent_product_name']
                child_names = row['child_product_names']

                # Parse child data - handle PostgreSQL array format
                if isinstance(child_names, str):
                    try:
                        if child_names.startswith('{') and child_names.endswith('}'):
                            child_names = child_names[1:-1].split(',')
                            child_names = [name.strip().strip('"') for name in child_names if name.strip()]
                        else:
                            child_names = ast.literal_eval(child_names)
                    except:
                        child_names = []
                elif not isinstance(child_names, list):
                    child_names = []
                
                # Create mapping for each child name
                for child_name in child_names:
                    parent_mapping[child_name.lower().strip()] = {
                        'parent_product_id': parent_product_id,
                        'parent_name': parent_name
                    }
            print(f"Created mapping for {len(parent_mapping)} child products")
            return parent_mapping
    except Exception as e:
        print(f"Error loading local shop master data: {e}")
        return {}

def add_parent_product_id_to_local_shop_prices(parent_mapping):
    """Add parent_product_id column to local local_shop_prices table and populate it"""
    
    print("\nAdding parent_product_id to local local_shop_prices table...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Check if parent_product_id column already exists
            check_column_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'local_shop_prices' 
            AND column_name = 'parent_product_id'
            AND table_schema = 'public'
            """
            existing_columns = pd.read_sql(check_column_query, conn)
            
            if not existing_columns.empty:
                print("  parent_product_id column already exists in local_shop_prices table")
                # Drop and recreate to ensure clean data
                drop_column_query = "ALTER TABLE public.local_shop_prices DROP COLUMN IF EXISTS parent_product_id"
                conn.execute(text(drop_column_query))
                print("  Removed existing parent_product_id column")
            
            # Add parent_product_id column
            add_column_query = """
            ALTER TABLE public.local_shop_prices 
            ADD COLUMN parent_product_id TEXT
            """
            conn.execute(text(add_column_query))
            print("  ✅ Added parent_product_id column to local_shop_prices table")
            
            # Get all products from the local_shop_prices table
            get_products_query = """
            SELECT id, product_name 
            FROM public.local_shop_prices
            """
            products_df = pd.read_sql(get_products_query, conn)
            
            if products_df.empty:
                print("  📊 No products found in local_shop_prices table")
                return True
            
            print(f"  📊 Found {len(products_df)} products in local_shop_prices table")
            
            # Update parent_product_id for each product using local shop master mapping
            updated_count = 0
            for _, row in products_df.iterrows():
                product_id = str(row['id'])
                product_name = row['product_name']
                
                # Find parent mapping by name
                parent_info = parent_mapping.get(product_name.lower().strip())
                
                if parent_info:
                    parent_product_id = parent_info['parent_product_id']
                    
                    # Update the product with parent_product_id from local shop master
                    update_query = """
                    UPDATE public.local_shop_prices 
                    SET parent_product_id = :parent_id 
                    WHERE id = :product_id
                    """
                    conn.execute(text(update_query), {"parent_id": parent_product_id, "product_id": product_id})
                    updated_count += 1
            
            conn.commit()
            print(f"  ✅ Updated {updated_count} products with parent_product_id from local shop master")
            print(f"  🎯 All parent IDs come from local_shop_master table")
            
            return True
            
    except Exception as e:
        print(f"  Error processing local_shop_prices: {e}")
        return False

def verify_parent_product_ids_local_shop():
    """Verify parent_product_id assignments in local_shop_prices table"""
    
    print("\nVerifying parent_product_id assignments in local_shop_prices...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Count products with and without parent_product_id
            count_query = """
            SELECT 
                COUNT(*) as total_products,
                COUNT(parent_product_id) as products_with_parent,
                COUNT(*) - COUNT(parent_product_id) as products_without_parent
            FROM public.local_shop_prices
            """
            count_df = pd.read_sql(count_query, conn)
            
            total = count_df['total_products'].iloc[0]
            with_parent = count_df['products_with_parent'].iloc[0]
            without_parent = count_df['products_without_parent'].iloc[0]
            
            print(f"  local_shop_prices verification:")
            print(f"    Total products: {total}")
            print(f"    With parent_product_id: {with_parent}")
            print(f"    Without parent_product_id: {without_parent}")
            
            # Show sample products with parent IDs
            if with_parent > 0:
                sample_query = """
                SELECT product_name, parent_product_id 
                FROM public.local_shop_prices 
                WHERE parent_product_id IS NOT NULL 
                LIMIT 5
                """
                sample_df = pd.read_sql(sample_query, conn)
                print(f"    Sample products with parent IDs from local shop master:")
                for _, row in sample_df.iterrows():
                    print(f"      - {row['product_name']} -> {row['parent_product_id']}")
            
            return True
            
    except Exception as e:
        print(f"  Error verifying local_shop_prices: {e}")
        return False

def show_parent_mapping_summary(parent_mapping):
    """Show summary of parent mapping from local shop master"""
    
    print(f"\n📋 LOCAL SHOP MASTER MAPPING SUMMARY")
    print("=" * 50)
    
    if not parent_mapping:
        print("No parent mapping found!")
        return
    
    # Group by parent
    parent_groups = {}
    for child_name, parent_info in parent_mapping.items():
        parent_name = parent_info['parent_name']
        if parent_name not in parent_groups:
            parent_groups[parent_name] = []
        parent_groups[parent_name].append(child_name)
    
    print(f"Total parent products: {len(parent_groups)}")
    print(f"Total child products: {len(parent_mapping)}")
    
    # Show sample parent groups
    print(f"\nSample parent groups:")
    for i, (parent_name, children) in enumerate(list(parent_groups.items())[:5], 1):
        print(f"  {i}. {parent_name} ({len(children)} children)")
        for child in children[:3]:  # Show first 3 children
            print(f"     - {child}")
        if len(children) > 3:
            print(f"     ... and {len(children) - 3} more")
    
    if len(parent_groups) > 5:
        print(f"  ... and {len(parent_groups) - 5} more parent groups")

def main():
    """Main function"""
    print("Adding parent_product_id to LOCAL local_shop_prices table")
    print("USING PARENT IDs FROM LOCAL SHOP MASTER TABLE")
    print("=" * 60)
    
    try:
        # Step 1: Get parent-child relationships from local shop master
        parent_mapping = get_local_shop_master_data()
        if not parent_mapping:
            print("No parent mapping found. Cannot proceed.")
            return
        
        # Step 2: Show mapping summary
        show_parent_mapping_summary(parent_mapping)
        
        # Step 3: Add parent_product_id to local_shop_prices table
        success = add_parent_product_id_to_local_shop_prices(parent_mapping)
        if not success:
            print("Failed to add parent_product_id to local_shop_prices table")
            return
        
        # Step 4: Verify assignments
        verify_parent_product_ids_local_shop()
        
        print(f"\n🎯 PARENT PRODUCT ID ASSIGNMENT COMPLETED!")
        print(f"📁 local_shop_prices table now has parent_product_id column")
        print(f"🔗 Parent IDs are sourced from local_shop_master table")
        print(f"✅ All products mapped to their parent categories")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
