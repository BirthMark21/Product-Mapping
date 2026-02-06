#!/usr/bin/env python3
"""
Script to add parent_product_id column to all 6 remote Supabase tables
Uses canonical_products_master as the source for parent IDs
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

def get_canonical_master_data():
    """Get parent-child relationships from canonical master table"""
    
    print("Loading parent-child relationships from canonical master...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_ids,
                child_product_names
            FROM public.canonical_products_master
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("No data found in canonical master table!")
                return {}
            
            print(f"Found {len(df)} parent groups in canonical master")
            
            # Create parent mapping
            parent_mapping = {}
            
            for _, row in df.iterrows():
                parent_product_id = row['parent_product_id']
                parent_name = row['parent_product_name']
                child_ids = row['child_product_ids']
                child_names = row['child_product_names']
                
                # Parse child data - handle PostgreSQL array format
                if isinstance(child_ids, str):
                    try:
                        # Handle PostgreSQL array format {item1,item2,item3}
                        if child_ids.startswith('{') and child_ids.endswith('}'):
                            child_ids = child_ids[1:-1].split(',')
                            child_ids = [id.strip() for id in child_ids if id.strip()]
                        else:
                            child_ids = ast.literal_eval(child_ids)
                    except:
                        child_ids = []
                elif not isinstance(child_ids, list):
                    child_ids = []
                
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
                
                # Create mapping for each child
                for child_id, child_name in zip(child_ids, child_names):
                    parent_mapping[str(child_id)] = {
                        'parent_product_id': parent_product_id,
                        'parent_name': parent_name
                    }
                    parent_mapping[child_name.lower().strip()] = {
                        'parent_product_id': parent_product_id,
                        'parent_name': parent_name
                    }
            
            print(f"Created mapping for {len(parent_mapping)} child products")
            return parent_mapping
            
    except Exception as e:
        print(f"Error loading canonical master data: {e}")
        return {}

def add_parent_product_id_to_remote_table(engine, table_name, parent_mapping):
    """Add parent_product_id column to a specific remote Supabase table - READ ONLY, NO DATA MODIFICATION"""
    
    print(f"\nProcessing remote table: {table_name}")
    
    try:
        with engine.connect() as conn:
            # Check if parent_product_id column already exists
            check_column_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name = 'parent_product_id'
            AND table_schema = 'public'
            """
            existing_columns = pd.read_sql(check_column_query, conn, params=(table_name,))
            
            if not existing_columns.empty:
                print(f"  parent_product_id column already exists in {table_name}")
                print(f"  SKIPPING - No changes made to existing data")
                return True
            
            # ADD COLUMN AND POPULATE WITH PARENT IDs FROM MASTER TABLE
            print(f"  Adding parent_product_id column to {table_name}...")
            add_column_query = f"""
            ALTER TABLE public.{table_name} 
            ADD COLUMN parent_product_id TEXT
            """
            conn.execute(text(add_column_query))
            print(f"  ✅ Successfully added parent_product_id column to {table_name}")
            
            # Get all products from the table
            get_products_query = f"""
            SELECT id, product_name 
            FROM public.{table_name}
            """
            products_df = pd.read_sql(get_products_query, conn)
            
            if products_df.empty:
                print(f"  📊 No products found in {table_name}")
                return True
            
            print(f"  📊 Found {len(products_df)} products in {table_name}")
            
            # Update parent_product_id for each product using master table mapping
            updated_count = 0
            for _, row in products_df.iterrows():
                product_id = str(row['id'])
                product_name = row['product_name']
                
                # Find parent mapping by ID first, then by name
                parent_info = parent_mapping.get(product_id)
                if not parent_info:
                    parent_info = parent_mapping.get(product_name.lower().strip())
                
                if parent_info:
                    parent_product_id = parent_info['parent_product_id']
                    
                    # Update the product with parent_product_id from master table
                    update_query = f"""
                    UPDATE public.{table_name} 
                    SET parent_product_id = :parent_id 
                    WHERE id = :product_id
                    """
                    conn.execute(text(update_query), {"parent_id": parent_product_id, "product_id": product_id})
                    updated_count += 1
            
            conn.commit()
            print(f"  ✅ Updated {updated_count} products with parent_product_id from master table")
            print(f"  🎯 All parent IDs come from canonical_products_master table")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error processing {table_name}: {e}")
        return False

def verify_parent_product_ids_remote(engine, table_name):
    """Verify parent_product_id assignments in remote table"""
    
    try:
        with engine.connect() as conn:
            # Count products with and without parent_product_id
            count_query = f"""
            SELECT 
                COUNT(*) as total_products,
                COUNT(parent_product_id) as products_with_parent,
                COUNT(*) - COUNT(parent_product_id) as products_without_parent
            FROM public.{table_name}
            """
            count_df = pd.read_sql(count_query, conn)
            
            total = count_df['total_products'].iloc[0]
            with_parent = count_df['products_with_parent'].iloc[0]
            without_parent = count_df['products_without_parent'].iloc[0]
            
            print(f"  ✅ {table_name} verification:")
            print(f"    Total products: {total}")
            print(f"    With parent_product_id: {with_parent}")
            print(f"    Without parent_product_id: {without_parent}")
            
            # Show sample products with parent IDs from master table
            if with_parent > 0:
                sample_query = f"""
                SELECT product_name, parent_product_id 
                FROM public.{table_name} 
                WHERE parent_product_id IS NOT NULL 
                LIMIT 3
                """
                sample_df = pd.read_sql(sample_query, conn)
                print(f"    Sample products with parent IDs from master table:")
                for _, row in sample_df.iterrows():
                    print(f"      - {row['product_name']} -> {row['parent_product_id']}")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error verifying {table_name}: {e}")
        return False

def main():
    """Main function"""
    print("Adding parent_product_id to all 6 REMOTE Supabase tables")
    print("USING PARENT IDs FROM CANONICAL MASTER TABLE")
    print("=" * 60)
    
    try:
        # Step 1: Get parent-child relationships from canonical master
        parent_mapping = get_canonical_master_data()
        if not parent_mapping:
            print("No parent mapping found. Cannot proceed.")
            return
        
        # Step 2: Get remote Supabase database engine
        remote_engine = get_db_engine('supabase')
        
        # Step 3: Get list of Supabase tables
        supabase_tables = settings.SUPABASE_TABLES
        print(f"Processing {len(supabase_tables)} remote Supabase tables:")
        for table in supabase_tables:
            print(f"  - {table}")
        
        print(f"\nCorrect remote table names from settings.py:")
        print(f"  - farm_prices")
        print(f"  - supermarket_prices") 
        print(f"  - distribution_center_prices")
        print(f"  - local_shop_prices")
        print(f"  - ecommerce_prices")
        print(f"  - sunday_market_prices")
        
        # Step 4: Add parent_product_id to each remote table
        successful_tables = []
        failed_tables = []
        
        for table in supabase_tables:
            success = add_parent_product_id_to_remote_table(remote_engine, table, parent_mapping)
            if success:
                successful_tables.append(table)
            else:
                failed_tables.append(table)
        
        # Step 5: Verify results
        print(f"\nVerification Results:")
        print("=" * 40)
        
        for table in successful_tables:
            verify_parent_product_ids_remote(remote_engine, table)
        
        # Step 6: Summary
        print(f"\nSummary:")
        print(f"  Successfully processed: {len(successful_tables)} remote tables")
        print(f"  Failed: {len(failed_tables)} remote tables")
        
        if successful_tables:
            print(f"  Successful remote tables: {', '.join(successful_tables)}")
        
        if failed_tables:
            print(f"  Failed remote tables: {', '.join(failed_tables)}")
        
        print(f"\nRemote parent product ID assignment completed!")
        
    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
