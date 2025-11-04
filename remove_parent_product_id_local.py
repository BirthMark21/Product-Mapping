#!/usr/bin/env python3
"""
Script to remove parent_product_id column from all 6 local Supabase tables
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def remove_parent_product_id_from_table(engine, table_name):
    """Remove parent_product_id column from a specific local table"""
    
    print(f"\nProcessing local table: {table_name}")
    
    try:
        with engine.connect() as conn:
            # Check if parent_product_id column exists
            check_column_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name = 'parent_product_id'
            AND table_schema = 'public'
            """
            existing_columns = pd.read_sql(check_column_query, conn, params=(table_name,))
            
            if existing_columns.empty:
                print(f"  📝 parent_product_id column does not exist in {table_name}")
                print(f"  ✅ No action needed")
                return True
            
            # Remove parent_product_id column
            print(f"  🗑️ Removing parent_product_id column from {table_name}...")
            drop_column_query = f"""
            ALTER TABLE public.{table_name} 
            DROP COLUMN parent_product_id
            """
            conn.execute(text(drop_column_query))
            conn.commit()
            print(f"  ✅ Successfully removed parent_product_id column from {table_name}")
            
            # Verify column was removed
            verify_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name = 'parent_product_id'
            AND table_schema = 'public'
            """
            verify_columns = pd.read_sql(verify_query, conn, params=(table_name,))
            
            if verify_columns.empty:
                print(f"  ✅ Verification: parent_product_id column successfully removed from {table_name}")
            else:
                print(f"  ❌ Verification failed: parent_product_id column still exists in {table_name}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error processing {table_name}: {e}")
        return False

def verify_table_structure(engine, table_name):
    """Verify table structure after column removal"""
    
    try:
        with engine.connect() as conn:
            # Get table structure
            structure_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            structure_df = pd.read_sql(structure_query, conn, params=(table_name,))
            
            print(f"  📋 {table_name} current structure:")
            for _, row in structure_df.iterrows():
                print(f"    - {row['column_name']}: {row['data_type']} ({'NULL' if row['is_nullable'] == 'YES' else 'NOT NULL'})")
            
            # Check if parent_product_id still exists
            parent_column_exists = any(row['column_name'] == 'parent_product_id' for _, row in structure_df.iterrows())
            
            if parent_column_exists:
                print(f"    ❌ parent_product_id column still exists!")
                return False
            else:
                print(f"    ✅ parent_product_id column successfully removed")
                return True
                
    except Exception as e:
        print(f"  ❌ Error verifying {table_name}: {e}")
        return False

def main():
    """Main function"""
    print("Removing parent_product_id from all 6 LOCAL Supabase tables")
    print("=" * 60)
    
    try:
        # Step 1: Get local database engine
        local_engine = get_db_engine('hub')
        
        # Step 2: Get list of Supabase tables
        supabase_tables = settings.SUPABASE_TABLES
        print(f"Processing {len(supabase_tables)} local Supabase tables:")
        for table in supabase_tables:
            print(f"  - {table}")
        
        print(f"\nCorrect local table names from settings.py:")
        print(f"  - farm_prices")
        print(f"  - supermarket_prices") 
        print(f"  - distribution_center_prices")
        print(f"  - local_shop_prices")
        print(f"  - ecommerce_prices")
        print(f"  - sunday_market_prices")
        
        # Step 3: Remove parent_product_id from each table
        successful_tables = []
        failed_tables = []
        
        for table in supabase_tables:
            success = remove_parent_product_id_from_table(local_engine, table)
            if success:
                successful_tables.append(table)
            else:
                failed_tables.append(table)
        
        # Step 4: Verify results
        print(f"\nVerification Results:")
        print("=" * 40)
        
        for table in successful_tables:
            verify_table_structure(local_engine, table)
        
        # Step 5: Summary
        print(f"\nSummary:")
        print(f"  Successfully processed: {len(successful_tables)} local tables")
        print(f"  Failed: {len(failed_tables)} local tables")
        
        if successful_tables:
            print(f"  Successful local tables: {', '.join(successful_tables)}")
        
        if failed_tables:
            print(f"  Failed local tables: {', '.join(failed_tables)}")
        
        print(f"\nLocal parent_product_id column removal completed!")
        
    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
