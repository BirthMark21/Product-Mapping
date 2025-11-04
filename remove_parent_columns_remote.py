#!/usr/bin/env python3
"""
Script to remove parent_id and parent_name columns from remote Supabase tables
This will clean up the remote database by removing these columns
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_supabase_engine():
    """Create database engine for remote Supabase"""
    supabase_url = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
    return create_engine(supabase_url)

def check_columns_exist(engine, table_name):
    """Check if parent_id and parent_name columns exist in the table"""
    
    try:
        with engine.connect() as conn:
            # Check if columns exist
            check_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name IN ('parent_id', 'parent_name')
            """
            result = conn.execute(text(check_query))
            existing_columns = [row[0] for row in result.fetchall()]
            
            return existing_columns
            
    except Exception as e:
        print(f"❌ Error checking columns in {table_name}: {e}")
        return []

def remove_parent_columns(engine, table_name):
    """Remove ONLY parent_id and parent_name columns from a table - NO OTHER DATA AFFECTED"""
    
    print(f"🔄 Processing table: {table_name}")
    
    try:
        with engine.connect() as conn:
            # Check which columns exist
            existing_columns = check_columns_exist(engine, table_name)
            
            if not existing_columns:
                print(f"   ✅ No parent_id or parent_name columns found in {table_name}")
                return True
            
            print(f"   📋 Found columns to remove: {existing_columns}")
            print(f"   ⚠️  SAFETY: Only removing parent_id and parent_name columns - NO OTHER DATA WILL BE AFFECTED")
            
            # Remove each column that exists (ONLY parent_id and parent_name)
            for column in existing_columns:
                if column in ['parent_id', 'parent_name']:  # SAFETY CHECK: Only remove these specific columns
                    try:
                        alter_query = f"ALTER TABLE public.{table_name} DROP COLUMN IF EXISTS {column};"
                        conn.execute(text(alter_query))
                        print(f"   ✅ Removed column '{column}' from {table_name}")
                    except Exception as e:
                        print(f"   ❌ Error removing column '{column}' from {table_name}: {e}")
                else:
                    print(f"   ⚠️  SKIPPING column '{column}' - not in parent_id/parent_name list")
            
            conn.commit()
            print(f"   ✅ Successfully processed {table_name} - NO OTHER DATA AFFECTED")
            return True
            
    except Exception as e:
        print(f"❌ Error processing {table_name}: {e}")
        return False

def verify_columns_removed(engine, table_name):
    """Verify that parent_id and parent_name columns have been removed"""
    
    try:
        with engine.connect() as conn:
            # Check if columns still exist
            check_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name IN ('parent_id', 'parent_name')
            """
            result = conn.execute(text(check_query))
            remaining_columns = [row[0] for row in result.fetchall()]
            
            if remaining_columns:
                print(f"   ⚠️  Columns still exist in {table_name}: {remaining_columns}")
                return False
            else:
                print(f"   ✅ All parent columns successfully removed from {table_name}")
                return True
                
    except Exception as e:
        print(f"❌ Error verifying {table_name}: {e}")
        return False

def show_table_structure(engine, table_name):
    """Show the current structure of a table"""
    
    try:
        with engine.connect() as conn:
            # Get all columns
            columns_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            result = conn.execute(text(columns_query))
            columns = result.fetchall()
            
            print(f"   📋 Current columns in {table_name}:")
            for col_name, data_type, is_nullable in columns:
                nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"     - {col_name:<20} {data_type:<15} {nullable_str}")
                
    except Exception as e:
        print(f"❌ Error showing structure for {table_name}: {e}")

def main():
    """Main function"""
    print("🚀 Removing Parent Columns from Remote Supabase Tables")
    print("=" * 60)
    print("📥 Source: Remote Supabase Database")
    print("🎯 Goal: Remove ONLY parent_id and parent_name columns")
    print("⚠️  SAFETY: NO OTHER DATA WILL BE AFFECTED")
    print("=" * 60)
    
    try:
        # Create Supabase connection
        engine = get_supabase_engine()
        
        # Tables to process
        tables = [
            "farm_prices", "supermarket_prices", "distribution_center_prices",
            "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
        ]
        
        success_count = 0
        
        # Process each table
        for table in tables:
            print(f"\n🔍 Processing {table}...")
            
            # Remove parent columns
            if remove_parent_columns(engine, table):
                # Verify removal
                if verify_columns_removed(engine, table):
                    success_count += 1
                    print(f"   ✅ {table} processed successfully")
                else:
                    print(f"   ⚠️  {table} processed but verification failed")
            else:
                print(f"   ❌ Failed to process {table}")
        
        print(f"\n📊 Summary:")
        print(f"   ✅ Successfully processed: {success_count}/{len(tables)} tables")
        
        # Show final table structures
        print(f"\n🔍 Final table structures:")
        print("=" * 60)
        for table in tables:
            print(f"\n📋 {table}:")
            show_table_structure(engine, table)
        
        print(f"\n✅ Parent column removal completed!")
        print(f"🎯 All parent_id and parent_name columns have been removed from remote Supabase tables!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
