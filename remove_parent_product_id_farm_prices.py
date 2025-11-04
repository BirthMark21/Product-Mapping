#!/usr/bin/env python3
"""
Script to remove parent_product_id column from farm_prices table in remote Supabase
This will only remove the parent_product_id column from farm_prices table
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

def check_parent_product_id_exists(engine, table_name):
    """Check if parent_product_id column exists in the table"""
    
    try:
        with engine.connect() as conn:
            # Check if parent_product_id column exists
            check_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name = 'parent_product_id'
            """
            result = conn.execute(text(check_query))
            exists = result.fetchone() is not None
            
            return exists
            
    except Exception as e:
        print(f"❌ Error checking column in {table_name}: {e}")
        return False

def remove_parent_product_id_from_farm_prices(engine):
    """Remove parent_product_id column from farm_prices table only"""
    
    table_name = "farm_prices"
    print(f"🔄 Processing table: {table_name}")
    
    try:
        with engine.connect() as conn:
            # Check if parent_product_id column exists
            if not check_parent_product_id_exists(engine, table_name):
                print(f"   ✅ No parent_product_id column found in {table_name}")
                return True
            
            print(f"   📋 parent_product_id column found in {table_name}")
            print(f"   ⚠️  SAFETY: Only removing parent_product_id column from {table_name} - NO OTHER DATA WILL BE AFFECTED")
            
            # Remove parent_product_id column
            try:
                alter_query = f"ALTER TABLE public.{table_name} DROP COLUMN IF EXISTS parent_product_id;"
                conn.execute(text(alter_query))
                conn.commit()
                print(f"   ✅ Removed parent_product_id column from {table_name}")
            except Exception as e:
                print(f"   ❌ Error removing parent_product_id column from {table_name}: {e}")
                return False
            
            # Verify removal
            if not check_parent_product_id_exists(engine, table_name):
                print(f"   ✅ parent_product_id column successfully removed from {table_name}")
                return True
            else:
                print(f"   ⚠️  parent_product_id column still exists in {table_name}")
                return False
                
    except Exception as e:
        print(f"❌ Error processing {table_name}: {e}")
        return False

def show_farm_prices_structure(engine):
    """Show the current structure of farm_prices table"""
    
    try:
        with engine.connect() as conn:
            # Get all columns
            columns_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'farm_prices'
            ORDER BY ordinal_position
            """
            result = conn.execute(text(columns_query))
            columns = result.fetchall()
            
            print(f"   📋 Current columns in farm_prices:")
            for col_name, data_type, is_nullable in columns:
                nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"     - {col_name:<30} {data_type:<20} {nullable_str}")
                
    except Exception as e:
        print(f"❌ Error showing structure for farm_prices: {e}")

def verify_other_tables(engine):
    """Verify that other tables don't have parent_product_id column"""
    
    print(f"\n🔍 Verifying other tables don't have parent_product_id column...")
    
    other_tables = [
        "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    for table in other_tables:
        try:
            with engine.connect() as conn:
                if check_parent_product_id_exists(engine, table):
                    print(f"   ⚠️  {table}: parent_product_id column found (unexpected)")
                else:
                    print(f"   ✅ {table}: No parent_product_id column (as expected)")
                    
        except Exception as e:
            print(f"❌ Error checking {table}: {e}")

def main():
    """Main function"""
    print("🚀 Removing Parent Product ID from Farm Prices Table")
    print("=" * 60)
    print("📥 Source: Remote Supabase Database")
    print("🎯 Goal: Remove parent_product_id column from farm_prices table only")
    print("⚠️  SAFETY: NO OTHER DATA WILL BE AFFECTED")
    print("=" * 60)
    
    try:
        # Create Supabase connection
        engine = get_supabase_engine()
        
        # Show current structure
        print("📋 Current farm_prices table structure:")
        show_farm_prices_structure(engine)
        
        # Remove parent_product_id column from farm_prices
        if remove_parent_product_id_from_farm_prices(engine):
            print(f"\n✅ parent_product_id column successfully removed from farm_prices!")
        else:
            print(f"\n❌ Failed to remove parent_product_id column from farm_prices!")
            return
        
        # Show final structure
        print(f"\n📋 Final farm_prices table structure:")
        show_farm_prices_structure(engine)
        
        # Verify other tables
        verify_other_tables(engine)
        
        print(f"\n✅ Parent product ID removal completed!")
        print(f"🎯 parent_product_id column removed from farm_prices table only!")
        print(f"📊 All other tables remain unchanged!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
