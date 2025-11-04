#!/usr/bin/env python3
"""
Simple script to check if distribution_center_prices table exists in local database
"""

import sys
import os
sys.path.append('.')

import config.setting as settings
from utils.db_connector import get_db_engine
from sqlalchemy import text

def check_local_table():
    """Check if distribution_center_prices table exists in local database"""
    
    print("🔍 Checking local database for distribution_center_prices table...")
    print("=" * 60)
    
    try:
        # Connect to local database
        print("📡 Connecting to local database...")
        local_engine = get_db_engine('hub')
        print("✅ Connected to local database successfully")
        
        with local_engine.connect() as conn:
            # Check if table exists
            print("\n🔍 Checking if table exists...")
            check_table = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'distribution_center_prices'
            """)
            
            result = conn.execute(check_table).fetchall()
            
            if result:
                print("✅ Table 'distribution_center_prices' EXISTS in local database")
                
                # Get record count
                print("\n📊 Getting record count...")
                count_query = text('SELECT COUNT(*) as count FROM public.distribution_center_prices')
                count_result = conn.execute(count_query).fetchone()
                print(f"📊 Total records: {count_result[0]}")
                
                # Show sample data
                print("\n📝 Sample data (first 5 records):")
                sample_query = text('SELECT product_name, created_at FROM public.distribution_center_prices LIMIT 5')
                sample_result = conn.execute(sample_query).fetchall()
                for i, row in enumerate(sample_result, 1):
                    print(f"  {i}. {row[0]} (created: {row[1]})")
                    
            else:
                print("❌ Table 'distribution_center_prices' NOT FOUND in local database")
                
                # List all tables in public schema
                print("\n📋 Available tables in public schema:")
                list_tables = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
                tables = conn.execute(list_tables).fetchall()
                
                if tables:
                    for table in tables:
                        print(f"  - {table[0]}")
                else:
                    print("  No tables found in public schema")
                    
    except Exception as e:
        print(f"❌ Error checking local database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_local_table()
