#!/usr/bin/env python3
"""
Test script to check how many records are actually in the remote Supabase database
This will help us understand why the ETL pipeline is getting small numbers
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def test_remote_supabase_data():
    """Test the remote Supabase database to see actual record counts"""
    
    print("🔍 Testing remote Supabase database...")
    print("=" * 50)
    
    # Create Supabase connection
    supabase_url = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
    
    try:
        engine = create_engine(supabase_url)
        
        tables = [
            "farm_prices", "supermarket_prices", "distribution_center_prices",
            "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
        ]
        
        total_records = 0
        
        for table in tables:
            try:
                with engine.connect() as conn:
                    # Count total records
                    count_query = f"SELECT COUNT(*) FROM public.{table}"
                    result = conn.execute(text(count_query))
                    count = result.scalar()
                    
                    print(f"📊 {table}: {count:,} records")
                    total_records += count
                    
                    # Test the actual query used by ETL pipeline
                    etl_query = f"SELECT id::text AS raw_product_id, product_name AS raw_product_name FROM public.{table}"
                    df = pd.read_sql(etl_query, conn)
                    print(f"   🔍 ETL query result: {len(df):,} records")
                    
            except Exception as e:
                print(f"❌ Error with {table}: {e}")
        
        print(f"\n📈 Total records across all tables: {total_records:,}")
        
        # Test the full UNION ALL query that the ETL pipeline uses
        print(f"\n🔍 Testing full ETL pipeline query...")
        union_queries = []
        for table in tables:
            union_queries.append(
                f"SELECT id::text AS raw_product_id, product_name AS raw_product_name FROM public.{table}"
            )
        
        full_query = text(" \nUNION ALL\n ".join(union_queries))
        
        with engine.connect() as connection:
            df = pd.read_sql(full_query, connection)
            print(f"✅ Full ETL query result: {len(df):,} records")
            
            if len(df) > 0:
                print(f"📋 Sample records:")
                print(df.head())
            else:
                print("⚠️  No records returned from ETL query!")
        
    except Exception as e:
        print(f"❌ Database connection error: {e}")

def main():
    """Main function"""
    print("🚀 Testing Remote Supabase Data Count")
    print("=" * 60)
    print("📥 Source: Remote Supabase (aws-0-eu-central-1.pooler.supabase.com)")
    print("🎯 Goal: Check actual record counts vs ETL pipeline results")
    print("=" * 60)
    
    test_remote_supabase_data()

if __name__ == "__main__":
    main()
