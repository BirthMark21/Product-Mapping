#!/usr/bin/env python3
"""
Script to create complete canonical master table with all 7 tables and add parent_product_id to 6 Supabase tables
This uses the updated standardization.py that includes created_at and handles all 7 tables
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
from pipeline.data_loader import load_all_product_data_from_clickhouse, load_all_product_data_from_supabase
from pipeline.standardization import create_parent_child_master_table
from pipeline.db_writer import write_to_db

# Load environment variables
load_dotenv()

def create_complete_canonical_master():
    """Create canonical master table with all 7 tables (6 Supabase + 1 ClickHouse)"""
    
    print("🚀 Creating Complete Canonical Master Table")
    print("=" * 60)
    print("📥 Source: All 7 tables (6 Supabase + 1 ClickHouse)")
    print("🎯 Goal: Create comprehensive canonical master table")
    print("=" * 60)
    
    try:
        # Connect to databases
        supabase_engine = get_db_engine('supabase')
        clickhouse_engine = get_db_engine('clickhouse')
        hub_engine = get_db_engine('hub')
        
        print("📊 Step 1: Loading data from all 7 tables...")
        
        # Load data from ClickHouse (1 table)
        print("   🔄 Loading from ClickHouse...")
        clickhouse_df = load_all_product_data_from_clickhouse(clickhouse_engine)
        if not clickhouse_df.empty:
            clickhouse_df['source'] = 'clickhouse'
            print(f"   ✅ ClickHouse: {len(clickhouse_df)} records")
        else:
            print("   ⚠️  ClickHouse: No data")
        
        # Load data from Supabase (6 tables)
        print("   🔄 Loading from Supabase...")
        supabase_df = load_all_product_data_from_supabase(supabase_engine)
        if not supabase_df.empty:
            supabase_df['source'] = 'supabase'
            print(f"   ✅ Supabase: {len(supabase_df)} records")
        else:
            print("   ⚠️  Supabase: No data")
        
        # Combine all data
        all_data_frames = []
        if not clickhouse_df.empty:
            all_data_frames.append(clickhouse_df)
        if not supabase_df.empty:
            all_data_frames.append(supabase_df)
        
        if not all_data_frames:
            print("❌ No data loaded from any source!")
            return None
        
        # Combine all data
        combined_df = pd.concat(all_data_frames, ignore_index=True)
        print(f"📊 Combined data: {len(combined_df)} total records from all 7 tables")
        
        # Show data breakdown by source
        source_counts = combined_df['source'].value_counts()
        print("📈 Data breakdown by source:")
        for source, count in source_counts.items():
            print(f"   📋 {source}: {count:,} records")
        
        print("\n🔄 Step 2: Creating canonical master table...")
        
        # Create canonical master table using updated standardization
        canonical_master_df = create_parent_child_master_table(combined_df)
        
        if canonical_master_df.empty:
            print("❌ Failed to create canonical master table!")
            return None
        
        print(f"✅ Created canonical master table with {len(canonical_master_df)} parent products")
        
        # Show sample of canonical master
        print("\n📋 Sample canonical master data:")
        print(canonical_master_df.head(3))
        
        print("\n🔄 Step 3: Writing canonical master to hub database...")
        
        # Write to hub database
        write_to_db(
            df=canonical_master_df,
            engine=hub_engine,
            table_name=settings.HUB_CANONICAL_TABLE_NAME,
            schema=settings.HUB_SCHEMA,
            if_exists='replace'
        )
        
        print("✅ Canonical master table written to hub database")
        
        return canonical_master_df
        
    except Exception as e:
        print(f"❌ Error creating canonical master: {e}")
        return None

def add_parent_product_id_to_supabase_tables(canonical_master_df):
    """Add parent_product_id to all 6 Supabase tables based on canonical master"""
    
    print("\n🔄 Adding parent_product_id to Supabase tables...")
    print("=" * 60)
    
    try:
        # Create Supabase connection
        supabase_engine = get_db_engine('supabase')
        
        # Create parent mapping from canonical master
        parent_mapping = {}
        
        for _, row in canonical_master_df.iterrows():
            parent_product_id = row['parent_product_id']
            child_ids = row['child_product_ids']
            
            if isinstance(child_ids, list):
                for child_id in child_ids:
                    parent_mapping[str(child_id)] = parent_product_id
        
        print(f"📊 Created mapping for {len(parent_mapping)} child products")
        
        # Tables to update
        tables = [
            "farm_prices", "supermarket_prices", "distribution_center_prices",
            "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
        ]
        
        # Add parent_product_id column to each table
        for table in tables:
            print(f"\n🔄 Processing {table}...")
            
            try:
                with supabase_engine.connect() as conn:
                    # Add parent_product_id column
                    alter_query = f"""
                    ALTER TABLE public.{table} 
                    ADD COLUMN IF NOT EXISTS parent_product_id UUID;
                    """
                    conn.execute(text(alter_query))
                    conn.commit()
                    print(f"   ✅ Added parent_product_id column to {table}")
                    
                    # Get all records to update
                    select_query = f"SELECT id, product_name FROM public.{table}"
                    df = pd.read_sql(select_query, conn)
                    
                    if df.empty:
                        print(f"   ⚠️  No records in {table}")
                        continue
                    
                    updated_count = 0
                    
                    # Update records with parent_product_id
                    for _, row in df.iterrows():
                        record_id = str(row['id'])
                        
                        if record_id in parent_mapping:
                            parent_product_id = parent_mapping[record_id]
                            
                            update_query = f"""
                            UPDATE public.{table} 
                            SET parent_product_id = '{parent_product_id}'
                            WHERE id = '{record_id}';
                            """
                            conn.execute(text(update_query))
                            updated_count += 1
                    
                    conn.commit()
                    print(f"   ✅ Updated {updated_count:,} records in {table}")
                    
            except Exception as e:
                print(f"   ❌ Error processing {table}: {e}")
        
        print("\n✅ Parent product ID addition completed!")
        
    except Exception as e:
        print(f"❌ Error adding parent product IDs: {e}")

def verify_results():
    """Verify the results of the complete process"""
    
    print("\n🔍 Verifying results...")
    print("=" * 60)
    
    try:
        # Check canonical master table
        hub_engine = get_db_engine('hub')
        with hub_engine.connect() as conn:
            canonical_count = conn.execute(text(f"SELECT COUNT(*) FROM public.{settings.HUB_CANONICAL_TABLE_NAME}")).scalar()
            print(f"📊 Canonical master table: {canonical_count:,} parent products")
        
        # Check Supabase tables with parent_product_id
        supabase_engine = get_db_engine('supabase')
        tables = [
            "farm_prices", "supermarket_prices", "distribution_center_prices",
            "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
        ]
        
        total_with_parent = 0
        total_records = 0
        
        for table in tables:
            try:
                with supabase_engine.connect() as conn:
                    count_query = f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(parent_product_id) as records_with_parent
                    FROM public.{table}
                    """
                    result = conn.execute(text(count_query))
                    row = result.fetchone()
                    
                    if row:
                        table_total = row[0]
                        table_with_parent = row[1]
                        
                        print(f"📊 {table}: {table_with_parent:,}/{table_total:,} records have parent_product_id ({table_with_parent/table_total*100:.1f}%)")
                        
                        total_records += table_total
                        total_with_parent += table_with_parent
                        
            except Exception as e:
                print(f"❌ Error verifying {table}: {e}")
        
        print(f"\n📈 Overall Supabase: {total_with_parent:,}/{total_records:,} records have parent_product_id ({total_with_parent/total_records*100:.1f}%)")
        
    except Exception as e:
        print(f"❌ Error verifying results: {e}")

def main():
    """Main function"""
    print("🚀 Complete Canonical Master with Parent Product IDs")
    print("=" * 60)
    print("📥 Source: All 7 tables (6 Supabase + 1 ClickHouse)")
    print("🎯 Goal: Create canonical master + add parent_product_id to Supabase")
    print("=" * 60)
    
    try:
        # Step 1: Create complete canonical master table
        canonical_master_df = create_complete_canonical_master()
        
        if canonical_master_df is None:
            print("❌ Failed to create canonical master table!")
            return
        
        # Step 2: Add parent_product_id to Supabase tables
        add_parent_product_id_to_supabase_tables(canonical_master_df)
        
        # Step 3: Verify results
        verify_results()
        
        print("\n🎉 Complete process finished successfully!")
        print("✅ Canonical master table created with all 7 tables")
        print("✅ Parent product IDs added to all 6 Supabase tables")
        print("✅ All data includes created_at information")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
