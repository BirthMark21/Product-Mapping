#!/usr/bin/env python3
"""
Test script to verify that created_at values come from actual source tables
This will show the created_at values from all 7 tables to ensure they're real timestamps
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
from pipeline.data_loader import load_all_product_data_from_clickhouse, load_all_product_data_from_supabase

# Load environment variables
load_dotenv()

def test_created_at_sources():
    """Test that created_at values come from actual source tables"""
    
    print("🔍 Testing Created At Sources")
    print("=" * 60)
    print("📥 Goal: Verify created_at values come from actual source tables")
    print("=" * 60)
    
    try:
        # Connect to databases
        supabase_engine = get_db_engine('supabase')
        clickhouse_engine = get_db_engine('clickhouse')
        
        print("📊 Step 1: Testing Supabase created_at values...")
        
        # Test Supabase data
        supabase_df = load_all_product_data_from_supabase(supabase_engine)
        if not supabase_df.empty:
            print(f"✅ Supabase: {len(supabase_df)} records loaded")
            
            # Show created_at statistics
            if 'created_at' in supabase_df.columns:
                created_at_stats = supabase_df['created_at'].describe()
                print(f"📅 Supabase created_at statistics:")
                print(f"   📋 Count: {created_at_stats['count']}")
                print(f"   📋 Min: {created_at_stats['min']}")
                print(f"   📋 Max: {created_at_stats['max']}")
                
                # Show sample created_at values
                sample_created_at = supabase_df['created_at'].dropna().head(5)
                print(f"📋 Sample Supabase created_at values:")
                for i, timestamp in enumerate(sample_created_at, 1):
                    print(f"   {i}. {timestamp}")
            else:
                print("❌ created_at column not found in Supabase data")
        else:
            print("⚠️  No Supabase data loaded")
        
        print("\n📊 Step 2: Testing ClickHouse created_at values...")
        
        # Test ClickHouse data
        clickhouse_df = load_all_product_data_from_clickhouse(clickhouse_engine)
        if not clickhouse_df.empty:
            print(f"✅ ClickHouse: {len(clickhouse_df)} records loaded")
            
            # Show created_at statistics
            if 'created_at' in clickhouse_df.columns:
                created_at_stats = clickhouse_df['created_at'].describe()
                print(f"📅 ClickHouse created_at statistics:")
                print(f"   📋 Count: {created_at_stats['count']}")
                print(f"   📋 Min: {created_at_stats['min']}")
                print(f"   📋 Max: {created_at_stats['max']}")
                
                # Show sample created_at values
                sample_created_at = clickhouse_df['created_at'].dropna().head(5)
                print(f"📋 Sample ClickHouse created_at values:")
                for i, timestamp in enumerate(sample_created_at, 1):
                    print(f"   {i}. {timestamp}")
            else:
                print("❌ created_at column not found in ClickHouse data")
        else:
            print("⚠️  No ClickHouse data loaded")
        
        print("\n📊 Step 3: Testing combined data...")
        
        # Combine data
        all_data_frames = []
        if not clickhouse_df.empty:
            all_data_frames.append(clickhouse_df)
        if not supabase_df.empty:
            all_data_frames.append(supabase_df)
        
        if all_data_frames:
            combined_df = pd.concat(all_data_frames, ignore_index=True)
            print(f"✅ Combined data: {len(combined_df)} total records")
            
            if 'created_at' in combined_df.columns:
                created_at_stats = combined_df['created_at'].describe()
                print(f"📅 Combined created_at statistics:")
                print(f"   📋 Count: {created_at_stats['count']}")
                print(f"   📋 Min: {created_at_stats['min']}")
                print(f"   📋 Max: {created_at_stats['max']}")
                
                # Check if created_at values are realistic (not all current time)
                unique_timestamps = combined_df['created_at'].nunique()
                print(f"📋 Unique timestamps: {unique_timestamps}")
                
                if unique_timestamps > 1:
                    print("✅ Good: Multiple unique timestamps found (real data)")
                else:
                    print("⚠️  Warning: Only one unique timestamp found (might be current time)")
                
                # Show date range
                min_date = combined_df['created_at'].min()
                max_date = combined_df['created_at'].max()
                print(f"📅 Date range: {min_date} to {max_date}")
                
            else:
                print("❌ created_at column not found in combined data")
        else:
            print("❌ No data to combine")
        
        print("\n✅ Created At Source Test Completed!")
        
    except Exception as e:
        print(f"❌ Error testing created_at sources: {e}")

def main():
    """Main function"""
    print("🚀 Testing Created At Sources")
    print("=" * 60)
    print("📥 Goal: Verify created_at values come from actual source tables")
    print("🎯 This ensures parent-child relationships use real timestamps")
    print("=" * 60)
    
    test_created_at_sources()

if __name__ == "__main__":
    main()
