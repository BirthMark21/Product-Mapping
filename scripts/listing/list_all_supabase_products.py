#!/usr/bin/env python3
"""
Script to list all products from all Supabase tables in the terminal
Displays all product names grouped by table and in a complete alphabetical list
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys

# Add the parent directory to the path so we can import config and utils
# Since this script is in scripts/listing/, we need to go up two levels
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def get_all_supabase_products():
    """Get all products from all Supabase tables"""
    
    print("Fetching all products from all Supabase tables...")
    print("=" * 60)
    
    all_products = []
    table_stats = {}  # Store stats per table
    supabase_engine = get_db_engine('supabase')
    
    if not supabase_engine:
        print("❌ Failed to get Supabase database engine.")
        return pd.DataFrame(), {}

    for table_name in settings.SUPABASE_TABLES:
        print(f"📋 Fetching from table: {table_name}...")
        try:
            with supabase_engine.connect() as conn:
                # Handle different column names for the new tables
                name_col = "product_name"
                if table_name == "products":
                    name_col = "name"
                
                query = f"""
                SELECT 
                    id::text AS product_id, 
                    {name_col} AS product_name, 
                    created_at
                FROM public.{table_name}
                ORDER BY {name_col}
                """
                df = pd.read_sql(query, conn)
                
                if not df.empty:
                    df['source_table'] = table_name
                    df['source_type'] = 'Supabase'
                    all_products.append(df)
                    
                    # Calculate stats for this table
                    raw_count = len(df)
                    unique_in_table = df['product_name'].nunique()
                    duplicates_in_table = raw_count - unique_in_table
                    
                    table_stats[table_name] = {
                        'raw_count': raw_count,
                        'unique_count': unique_in_table,
                        'duplicates_count': duplicates_in_table
                    }
                    
                    print(f"  ✅ Fetched {raw_count} products from {table_name} ({unique_in_table} unique names)")
                else:
                    table_stats[table_name] = {
                        'raw_count': 0,
                        'unique_count': 0,
                        'duplicates_count': 0
                    }
                    print(f"  ⚠️ No products found in {table_name}")
                    
        except Exception as e:
            print(f"  ❌ Error fetching from {table_name}: {e}")
            table_stats[table_name] = {
                'raw_count': 0,
                'unique_count': 0,
                'duplicates_count': 0
            }
    
    if all_products:
        combined_df = pd.concat(all_products, ignore_index=True)
        print(f"\n📊 Total products fetched from all Supabase tables: {len(combined_df)}")
        
        # Remove duplicates WITHIN each table only (not across tables)
        print("🔄 Removing duplicate products by name WITHIN each table...")
        
        unique_products_per_table = []
        total_duplicates_removed = 0
        
        for table_name in settings.SUPABASE_TABLES:
            table_df = combined_df[combined_df['source_table'] == table_name]
            if not table_df.empty:
                initial_count = len(table_df)
                # Remove duplicates within this table only
                unique_table_df = table_df.drop_duplicates(subset=['product_name'], keep='first')
                duplicates_in_table = initial_count - len(unique_table_df)
                total_duplicates_removed += duplicates_in_table
                unique_products_per_table.append(unique_table_df)
                print(f"  ✅ {table_name}: {initial_count:,} → {len(unique_table_df):,} unique (removed {duplicates_in_table:,} duplicates)")
        
        # Combine all unique products from all tables (WITHOUT cross-table deduplication)
        unique_df = pd.concat(unique_products_per_table, ignore_index=True)
        final_count = len(unique_df)
        
        print(f"\n  ✅ Total duplicates removed (within tables): {total_duplicates_removed:,}")
        print(f"  📊 Total unique product names (sum of unique per table): {final_count}")
        print(f"  ℹ️  Note: Products may appear in multiple tables (no cross-table deduplication)")
        
        return unique_df, table_stats
    else:
        print("❌ No products fetched from any Supabase table.")
        return pd.DataFrame(), {}

def show_duplicate_analysis(products_df, table_stats):
    """Show detailed analysis of duplicate products per table"""
    
    print(f"\n" + "=" * 80)
    print("🔍 DETAILED DUPLICATE ANALYSIS BY TABLE (WITHIN TABLE ONLY)")
    print("=" * 80)
    
    print(f"\n{'TABLE NAME':<30} | {'RAW COUNT':<12} | {'UNIQUE NAMES':<15} | {'DUPLICATES':<12} | {'% DUPLICATES'}")
    print("-" * 80)
    
    total_unique = 0
    for table_name in settings.SUPABASE_TABLES:
        if table_name in table_stats:
            stats = table_stats[table_name]
            raw = stats['raw_count']
            unique = stats['unique_count']
            dupes = stats['duplicates_count']
            total_unique += unique
            
            if raw > 0:
                dup_percent = (dupes / raw) * 100
                print(f"{table_name:<30} | {raw:<12,} | {unique:<15} | {dupes:<12,} | {dup_percent:>10.1f}%")
            else:
                print(f"{table_name:<30} | {raw:<12} | {unique:<15} | {dupes:<12} | {'N/A':>10}")
    
    print("-" * 80)
    print(f"{'TOTAL (sum of unique per table)':<30} | {'':<12} | {total_unique:<15} | {'':<12} | {'':>10}")
    
    print(f"\nℹ️  NOTE: Duplicates are removed WITHIN each table only.")
    print(f"ℹ️  Products may appear in multiple tables (no cross-table deduplication).")

def display_all_product_names(products_df):
    """Display all product names from all eight Supabase tables in the terminal"""
    
    print(f"\n" + "=" * 80)
    print("📦 ALL UNIQUE PRODUCTS (ID + NAME) FROM ALL SUPABASE TABLES")
    print("=" * 80)
    print("ℹ️  Showing unique products per table (no cross-table deduplication)")
    print("=" * 80)
    
    if products_df.empty:
        print("❌ No products found!")
        return
    
    # Count total unique products per table
    total_unique_per_table = {}
    for table_name in settings.SUPABASE_TABLES:
        table_count = len(products_df[products_df['source_table'] == table_name])
        total_unique_per_table[table_name] = table_count
    
    total_count = len(products_df)
    
    print(f"\nTotal unique products (sum across all tables): {total_count}\n")
    
    # Display all product names grouped by table
    for table_name in settings.SUPABASE_TABLES:
        table_products = products_df[products_df['source_table'] == table_name].copy()
        if not table_products.empty:
            # Sort by product name within each table
            table_products = table_products.sort_values('product_name')
            print(f"\n{'─' * 80}")
            print(f"📋 TABLE: {table_name} ({len(table_products)} unique products)")
            print(f"{'─' * 80}")
            
            for i, (_, row) in enumerate(table_products.iterrows(), 1):
                print(f"  {i:3d}. ID: {row['product_id']:<40} | {row['product_name']}")
        else:
            print(f"\n{'─' * 80}")
            print(f"📋 TABLE: {table_name} (0 unique products)")
            print(f"{'─' * 80}")
            print("  No products found in this table")
    
    # Also show complete alphabetical list
    print(f"\n{'=' * 80}")
    print(f"📝 COMPLETE ALPHABETICAL LIST OF ALL UNIQUE PRODUCTS ({total_count} total)")
    print(f"{'=' * 80}")
    print(f"ℹ️  Products may appear multiple times if they exist in multiple tables")
    print(f"{'=' * 80}\n")
    
    # Sort all products alphabetically
    sorted_all = products_df.sort_values('product_name')
    for i, (_, row) in enumerate(sorted_all.iterrows(), 1):
        print(f"  {i:3d}. ID: {row['product_id']:<40} | {row['product_name']} [{row['source_table']}]")
    
    print(f"\n{'=' * 80}")
    print(f"✅ Total: {total_count} unique product names displayed")
    print(f"{'=' * 80}\n")

def show_products_summary(products_df):
    """Show summary of all Supabase products"""
    
    print(f"\n📋 SUPABASE PRODUCTS SUMMARY")
    print("=" * 50)
    
    if products_df.empty:
        print("No products found!")
        return
    
    # Total count
    total_count = len(products_df)
    print(f"Total unique products: {total_count}")
    
    # Count by source table
    table_counts = products_df['source_table'].value_counts()
    print(f"\nUnique products by table:")
    for table, count in table_counts.items():
        percentage = (count / total_count) * 100
        print(f"  {table}: {count} products ({percentage:.1f}%)")
    
    # Show sample products from each table
    print(f"\n  Sample products from each table:")
    for table_name in settings.SUPABASE_TABLES:
        table_products = products_df[products_df['source_table'] == table_name]
        if not table_products.empty:
            print(f"\n  📋 {table_name} ({len(table_products)} products):")
            sample_products = table_products.head(3)
            for i, (_, row) in enumerate(sample_products.iterrows(), 1):
                print(f"    {i}. {row['product_name']} (ID: {row['product_id']})")
            if len(table_products) > 3:
                print(f"    ... and {len(table_products) - 3} more products")
        else:
            print(f"\n  📋 {table_name}: No products found")

def export_to_csv(products_df):
    """Export all products to CSV file"""
    
    print("\n📄 Exporting to CSV...")
    
    try:
        csv_filename = "all_supabase_products.csv"
        products_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(products_df)} products to {csv_filename}")
        
        # Show sample from CSV
        print("Sample from CSV file:")
        sample_df = products_df.head(5)
        for i, (_, row) in enumerate(sample_df.iterrows(), 1):
            print(f"  {i}. {row['product_name']} (ID: {row['product_id']}) - {row['source_table']}")
            
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def create_local_table(products_df):
    """Create a local table with all Supabase products"""
    
    print("\n💾 Creating local table with all Supabase products...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.all_supabase_products CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing all_supabase_products table if it existed")
            
            # Create new table
            create_query = """
            CREATE TABLE public.all_supabase_products (
                product_id TEXT NOT NULL,
                product_name TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE,
                source_table TEXT NOT NULL,
                source_type TEXT DEFAULT 'Supabase'
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created all_supabase_products table")
            
            # Insert data
            products_df.to_sql(
                name='all_supabase_products',
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"  ✅ Inserted {len(products_df)} products into local table")
            
            # Verify insertion
            verify_query = "SELECT COUNT(*) as count FROM public.all_supabase_products"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(products_df):
                print(f"  ✅ Verification: {inserted_count} products inserted successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(products_df)}, got {inserted_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating local table: {e}")
        return False

def main():
    """Main function"""
    print("Listing All Products from All Supabase Tables")
    print("=" * 60)
    
    try:
        # Step 1: Get all products from all Supabase tables
        all_products_df, table_stats = get_all_supabase_products()
        if all_products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Show duplicate analysis first (before displaying products)
        show_duplicate_analysis(all_products_df, table_stats)
        
        # Step 3: Display ALL product names in terminal
        display_all_product_names(all_products_df)
        
        print(f"\n🎯 SUPABASE PRODUCTS LISTING COMPLETED!")
        print(f"✅ Total unique products displayed: {len(all_products_df)}")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
