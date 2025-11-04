#!/usr/bin/env python3
"""
Script to list all products from all 6 Supabase tables
Shows product ID, name, and source table for each product
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def get_all_supabase_products():
    """Get all products from all 6 Supabase tables"""
    
    print("Fetching all products from all 6 Supabase tables...")
    print("=" * 60)
    
    all_products = []
    supabase_engine = get_db_engine('supabase')
    
    if not supabase_engine:
        print("❌ Failed to get Supabase database engine.")
        return pd.DataFrame()

    for table_name in settings.SUPABASE_TABLES:
        print(f"📋 Fetching from table: {table_name}...")
        try:
            with supabase_engine.connect() as conn:
                query = f"""
                SELECT 
                    id::text AS product_id, 
                    product_name, 
                    created_at
                FROM public.{table_name}
                ORDER BY product_name
                """
                df = pd.read_sql(query, conn)
                
                if not df.empty:
                    df['source_table'] = table_name
                    df['source_type'] = 'Supabase'
                    all_products.append(df)
                    print(f"  ✅ Fetched {len(df)} products from {table_name}")
                else:
                    print(f"  ⚠️ No products found in {table_name}")
                    
        except Exception as e:
            print(f"  ❌ Error fetching from {table_name}: {e}")
    
    if all_products:
        combined_df = pd.concat(all_products, ignore_index=True)
        print(f"\n📊 Total products fetched from all Supabase tables: {len(combined_df)}")
        
        # Remove duplicates based on product_name only (keep first occurrence)
        print("🔄 Removing duplicate products by name...")
        initial_count = len(combined_df)
        
        # Remove duplicates based on product_name only, keeping the first occurrence
        unique_df = combined_df.drop_duplicates(subset=['product_name'], keep='first')
        final_count = len(unique_df)
        duplicates_removed = initial_count - final_count
        
        print(f"  ✅ Removed {duplicates_removed} duplicate products")
        print(f"  📊 Unique product names: {final_count}")
        
        return unique_df
    else:
        print("❌ No products fetched from any Supabase table.")
        return pd.DataFrame()

def show_duplicate_analysis(products_df):
    """Show analysis of duplicate products"""
    
    print(f"\n🔍 DUPLICATE ANALYSIS")
    print("=" * 40)
    
    # Since we already removed duplicates by name, show what was removed
    print("✅ Duplicates have been removed based on product name")
    print("✅ Each unique product name appears only once in the final list")
    
    # Show some examples of unique products
    print(f"\nSample unique products:")
    sample_products = products_df.head(10)
    for i, (_, row) in enumerate(sample_products.iterrows(), 1):
        print(f"  {i}. {row['product_name']} (from {row['source_table']})")
    
    if len(products_df) > 10:
        print(f"  ... and {len(products_df) - 10} more unique products")

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
    print(f"\nSample products from each table:")
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
    print("Listing All Products from All 6 Supabase Tables")
    print("=" * 60)
    
    try:
        # Step 1: Get all products from all Supabase tables
        all_products_df = get_all_supabase_products()
        if all_products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Show duplicate analysis
        show_duplicate_analysis(all_products_df)
        
        # Step 3: Show summary
        show_products_summary(all_products_df)
        
        # Step 4: Export to CSV
        export_to_csv(all_products_df)
        
        # Step 5: Create local table
        create_local_table(all_products_df)
        
        print(f"\n🎯 SUPABASE PRODUCTS LISTING COMPLETED!")
        print(f"📁 All Supabase products are now available in:")
        print(f"  - Local database: public.all_supabase_products table")
        print(f"  - CSV file: all_supabase_products.csv")
        print(f"  - Total products: {len(all_products_df)}")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
