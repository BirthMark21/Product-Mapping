#!/usr/bin/env python3
"""
Generic script to list all unique products from any Supabase table
Usage: python list_supabase_table_products.py <table_name>
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys

# Add the parent directory to the path so we can import config and utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def get_supabase_table_products(table_name):
    """Get all unique products from specified remote Supabase table"""
    
    print(f"Fetching unique products from remote Supabase {table_name} table...")
    print("=" * 70)
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        if not supabase_engine:
            print("❌ Failed to get Supabase database engine.")
            return pd.DataFrame()
        
        with supabase_engine.connect() as conn:
            # Query to get all unique products with their details
            query = f"""
            SELECT 
                id::text AS product_id,
                product_name,
                created_at
            FROM public.{table_name}
            WHERE product_name IS NOT NULL 
            AND product_name != ''
            ORDER BY product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print(f"❌ No products found in remote Supabase {table_name} table!")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} products in remote Supabase {table_name} table")
            
            # Get unique product names
            unique_products = df.drop_duplicates(subset=['product_name'])
            print(f"📊 Unique product names: {len(unique_products)}")
            
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase {table_name}: {e}")
        return pd.DataFrame()

def show_product_summary(products_df, table_name):
    """Show summary of products found"""
    
    print(f"\n📋 {table_name.upper()} PRODUCTS SUMMARY")
    print("=" * 50)
    
    if products_df.empty:
        print("No products found!")
        return
    
    # Basic statistics
    total_products = len(products_df)
    unique_products = len(products_df.drop_duplicates(subset=['product_name']))
    
    print(f"Total product records: {total_products}")
    print(f"Unique product names: {unique_products}")
    
    # Show sample products
    print(f"\nSample products (first 20):")
    sample_df = products_df.head(20)
    for i, (_, row) in enumerate(sample_df.iterrows(), 1):
        print(f"  {i:2d}. {row['product_name']} (ID: {row['product_id']})")
    
    if total_products > 20:
        print(f"  ... and {total_products - 20} more products")
    
    # Check for products with name '0'
    zero_products = products_df[products_df['product_name'].astype(str).str.strip() == '0']
    if not zero_products.empty:
        print(f"\n⚠️ Found {len(zero_products)} products with name '0':")
        for _, row in zero_products.iterrows():
            print(f"   - ID: {row['product_id']}, Name: '{row['product_name']}'")
    else:
        print(f"\n✅ No products with name '0' found.")

def export_to_csv(products_df, table_name):
    """Export products to CSV file"""
    
    print("\n📄 Exporting to CSV...")
    
    try:
        csv_filename = f"{table_name}_products.csv"
        products_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(products_df)} products to {csv_filename}")
        
        # Also export unique products only
        unique_products = products_df.drop_duplicates(subset=['product_name'])
        unique_csv_filename = f"{table_name}_unique_products.csv"
        unique_products.to_csv(unique_csv_filename, index=False)
        print(f"✅ Exported {len(unique_products)} unique products to {unique_csv_filename}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    
    # Check if table name is provided
    if len(sys.argv) != 2:
        print("Usage: python list_supabase_table_products.py <table_name>")
        print("Available tables:")
        for table in settings.SUPABASE_TABLES:
            print(f"  - {table}")
        return
    
    table_name = sys.argv[1]
    
    print(f"{table_name.title()} Products Lister")
    print("=" * 50)
    print(f"📥 Source: Remote Supabase {table_name} table")
    print("🎯 Goal: List all unique products with details")
    print("=" * 50)
    
    try:
        # Step 1: Get all products from remote Supabase
        products_df = get_supabase_table_products(table_name)
        if products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Show summary
        show_product_summary(products_df, table_name)
        
        # Step 3: Export to CSV
        export_to_csv(products_df, table_name)
        
        print(f"\n🎯 {table_name.upper()} PRODUCTS LISTING COMPLETED!")
        print(f"📁 Files created:")
        print(f"  - {table_name}_products.csv (all products)")
        print(f"  - {table_name}_unique_products.csv (unique products only)")
        print(f"📊 Total products: {len(products_df)}")
        print(f"📊 Unique products: {len(products_df.drop_duplicates(subset=['product_name']))}")
        print(f"🔗 Source: Remote Supabase {table_name} table")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
