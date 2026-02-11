#!/usr/bin/env python3
"""
Script to list all products from the Supabase 'products' table.
Displays all product names including duplicates.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys

# Add the parent directory to the path so we can import config and utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def get_supabase_products():
    """Get all products from the products table without deduplication"""
    
    print("Fetching all products from Supabase 'products' table...")
    print("=" * 60)
    
    supabase_engine = get_db_engine('supabase')
    
    if not supabase_engine:
        print("❌ Failed to get Supabase database engine.")
        return pd.DataFrame()

    table_name = "products"
    
    try:
        with supabase_engine.connect() as conn:
            print(f"📋 Fetching from table: {table_name}...")
            
            # Query strictly for 'products' table
            query = """
            SELECT 
                id::text AS product_id, 
                name AS product_name, 
                created_at
            FROM public.products
            ORDER BY name
            """
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                df['source_table'] = table_name
                df['source_type'] = 'Supabase'
                print(f"  ✅ Fetched {len(df)} products from {table_name}")
                return df
            else:
                print(f"  ⚠️ No products found in {table_name}")
                return pd.DataFrame()
                    
    except Exception as e:
        print(f"  ❌ Error fetching from {table_name}: {e}")
        return pd.DataFrame()

def display_product_list(products_df):
    """Display all products in the terminal"""
    
    print(f"\n" + "=" * 80)
    print("📦 ALL PRODUCTS FROM 'products' TABLE (INCLUDING DUPLICATES)")
    print("=" * 80)
    
    if products_df.empty:
        print("❌ No products found!")
        return
    
    total_count = len(products_df)
    
    print(f"Total count: {total_count}")
    print("-" * 80)
    
    # Sort by name for easier reading
    sorted_df = products_df.sort_values('product_name')
    
    for i, (_, row) in enumerate(sorted_df.iterrows(), 1):
        # Handle potential None/NaN values for clean output
        p_name = str(row['product_name']) if row['product_name'] is not None else "None"
        p_id = str(row['product_id']) if row['product_id'] is not None else "None"
        
        print(f"{i:4d}. {p_name:<50} | ID: {p_id}")
    
    print("-" * 80)
    print(f"✅ Total listed: {total_count}")
    print("=" * 80)

def main():
    """Main function"""
    try:
        # Step 1: Get raw products
        products_df = get_supabase_products()
        
        if products_df.empty:
            return
            
        # Step 2: Display list
        display_product_list(products_df)
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
