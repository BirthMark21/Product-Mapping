#!/usr/bin/env python3
"""
Script to list all unique product names from local local_shop_prices table
Shows only unique product names (no duplicates)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def get_local_shop_unique_products():
    """Get all unique product names from remote Supabase local_shop_prices table"""
    
    print("Fetching unique product names from remote Supabase local_shop_prices table...")
    print("=" * 70)
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        with supabase_engine.connect() as conn:
            query = """
            SELECT DISTINCT product_name
            FROM public.local_shop_prices
            WHERE product_name IS NOT NULL 
            AND product_name != ''
            ORDER BY product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("❌ No products found in remote Supabase local_shop_prices table!")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} unique product names in remote Supabase local_shop_prices table")
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase local_shop_prices: {e}")
        return pd.DataFrame()

def show_products_summary(products_df):
    """Show summary of unique products"""
    
    print(f"\n📋 REMOTE SUPABASE LOCAL SHOP UNIQUE PRODUCTS SUMMARY")
    print("=" * 60)
    
    if products_df.empty:
        print("No products found!")
        return
    
    # Total count
    total_count = len(products_df)
    print(f"Total unique product names: {total_count}")
    
    # Show sample products
    print(f"\nSample unique product names:")
    sample_products = products_df.head(20)
    for i, (_, row) in enumerate(sample_products.iterrows(), 1):
        print(f"  {i}. {row['product_name']}")
    
    if total_count > 20:
        print(f"  ... and {total_count - 20} more unique products")
    
    # Show products starting with different letters
    print(f"\nProducts by first letter:")
    products_df['first_letter'] = products_df['product_name'].str[0].str.upper()
    letter_counts = products_df['first_letter'].value_counts().sort_index()
    
    for letter, count in letter_counts.head(10).items():
        print(f"  {letter}: {count} products")
    
    if len(letter_counts) > 10:
        print(f"  ... and {len(letter_counts) - 10} more letters")

def export_to_csv(products_df):
    """Export unique products to CSV file"""
    
    print("\n📄 Exporting to CSV...")
    
    try:
        csv_filename = "local_shop_unique_products.csv"
        products_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(products_df)} unique product names to {csv_filename}")
        
        # Show sample from CSV
        print("Sample from CSV file:")
        sample_df = products_df.head(5)
        for i, (_, row) in enumerate(sample_df.iterrows(), 1):
            print(f"  {i}. {row['product_name']}")
            
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def create_local_table(products_df):
    """Create a local table with unique product names"""
    
    print("\n💾 Creating local table with unique product names...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.local_shop_unique_products CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing local_shop_unique_products table if it existed")
            
            # Create new table
            create_query = """
            CREATE TABLE public.local_shop_unique_products (
                id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created local_shop_unique_products table")
            
            # Insert data (remove first_letter column if it exists)
            clean_df = products_df[['product_name']].copy()
            clean_df.to_sql(
                name='local_shop_unique_products',
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"  ✅ Inserted {len(clean_df)} unique product names into local table")
            
            # Verify insertion
            verify_query = "SELECT COUNT(*) as count FROM public.local_shop_unique_products"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(clean_df):
                print(f"  ✅ Verification: {inserted_count} unique product names inserted successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(clean_df)}, got {inserted_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating local table: {e}")
        return False

def search_products(products_df):
    """Allow searching for specific products"""
    
    print(f"\n🔍 PRODUCT SEARCH")
    print("=" * 30)
    
    # Show some search examples
    search_terms = ['Apple', 'Banana', 'Rice', 'Oil', 'Milk']
    
    for term in search_terms:
        matching_products = products_df[products_df['product_name'].str.contains(term, case=False, na=False)]
        if not matching_products.empty:
            print(f"Products containing '{term}': {len(matching_products)}")
            for _, row in matching_products.head(3).iterrows():
                print(f"  - {row['product_name']}")
            if len(matching_products) > 3:
                print(f"  ... and {len(matching_products) - 3} more")
        else:
            print(f"No products found containing '{term}'")

def main():
    """Main function"""
    print("Listing Unique Product Names from Remote Supabase local_shop_prices Table")
    print("=" * 80)
    
    try:
        # Step 1: Get unique product names from remote Supabase table
        unique_products_df = get_local_shop_unique_products()
        if unique_products_df.empty:
            print("No unique products found in remote Supabase table. Cannot proceed.")
            return
        
        # Step 2: Show summary
        show_products_summary(unique_products_df)
        
        # Step 3: Show search examples
        search_products(unique_products_df)
        
        # Step 4: Export to CSV
        export_to_csv(unique_products_df)
        
        # Step 5: Create local table
        create_local_table(unique_products_df)
        
        print(f"\n🎯 REMOTE SUPABASE LOCAL SHOP UNIQUE PRODUCTS LISTING COMPLETED!")
        print(f"📁 Unique product names are now available in:")
        print(f"  - Local database: public.local_shop_unique_products table")
        print(f"  - CSV file: local_shop_unique_products.csv")
        print(f"  - Total unique products: {len(unique_products_df)}")
        print(f"  - Source: Remote Supabase local_shop_prices table")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
