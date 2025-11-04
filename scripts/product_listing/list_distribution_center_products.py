#!/usr/bin/env python3
"""
Script to list all products from distribution_center_prices table
Shows product ID, name, and creation date for each product
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

def get_distribution_center_products():
    """Get all products from distribution_center_prices table"""
    
    print("Fetching all products from distribution_center_prices table...")
    print("=" * 60)
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        if not supabase_engine:
            print("❌ Failed to get Supabase database engine.")
            return pd.DataFrame()
        
        with supabase_engine.connect() as conn:
            query = """
            SELECT 
                id::text AS product_id, 
                product_name, 
                created_at,
                updated_at
            FROM public.distribution_center_prices
            WHERE product_name IS NOT NULL 
            AND product_name != ''
            ORDER BY product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("❌ No products found in distribution_center_prices table!")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} products in distribution_center_prices table")
            
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from distribution_center_prices: {e}")
        return pd.DataFrame()

def show_products_summary(products_df):
    """Show summary of distribution center products"""
    
    print(f"\n📋 DISTRIBUTION CENTER PRODUCTS SUMMARY")
    print("=" * 50)
    
    if products_df.empty:
        print("No products found!")
        return
    
    # Total count
    total_count = len(products_df)
    print(f"Total products: {total_count}")
    
    # Show all products with their details
    print(f"\nAll Distribution Center Products:")
    print("-" * 50)
    for i, (_, row) in enumerate(products_df.iterrows(), 1):
        print(f"{i:3d}. \"{row['product_name']}\" \"{row['created_at']}\"")
    
    # Check for products with name '0'
    zero_products = products_df[products_df['product_name'].astype(str).str.strip() == '0']
    if not zero_products.empty:
        print(f"\n⚠️ Found {len(zero_products)} products with name '0':")
        for _, row in zero_products.iterrows():
            print(f"   - ID: {row['product_id']}, Name: '{row['product_name']}'")
    else:
        print(f"\n✅ No products with name '0' found.")

def create_local_table(products_df):
    """Create a local table with distribution center products"""
    
    print("\n💾 Creating local table with distribution center products...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.distribution_center_prices CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing distribution_center_prices table if it existed")
            
            # Create new table
            create_query = """
            CREATE TABLE public.distribution_center_prices (
                id TEXT PRIMARY KEY,
                product_name TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created distribution_center_prices table")
            
            # Insert data in small chunks to avoid parameter limits
            chunk_size = 10
            total_rows = len(products_df)
            
            print(f"  📝 Inserting {total_rows} products in chunks of {chunk_size}...")
            
            for i in range(0, total_rows, chunk_size):
                chunk_df = products_df.iloc[i:i+chunk_size]
                
                # Use individual INSERT statements for each row
                for _, row in chunk_df.iterrows():
                    try:
                        insert_query = text("""
                            INSERT INTO public.distribution_center_prices 
                            (id, product_name, created_at, updated_at) 
                            VALUES (:product_id, :product_name, :created_at, :updated_at)
                        """)
                        
                        conn.execute(insert_query, {
                            'product_id': row['product_id'],
                            'product_name': row['product_name'],
                            'created_at': row['created_at'],
                            'updated_at': row['updated_at']
                        })
                        conn.commit()
                        
                    except Exception as e:
                        print(f"    ⚠️  Error inserting row {row['product_id']}: {e}")
                        continue
                
                print(f"  📝 Inserted chunk {i//chunk_size + 1}/{(total_rows-1)//chunk_size + 1} ({len(chunk_df)} records)")
            
            print(f"  ✅ Successfully inserted {len(products_df)} products to local database")
            
            # Verify insertion
            verify_query = "SELECT COUNT(*) as count FROM public.distribution_center_prices"
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

def export_to_csv(products_df):
    """Export all products to CSV file"""
    
    print("\n📄 Exporting to CSV...")
    
    try:
        csv_filename = "distribution_center_products.csv"
        products_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(products_df)} products to {csv_filename}")
        
        # Also export unique products only
        unique_products = products_df.drop_duplicates(subset=['product_name'])
        unique_csv_filename = "distribution_center_unique_products.csv"
        unique_products.to_csv(unique_csv_filename, index=False)
        print(f"✅ Exported {len(unique_products)} unique products to {unique_csv_filename}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("Listing All Products from Distribution Center Prices Table")
    print("=" * 60)
    
    try:
        # Step 1: Get all products from distribution_center_prices table
        products_df = get_distribution_center_products()
        if products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Show summary
        show_products_summary(products_df)
        
        # Step 3: Create local table
        create_local_table(products_df)
        
        # Step 4: Export to CSV (optional)
        export_to_csv(products_df)
        
        print(f"\n🎯 DISTRIBUTION CENTER PRODUCTS LISTING COMPLETED!")
        print(f"📁 Local database: public.distribution_center_prices table created")
        print(f"📄 CSV files created:")
        print(f"  - distribution_center_products.csv")
        print(f"  - distribution_center_unique_products.csv")
        print(f"📊 Total products: {len(products_df)}")
        print(f"📊 Unique products: {len(products_df.drop_duplicates(subset=['product_name']))}")
        print(f"🔗 Source: Remote Supabase distribution_center_prices table")
        print(f"💾 Destination: Local hub database")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()