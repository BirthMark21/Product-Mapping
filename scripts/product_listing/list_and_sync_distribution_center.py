#!/usr/bin/env python3
"""
Script to list products from remote Supabase distribution_center_prices table
and sync them to local database
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

def get_remote_distribution_center_products():
    """Get all products from remote Supabase distribution_center_prices table"""
    
    print("Fetching products from remote Supabase distribution_center_prices table...")
    print("=" * 70)
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        if not supabase_engine:
            print("❌ Failed to get Supabase database engine.")
            return pd.DataFrame()
        
        with supabase_engine.connect() as conn:
            # Query to get all products with their details
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
                print("❌ No products found in remote Supabase distribution_center_prices table!")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} products in remote Supabase distribution_center_prices table")
            
            # Get unique product names
            unique_products = df.drop_duplicates(subset=['product_name'])
            print(f"📊 Unique product names: {len(unique_products)}")
            
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase distribution_center_prices: {e}")
        return pd.DataFrame()

def create_local_distribution_center_table():
    """Create local distribution_center_prices table in local database"""
    
    print("\nCreating local distribution_center_prices table...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.distribution_center_prices CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing distribution_center_prices table if it existed")
            
            # Create new distribution_center_prices table
            create_query = """
            CREATE TABLE public.distribution_center_prices (
                id TEXT PRIMARY KEY,
                product_name TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created local distribution_center_prices table")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating local distribution_center_prices table: {e}")
        return False

def sync_products_to_local(products_df):
    """Sync products from remote to local database"""
    
    print("\nSyncing products to local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        # Create local table first
        table_created = create_local_distribution_center_table()
        if not table_created:
            print("Failed to create local distribution_center_prices table")
            return False
        
        # Prepare data for insertion
        print("  📝 Preparing products data for local sync...")
        
        # Create a clean dataframe with required columns
        clean_df = products_df[['product_id', 'product_name', 'created_at', 'updated_at']].copy()
        clean_df = clean_df.drop_duplicates(subset=['product_id'])
        clean_df = clean_df.dropna(subset=['product_id', 'product_name'])
        
        print(f"  📊 Prepared {len(clean_df)} unique products for local sync")
        
        # Insert data into local distribution_center_prices table in very small chunks
        chunk_size = 10  # Insert only 10 records at a time (40 parameters max)
        total_rows = len(clean_df)
        
        print(f"  📝 Inserting {total_rows} products in chunks of {chunk_size}...")
        
        for i in range(0, total_rows, chunk_size):
            chunk_df = clean_df.iloc[i:i+chunk_size]
            
            # Use individual INSERT statements for each row
            for _, row in chunk_df.iterrows():
                try:
                    insert_query = text("""
                        INSERT INTO public.distribution_center_prices 
                        (id, product_name, created_at, updated_at) 
                        VALUES (:product_id, :product_name, :created_at, :updated_at)
                    """)
                    
                    with local_engine.connect() as conn:
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
        
        print(f"  ✅ Successfully synced {len(clean_df)} products to local database")
        
        # Verify sync
        with local_engine.connect() as conn:
            verify_query = "SELECT COUNT(*) as count FROM public.distribution_center_prices"
            verify_df = pd.read_sql(verify_query, conn)
            synced_count = verify_df['count'].iloc[0]
            
            if synced_count == len(clean_df):
                print(f"  ✅ Verification: {synced_count} products synced successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(clean_df)}, got {synced_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error syncing products to local database: {e}")
        return False

def show_sync_summary(products_df):
    """Show summary of synced products"""
    
    print(f"\n📋 SYNC SUMMARY")
    print("=" * 30)
    
    if products_df.empty:
        print("No products to sync!")
        return
    
    # Basic statistics
    total_products = len(products_df)
    unique_products = len(products_df.drop_duplicates(subset=['product_name']))
    
    print(f"Total product records: {total_products}")
    print(f"Unique product names: {unique_products}")
    
    # Show sample products
    print(f"\nSample synced products (first 10):")
    sample_df = products_df.head(10)
    for i, (_, row) in enumerate(sample_df.iterrows(), 1):
        print(f"  {i:2d}. {row['product_name']} (ID: {row['product_id']})")
    
    if total_products > 10:
        print(f"  ... and {total_products - 10} more products")
    
    # Check for products with name '0'
    zero_products = products_df[products_df['product_name'].astype(str).str.strip() == '0']
    if not zero_products.empty:
        print(f"\n⚠️ Found {len(zero_products)} products with name '0':")
        for _, row in zero_products.iterrows():
            print(f"   - ID: {row['product_id']}, Name: '{row['product_name']}'")
    else:
        print(f"\n✅ No products with name '0' found.")

def export_to_csv(products_df):
    """Export products to CSV file"""
    
    print("\n📄 Exporting to CSV...")
    
    try:
        csv_filename = "distribution_center_products_synced.csv"
        products_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(products_df)} products to {csv_filename}")
        
        # Also export unique products only
        unique_products = products_df.drop_duplicates(subset=['product_name'])
        unique_csv_filename = "distribution_center_unique_products_synced.csv"
        unique_products.to_csv(unique_csv_filename, index=False)
        print(f"✅ Exported {len(unique_products)} unique products to {unique_csv_filename}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def verify_local_table():
    """Verify the local table was created correctly"""
    
    print("\n🔍 Verifying local table...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Check table structure
            structure_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'distribution_center_prices' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            structure_df = pd.read_sql(structure_query, conn)
            
            print("  📋 Local table structure:")
            for _, row in structure_df.iterrows():
                nullable = "NULL" if row['is_nullable'] == 'YES' else 'NOT NULL'
                print(f"    {row['column_name']}: {row['data_type']} ({nullable})")
            
            # Check record count
            count_query = "SELECT COUNT(*) as count FROM public.distribution_center_prices"
            count_df = pd.read_sql(count_query, conn)
            record_count = count_df['count'].iloc[0]
            
            print(f"  📊 Total records in local table: {record_count}")
            
            # Show sample data
            if record_count > 0:
                sample_query = """
                SELECT product_name, created_at 
                FROM public.distribution_center_prices 
                LIMIT 5
                """
                sample_df = pd.read_sql(sample_query, conn)
                print(f"  📝 Sample data:")
                for _, row in sample_df.iterrows():
                    print(f"    - {row['product_name']} (created: {row['created_at']})")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error verifying local table: {e}")
        return False

def main():
    """Main function"""
    print("Distribution Center Products - List and Sync to Local Database")
    print("=" * 70)
    print("📥 Source: Remote Supabase distribution_center_prices table")
    print("🎯 Goal: List products and sync to local database")
    print("=" * 70)
    
    try:
        # Step 1: Get all products from remote Supabase
        products_df = get_remote_distribution_center_products()
        if products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Show summary
        show_sync_summary(products_df)
        
        # Step 3: Sync to local database
        sync_success = sync_products_to_local(products_df)
        if not sync_success:
            print("Failed to sync products to local database")
            return
        
        # Step 4: Verify local table
        verify_local_table()
        
        # Step 5: Export to CSV
        export_to_csv(products_df)
        
        print(f"\n🎯 DISTRIBUTION CENTER SYNC COMPLETED!")
        print(f"📁 Local database: public.distribution_center_prices table created")
        print(f"📄 CSV files created:")
        print(f"  - distribution_center_products_synced.csv")
        print(f"  - distribution_center_unique_products_synced.csv")
        print(f"📊 Total products synced: {len(products_df)}")
        print(f"📊 Unique products: {len(products_df.drop_duplicates(subset=['product_name']))}")
        print(f"🔗 Source: Remote Supabase distribution_center_prices table")
        print(f"💾 Destination: Local hub database")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
