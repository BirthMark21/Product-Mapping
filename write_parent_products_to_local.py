#!/usr/bin/env python3
"""
Script to write parent product names with IDs to local master database
Creates a dedicated table for parent products from canonical master
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def get_parent_products_from_canonical_master():
    """Get parent products from canonical master table"""
    
    print("Loading parent products from canonical master...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                created_at
            FROM public.canonical_products_master
            ORDER BY parent_product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("No parent products found in canonical master table!")
                return pd.DataFrame()
            
            print(f"Found {len(df)} parent products in canonical master")
            
            # Show sample data
            print("Sample parent products:")
            sample_df = df.head(5)
            for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                print(f"  {i}. {row['parent_product_name']} (ID: {row['parent_product_id']})")
            
            if len(df) > 5:
                print(f"  ... and {len(df) - 5} more parent products")
            
            return df
            
    except Exception as e:
        print(f"Error loading parent products from canonical master: {e}")
        return pd.DataFrame()

def create_parent_products_table(local_engine):
    """Create dedicated parent_products table in local database"""
    
    print("Creating parent_products table in local database...")
    
    try:
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.parent_products CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing parent_products table if it existed")
            
            # Create new parent_products table
            create_query = """
            CREATE TABLE public.parent_products (
                id SERIAL PRIMARY KEY,
                parent_product_id TEXT NOT NULL UNIQUE,
                parent_product_name TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE,
                created_date DATE DEFAULT CURRENT_DATE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created parent_products table")
            
            # Create index on parent_product_id for faster lookups
            index_query = "CREATE INDEX idx_parent_products_id ON public.parent_products(parent_product_id)"
            conn.execute(text(index_query))
            conn.commit()
            print("  ✅ Created index on parent_product_id")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating parent_products table: {e}")
        return False

def write_parent_products_to_local(parent_products_df):
    """Write parent products to local database"""
    
    print("Writing parent products to local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        # Create the table first
        table_created = create_parent_products_table(local_engine)
        if not table_created:
            print("Failed to create parent_products table")
            return False
        
        # Prepare data for insertion
        print("  📝 Preparing parent products data...")
        
        # Create a clean dataframe with required columns
        clean_df = parent_products_df[['parent_product_id', 'parent_product_name', 'created_at']].copy()
        clean_df = clean_df.drop_duplicates(subset=['parent_product_id'])
        clean_df = clean_df.dropna(subset=['parent_product_id', 'parent_product_name'])
        
        print(f"  📊 Prepared {len(clean_df)} unique parent products for insertion")
        
        # Insert data into parent_products table
        with local_engine.connect() as conn:
            # Insert data
            clean_df.to_sql(
                name='parent_products',
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"  ✅ Successfully inserted {len(clean_df)} parent products")
            
            # Verify insertion
            verify_query = "SELECT COUNT(*) as count FROM public.parent_products"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(clean_df):
                print(f"  ✅ Verification: {inserted_count} parent products inserted successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(clean_df)}, got {inserted_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error writing parent products: {e}")
        return False

def show_parent_products_summary(local_engine):
    """Show summary of parent products in local database"""
    
    print(f"\n📊 PARENT PRODUCTS SUMMARY")
    print("=" * 50)
    
    try:
        with local_engine.connect() as conn:
            # Get total count
            count_query = "SELECT COUNT(*) as total FROM public.parent_products"
            count_df = pd.read_sql(count_query, conn)
            total_count = count_df['total'].iloc[0]
            
            print(f"Total parent products: {total_count}")
            
            # Show sample parent products
            sample_query = """
            SELECT parent_product_id, parent_product_name, created_at
            FROM public.parent_products
            ORDER BY parent_product_name
            LIMIT 10
            """
            sample_df = pd.read_sql(sample_query, conn)
            
            print(f"\nSample parent products:")
            for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                print(f"  {i}. {row['parent_product_name']} (ID: {row['parent_product_id']})")
            
            if total_count > 10:
                print(f"  ... and {total_count - 10} more parent products")
            
            # Show table structure
            structure_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'parent_products' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            structure_df = pd.read_sql(structure_query, conn)
            
            print(f"\nTable structure:")
            for _, row in structure_df.iterrows():
                nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                print(f"  - {row['column_name']}: {row['data_type']} ({nullable})")
                
    except Exception as e:
        print(f"❌ Error showing summary: {e}")

def export_parent_products_to_csv(parent_products_df):
    """Export parent products to CSV file"""
    
    print("Exporting parent products to CSV...")
    
    try:
        csv_filename = "parent_products.csv"
        parent_products_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(parent_products_df)} parent products to {csv_filename}")
        
        # Show sample from CSV
        print("Sample from CSV file:")
        sample_df = parent_products_df.head(5)
        for i, (_, row) in enumerate(sample_df.iterrows(), 1):
            print(f"  {i}. {row['parent_product_name']} (ID: {row['parent_product_id']})")
            
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("Writing Parent Products to Local Master Database")
    print("=" * 60)
    
    try:
        # Step 1: Get parent products from canonical master
        parent_products_df = get_parent_products_from_canonical_master()
        if parent_products_df.empty:
            print("No parent products found. Cannot proceed.")
            return
        
        # Step 2: Write parent products to local database
        success = write_parent_products_to_local(parent_products_df)
        if not success:
            print("Failed to write parent products to local database")
            return
        
        # Step 3: Show summary
        local_engine = get_db_engine('hub')
        show_parent_products_summary(local_engine)
        
        # Step 4: Export to CSV
        export_parent_products_to_csv(parent_products_df)
        
        print(f"\n🎯 PARENT PRODUCTS WRITE COMPLETED!")
        print(f"📁 Parent products are now available in:")
        print(f"  - Local database: public.parent_products table")
        print(f"  - CSV file: parent_products.csv")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
