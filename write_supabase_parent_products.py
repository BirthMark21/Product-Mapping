#!/usr/bin/env python3
"""
Script to write parent product names with IDs that come from Supabase sources only
Filters parent products based on their source (Supabase vs ClickHouse)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
import ast

# Load environment variables
load_dotenv()

def get_supabase_parent_products_from_canonical_master():
    """Get parent products that come from Supabase sources only"""
    
    print("Loading parent products from Supabase sources only...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # First, let's get all products from Supabase tables to create a lookup
            print("  Getting Supabase product IDs for filtering...")
            supabase_engine = get_db_engine('supabase')
            supabase_product_ids = set()
            
            if supabase_engine:
                with supabase_engine.connect() as supabase_conn:
                    for table_name in settings.SUPABASE_TABLES:
                        try:
                            query = f"SELECT id::text FROM public.{table_name}"
                            result = pd.read_sql(query, supabase_conn)
                            supabase_product_ids.update(result['id'].tolist())
                        except Exception as e:
                            print(f"    Warning: Could not get IDs from {table_name}: {e}")
            
            print(f"  Found {len(supabase_product_ids)} unique product IDs in Supabase tables")
            
            # Now get the canonical master data
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_ids,
                child_product_names,
                created_at
            FROM public.canonical_products_master
            ORDER BY parent_product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("No parent products found in canonical master table!")
                return pd.DataFrame()
            
            print(f"Found {len(df)} total parent products in canonical master")
            
            # Filter for Supabase-only parent products
            supabase_parents = []
            
            for _, row in df.iterrows():
                parent_product_id = row['parent_product_id']
                parent_name = row['parent_product_name']
                child_ids = row['child_product_ids']
                child_names = row['child_product_names']
                created_at = row['created_at']
                
                # Parse child data - handle PostgreSQL array format
                if isinstance(child_ids, str):
                    try:
                        if child_ids.startswith('{') and child_ids.endswith('}'):
                            child_ids = child_ids[1:-1].split(',')
                            child_ids = [id.strip() for id in child_ids if id.strip()]
                        else:
                            child_ids = ast.literal_eval(child_ids)
                    except:
                        child_ids = []
                elif not isinstance(child_ids, list):
                    child_ids = []
                
                if isinstance(child_names, str):
                    try:
                        if child_names.startswith('{') and child_names.endswith('}'):
                            child_names = child_names[1:-1].split(',')
                            child_names = [name.strip().strip('"') for name in child_names if name.strip()]
                        else:
                            child_names = ast.literal_eval(child_names)
                    except:
                        child_names = []
                elif not isinstance(child_names, list):
                    child_names = []
                
                # Check if this parent has any children from Supabase sources
                # Use the pre-loaded Supabase product IDs for efficient lookup
                has_supabase_children = False
                
                for child_id in child_ids:
                    if str(child_id) in supabase_product_ids:
                        has_supabase_children = True
                        break
                
                # If this parent has Supabase children, include it
                if has_supabase_children:
                    supabase_parents.append({
                        'parent_product_id': parent_product_id,
                        'parent_product_name': parent_name
                    })
            
            if not supabase_parents:
                print("No parent products found with Supabase children!")
                return pd.DataFrame()
            
            supabase_df = pd.DataFrame(supabase_parents)
            print(f"Found {len(supabase_df)} parent products with Supabase children")
            
            # Show sample data
            print("Sample Supabase parent products:")
            sample_df = supabase_df.head(5)
            for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                print(f"  {i}. {row['parent_product_name']} (ID: {row['parent_product_id']})")
            
            if len(supabase_df) > 5:
                print(f"  ... and {len(supabase_df) - 5} more Supabase parent products")
            
            return supabase_df
            
    except Exception as e:
        print(f"Error loading Supabase parent products: {e}")
        return pd.DataFrame()

def create_supabase_parent_products_table(local_engine):
    """Create dedicated supabase_parent_products table in local database"""
    
    print("Creating supabase_parent_products table in local database...")
    
    try:
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.supabase_parent_products CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing supabase_parent_products table if it existed")
            
            # Create new supabase_parent_products table
            create_query = """
            CREATE TABLE public.supabase_parent_products (
                parent_product_id TEXT PRIMARY KEY,
                parent_product_name TEXT NOT NULL
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created supabase_parent_products table")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating supabase_parent_products table: {e}")
        return False

def write_supabase_parent_products_to_local(supabase_parents_df):
    """Write Supabase parent products to local database"""
    
    print("Writing Supabase parent products to local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        # Create the table first
        table_created = create_supabase_parent_products_table(local_engine)
        if not table_created:
            print("Failed to create supabase_parent_products table")
            return False
        
        # Prepare data for insertion
        print("  📝 Preparing Supabase parent products data...")
        
        # Create a clean dataframe with required columns
        clean_df = supabase_parents_df[['parent_product_id', 'parent_product_name']].copy()
        clean_df = clean_df.drop_duplicates(subset=['parent_product_id'])
        clean_df = clean_df.dropna(subset=['parent_product_id', 'parent_product_name'])
        
        print(f"  📊 Prepared {len(clean_df)} unique Supabase parent products for insertion")
        
        # Insert data into supabase_parent_products table
        clean_df.to_sql(
            name='supabase_parent_products',
            con=local_engine,
            schema='public',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        print(f"  ✅ Successfully inserted {len(clean_df)} Supabase parent products")
        
        # Verify insertion
        with local_engine.connect() as conn:
            verify_query = "SELECT COUNT(*) as count FROM public.supabase_parent_products"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(clean_df):
                print(f"  ✅ Verification: {inserted_count} Supabase parent products inserted successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(clean_df)}, got {inserted_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error writing Supabase parent products: {e}")
        return False

def show_supabase_parent_products_summary(local_engine):
    """Show summary of Supabase parent products in local database"""
    
    print(f"\n📊 SUPABASE PARENT PRODUCTS SUMMARY")
    print("=" * 50)
    
    try:
        with local_engine.connect() as conn:
            # Get total count
            count_query = "SELECT COUNT(*) as total FROM public.supabase_parent_products"
            count_df = pd.read_sql(count_query, conn)
            total_count = count_df['total'].iloc[0]
            
            print(f"Total Supabase parent products: {total_count}")
            
            # Get statistics
            print(f"Statistics:")
            print(f"  Total Supabase parent products: {total_count}")
            
            # Show sample parent products
            sample_query = """
            SELECT parent_product_id, parent_product_name
            FROM public.supabase_parent_products
            ORDER BY parent_product_name
            LIMIT 10
            """
            sample_df = pd.read_sql(sample_query, conn)
            
            print(f"\nSample Supabase parent products:")
            for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                print(f"  {i}. {row['parent_product_name']} (ID: {row['parent_product_id']})")
            
            if total_count > 10:
                print(f"  ... and {total_count - 10} more Supabase parent products")
            
            # Show table structure
            structure_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'supabase_parent_products' 
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

def export_supabase_parent_products_to_csv(supabase_parents_df):
    """Export Supabase parent products to CSV file"""
    
    print("Exporting Supabase parent products to CSV...")
    
    try:
        csv_filename = "supabase_parent_products.csv"
        supabase_parents_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(supabase_parents_df)} Supabase parent products to {csv_filename}")
        
        # Show sample from CSV
        print("Sample from CSV file:")
        sample_df = supabase_parents_df.head(5)
        for i, (_, row) in enumerate(sample_df.iterrows(), 1):
            print(f"  {i}. {row['parent_product_name']} (ID: {row['parent_product_id']})")
            
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("Writing Supabase Parent Products to Local Master Database")
    print("=" * 60)
    
    try:
        # Step 1: Get Supabase parent products from canonical master
        supabase_parents_df = get_supabase_parent_products_from_canonical_master()
        if supabase_parents_df.empty:
            print("No Supabase parent products found. Cannot proceed.")
            return
        
        # Step 2: Write Supabase parent products to local database
        success = write_supabase_parent_products_to_local(supabase_parents_df)
        if not success:
            print("Failed to write Supabase parent products to local database")
            return
        
        # Step 3: Show summary
        local_engine = get_db_engine('hub')
        show_supabase_parent_products_summary(local_engine)
        
        # Step 4: Export to CSV
        export_supabase_parent_products_to_csv(supabase_parents_df)
        
        print(f"\n🎯 SUPABASE PARENT PRODUCTS WRITE COMPLETED!")
        print(f"📁 Supabase parent products are now available in:")
        print(f"  - Local database: public.supabase_parent_products table")
        print(f"  - CSV file: supabase_parent_products.csv")
        print(f"  - Only parent products with Supabase children are included")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
