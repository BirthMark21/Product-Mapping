#!/usr/bin/env python3
"""
Script to create a simple verification table with all products and their parent names
Shows all products from all 7 tables with their assigned parent names
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

def get_all_products_with_sources():
    """Get all products from all 7 tables with source information"""
    
    print("🔍 Loading all products from all 7 tables...")
    
    all_products = []
    
    try:
        # Connect to databases
        supabase_engine = get_db_engine('supabase')
        clickhouse_engine = get_db_engine('clickhouse')
        local_engine = get_db_engine('hub')
        
        # Load from ClickHouse
        print("   📥 Loading from ClickHouse...")
        clickhouse_df = load_all_product_data_from_clickhouse(clickhouse_engine)
        if not clickhouse_df.empty:
            clickhouse_df['source_table'] = 'clickhouse_product_names'
            clickhouse_df['table_type'] = 'ClickHouse'
            all_products.append(clickhouse_df)
            print(f"   ✅ ClickHouse: {len(clickhouse_df)} products")
        
        # Load from Supabase
        print("   📥 Loading from Supabase...")
        supabase_df = load_all_product_data_from_supabase(supabase_engine)
        if not supabase_df.empty:
            supabase_df['source_table'] = 'supabase_combined'
            supabase_df['table_type'] = 'Supabase'
            all_products.append(supabase_df)
            print(f"   ✅ Supabase: {len(supabase_df)} products")
        
        # Load from local tables individually
        print("   📥 Loading from local tables...")
        local_tables = [
            "farm_prices", "supermarket_prices", "distribution_center_prices",
            "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
        ]
        
        for table in local_tables:
            try:
                with local_engine.connect() as conn:
                    query = f"SELECT id::text AS raw_product_id, product_name AS raw_product_name FROM public.{table}"
                    df = pd.read_sql(query, conn)
                    if not df.empty:
                        df['source_table'] = f'local_{table}'
                        df['table_type'] = 'Local'
                        all_products.append(df)
                        print(f"   ✅ {table}: {len(df)} products")
            except Exception as e:
                print(f"   ❌ Error loading {table}: {e}")
        
        # Combine all products
        if all_products:
            combined_df = pd.concat(all_products, ignore_index=True)
            print(f"📊 Total products loaded: {len(combined_df)}")
            return combined_df
        else:
            print("❌ No products loaded from any source")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error loading products: {e}")
        return pd.DataFrame()

def get_parent_mapping_from_canonical():
    """Get parent mapping from canonical master table"""
    
    print("🔍 Loading parent mapping from canonical master...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_ids,
                child_product_names
            FROM public.canonical_products_master
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("❌ No data found in canonical master table!")
                return {}
            
            print(f"✅ Found {len(df)} parent groups in canonical master table")
            
            # Create parent mapping
            parent_mapping = {}
            
            for _, row in df.iterrows():
                parent_product_id = row['parent_product_id']
                parent_name = row['parent_product_name']
                child_ids = row['child_product_ids']
                child_names = row['child_product_names']
                
                # Parse child data
                if isinstance(child_ids, str):
                    try:
                        import ast
                        child_ids = ast.literal_eval(child_ids)
                    except:
                        child_ids = []
                elif not isinstance(child_ids, list):
                    child_ids = []
                
                if isinstance(child_names, str):
                    try:
                        import ast
                        child_names = ast.literal_eval(child_names)
                    except:
                        child_names = []
                elif not isinstance(child_names, list):
                    child_names = []
                
                # Create mapping for each child
                for child_id, child_name in zip(child_ids, child_names):
                    parent_mapping[str(child_id)] = parent_name
                    parent_mapping[child_name.lower().strip()] = parent_name
                
                print(f"   📋 {parent_name}: {len(child_ids)} child products")
            
            print(f"✅ Created parent mapping for {len(parent_mapping)} child products")
            return parent_mapping
            
    except Exception as e:
        print(f"❌ Error loading parent mapping: {e}")
        return {}

def create_verification_table(all_products_df, parent_mapping):
    """Create verification table with all products and their parent names"""
    
    print("🔄 Creating verification table...")
    
    verification_data = []
    
    for _, row in all_products_df.iterrows():
        product_id = row['raw_product_id']
        product_name = row['raw_product_name']
        source_table = row['source_table']
        table_type = row['table_type']
        
        # Find parent name for this product
        parent_name = parent_mapping.get(str(product_id))
        if not parent_name:
            parent_name = parent_mapping.get(product_name.lower().strip())
        if not parent_name:
            parent_name = "NO_PARENT_ASSIGNED"
        
        verification_data.append({
            'all_products': product_name,
            'parent_name': parent_name,
            'product_id': product_id,
            'source_table': source_table,
            'table_type': table_type
        })
    
    verification_df = pd.DataFrame(verification_data)
    print(f"✅ Created verification table with {len(verification_df)} products")
    return verification_df

def save_verification_table(verification_df):
    """Save verification table to local database"""
    
    print("💾 Saving verification table to local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        # Create verification table
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            conn.execute(text("DROP TABLE IF EXISTS public.product_verification_simple CASCADE;"))
            
            # Create new table
            create_query = """
            CREATE TABLE public.product_verification_simple (
                id SERIAL PRIMARY KEY,
                all_products TEXT NOT NULL,
                parent_name TEXT NOT NULL,
                product_id TEXT,
                source_table TEXT,
                table_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("✅ Created product_verification_simple table")
        
        # Insert data
        verification_df.to_sql(
            name='product_verification_simple',
            con=local_engine,
            schema='public',
            if_exists='append',
            index=False
        )
        
        print("✅ Verification table saved to local database")
        
        # Show summary
        with local_engine.connect() as conn:
            # Count by parent name
            parent_query = """
            SELECT 
                parent_name,
                COUNT(*) as count
            FROM public.product_verification_simple
            GROUP BY parent_name
            ORDER BY count DESC
            LIMIT 10
            """
            parent_df = pd.read_sql(parent_query, conn)
            print("\n📊 Top 10 Parent Names:")
            for _, row in parent_df.iterrows():
                print(f"   {row['parent_name']}: {row['count']} products")
            
            # Count by table type
            table_query = """
            SELECT 
                table_type,
                COUNT(*) as count
            FROM public.product_verification_simple
            GROUP BY table_type
            ORDER BY count DESC
            """
            table_df = pd.read_sql(table_query, conn)
            print("\n📊 Products by Source:")
            for _, row in table_df.iterrows():
                print(f"   {row['table_type']}: {row['count']} products")
            
            # Count products without parent
            no_parent_query = """
            SELECT COUNT(*) as count
            FROM public.product_verification_simple
            WHERE parent_name = 'NO_PARENT_ASSIGNED'
            """
            no_parent_result = conn.execute(text(no_parent_query))
            no_parent_count = no_parent_result.scalar()
            print(f"\n⚠️  Products without parent: {no_parent_count}")
        
    except Exception as e:
        print(f"❌ Error saving verification table: {e}")

def export_verification_to_csv(verification_df):
    """Export verification table to CSV for manual review"""
    
    print("📄 Exporting verification table to CSV...")
    
    try:
        csv_filename = "product_verification_simple.csv"
        verification_df.to_csv(csv_filename, index=False)
        print(f"✅ Verification table exported to {csv_filename}")
        
        # Show sample data
        print("\n📋 Sample verification data:")
        sample_df = verification_df.head(10)
        for _, row in sample_df.iterrows():
            print(f"   {row['all_products']} → {row['parent_name']}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("🚀 Creating Simple Product Verification Table")
    print("=" * 60)
    print("📥 Source: All 7 tables + Canonical Master")
    print("🎯 Goal: Create table with all products and their parent names")
    print("=" * 60)
    
    try:
        # Step 1: Load all products from all tables
        all_products_df = get_all_products_with_sources()
        if all_products_df.empty:
            print("❌ No products loaded!")
            return
        
        # Step 2: Load parent mapping from canonical master
        parent_mapping = get_parent_mapping_from_canonical()
        if not parent_mapping:
            print("❌ No parent mapping found!")
            return
        
        # Step 3: Create verification table
        verification_df = create_verification_table(all_products_df, parent_mapping)
        
        # Step 4: Save to database
        save_verification_table(verification_df)
        
        # Step 5: Export to CSV
        export_verification_to_csv(verification_df)
        
        print("\n✅ Simple verification table creation completed!")
        print("🎯 You can now review the verification table in your database or CSV file!")
        print("📊 The table shows all products with their assigned parent names!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
