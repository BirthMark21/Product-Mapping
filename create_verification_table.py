#!/usr/bin/env python3
"""
Script to create a verification table for parent-child relationships
Shows all parent products and their child products from all 7 tables with verification checkboxes
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

def get_all_products_from_all_tables():
    """Get all products from all 7 tables (6 Supabase + 1 ClickHouse)"""
    
    print("🔍 Loading products from all 7 tables...")
    
    try:
        # Connect to databases
        supabase_engine = get_db_engine('supabase')
        clickhouse_engine = get_db_engine('clickhouse')
        local_engine = get_db_engine('hub')
        
        all_products = []
        
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
            # Add source table information for each Supabase table
            supabase_tables = settings.SUPABASE_TABLES
            supabase_df['source_table'] = 'supabase_combined'
            supabase_df['table_type'] = 'Supabase'
            all_products.append(supabase_df)
            print(f"   ✅ Supabase: {len(supabase_df)} products")
        
        # Load from local tables
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

def get_canonical_master_data():
    """Get canonical master table data"""
    
    print("🔍 Loading canonical master table...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
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
                print("❌ No data found in canonical master table!")
                return None
            
            print(f"✅ Found {len(df)} parent groups in canonical master table")
            return df
            
    except Exception as e:
        print(f"❌ Error loading canonical master: {e}")
        return None

def create_verification_table(canonical_df, all_products_df):
    """Create verification table with parent-child relationships"""
    
    print("🔄 Creating verification table...")
    
    verification_data = []
    
    for _, parent_row in canonical_df.iterrows():
        parent_id = parent_row['parent_product_id']
        parent_name = parent_row['parent_product_name']
        child_ids = parent_row['child_product_ids']
        child_names = parent_row['child_product_names']
        created_at = parent_row['created_at']
        
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
        
        # Create verification row for parent
        verification_data.append({
            'parent_product_id': parent_id,
            'parent_product_name': parent_name,
            'child_product_id': '',
            'child_product_name': '',
            'source_table': '',
            'table_type': '',
            'has_parent': '✅',
            'verification_status': 'PARENT',
            'created_at': created_at,
            'notes': f'Parent group with {len(child_ids)} child products'
        })
        
        # Create verification rows for each child
        for i, (child_id, child_name) in enumerate(zip(child_ids, child_names)):
            # Find the child product in all products
            child_info = all_products_df[
                (all_products_df['raw_product_id'] == str(child_id)) |
                (all_products_df['raw_product_name'].str.lower() == str(child_name).lower())
            ]
            
            if not child_info.empty:
                source_table = child_info.iloc[0]['source_table']
                table_type = child_info.iloc[0]['table_type']
                has_parent = '✅'
                verification_status = 'VERIFIED'
                notes = f'Child product found in {source_table}'
            else:
                source_table = 'NOT_FOUND'
                table_type = 'NOT_FOUND'
                has_parent = '❌'
                verification_status = 'NOT_FOUND'
                notes = 'Child product not found in any source table'
            
            verification_data.append({
                'parent_product_id': parent_id,
                'parent_product_name': parent_name,
                'child_product_id': child_id,
                'child_product_name': child_name,
                'source_table': source_table,
                'table_type': table_type,
                'has_parent': has_parent,
                'verification_status': verification_status,
                'created_at': created_at,
                'notes': notes
            })
    
    verification_df = pd.DataFrame(verification_data)
    print(f"✅ Created verification table with {len(verification_df)} rows")
    return verification_df

def save_verification_table(verification_df):
    """Save verification table to local database"""
    
    print("💾 Saving verification table to local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        # Create verification table
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            conn.execute(text("DROP TABLE IF EXISTS public.product_verification CASCADE;"))
            
            # Create new table
            create_query = """
            CREATE TABLE public.product_verification (
                id SERIAL PRIMARY KEY,
                parent_product_id UUID,
                parent_product_name TEXT,
                child_product_id TEXT,
                child_product_name TEXT,
                source_table TEXT,
                table_type TEXT,
                has_parent TEXT,
                verification_status TEXT,
                created_at TIMESTAMP,
                notes TEXT,
                created_at_verification TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("✅ Created product_verification table")
        
        # Insert data
        verification_df.to_sql(
            name='product_verification',
            con=local_engine,
            schema='public',
            if_exists='append',
            index=False
        )
        
        print("✅ Verification table saved to local database")
        
        # Show summary
        with local_engine.connect() as conn:
            # Count by verification status
            status_query = """
            SELECT 
                verification_status,
                COUNT(*) as count
            FROM public.product_verification
            GROUP BY verification_status
            ORDER BY count DESC
            """
            status_df = pd.read_sql(status_query, conn)
            print("\n📊 Verification Summary:")
            for _, row in status_df.iterrows():
                print(f"   {row['verification_status']}: {row['count']} records")
            
            # Count by table type
            table_query = """
            SELECT 
                table_type,
                COUNT(*) as count
            FROM public.product_verification
            WHERE verification_status = 'VERIFIED'
            GROUP BY table_type
            ORDER BY count DESC
            """
            table_df = pd.read_sql(table_query, conn)
            print("\n📊 Products by Source:")
            for _, row in table_df.iterrows():
                print(f"   {row['table_type']}: {row['count']} products")
        
    except Exception as e:
        print(f"❌ Error saving verification table: {e}")

def export_verification_to_csv(verification_df):
    """Export verification table to CSV for manual review"""
    
    print("📄 Exporting verification table to CSV...")
    
    try:
        csv_filename = "product_verification.csv"
        verification_df.to_csv(csv_filename, index=False)
        print(f"✅ Verification table exported to {csv_filename}")
        
        # Show sample data
        print("\n📋 Sample verification data:")
        sample_df = verification_df.head(10)
        for _, row in sample_df.iterrows():
            print(f"   {row['parent_product_name']} → {row['child_product_name']} ({row['verification_status']})")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("🚀 Creating Product Verification Table")
    print("=" * 60)
    print("📥 Source: All 7 tables + Canonical Master")
    print("🎯 Goal: Create verification table with parent-child relationships")
    print("=" * 60)
    
    try:
        # Step 1: Load all products from all tables
        all_products_df = get_all_products_from_all_tables()
        if all_products_df.empty:
            print("❌ No products loaded!")
            return
        
        # Step 2: Load canonical master data
        canonical_df = get_canonical_master_data()
        if canonical_df is None:
            print("❌ No canonical master data!")
            return
        
        # Step 3: Create verification table
        verification_df = create_verification_table(canonical_df, all_products_df)
        
        # Step 4: Save to database
        save_verification_table(verification_df)
        
        # Step 5: Export to CSV
        export_verification_to_csv(verification_df)
        
        print("\n✅ Verification table creation completed!")
        print("🎯 You can now review the verification table in your database or CSV file!")
        print("📊 The table shows all parent-child relationships with verification status!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
