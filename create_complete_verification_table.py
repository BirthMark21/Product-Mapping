#!/usr/bin/env python3
"""
Script to create complete verification table with ALL products from all 7 tables
Shows all products with their parent assignments and verification checkboxes
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
from pipeline.data_loader import load_all_product_data_from_clickhouse, load_all_product_data_from_supabase
import ast

# Load environment variables
load_dotenv()

def get_all_products_from_all_tables():
    """Get ALL products from all 7 tables (ClickHouse + 6 Supabase)"""
    
    print("🔍 Loading ALL products from all 7 tables...")
    
    all_products = []
    
    try:
        # Connect to databases
        supabase_engine = get_db_engine('supabase')
        clickhouse_engine = get_db_engine('clickhouse')
        
        # Load from ClickHouse
        print("   📥 Loading from ClickHouse...")
        clickhouse_df = load_all_product_data_from_clickhouse(clickhouse_engine)
        if not clickhouse_df.empty:
            clickhouse_df['source_table'] = 'clickhouse_product_names'
            clickhouse_df['table_type'] = 'ClickHouse'
            all_products.append(clickhouse_df)
            print(f"   ✅ ClickHouse: {len(clickhouse_df)} products")
        
        # Load from Supabase (all 6 tables)
        print("   📥 Loading from Supabase...")
        supabase_df = load_all_product_data_from_supabase(supabase_engine)
        if not supabase_df.empty:
            supabase_df['source_table'] = 'supabase_combined'
            supabase_df['table_type'] = 'Supabase'
            all_products.append(supabase_df)
            print(f"   ✅ Supabase: {len(supabase_df)} products")
        
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

def get_canonical_master_relationships():
    """Get parent-child relationships from canonical master table"""
    
    print("🔍 Loading parent-child relationships from canonical master...")
    
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
            
            # Debug: Show sample data from canonical master
            print("\n🔍 Sample data from canonical master:")
            sample_df = df.head(3)
            for _, row in sample_df.iterrows():
                print(f"   Parent: {row['parent_product_name']}")
                print(f"   Child IDs: {row['child_product_ids']} (type: {type(row['child_product_ids'])})")
                print(f"   Child Names: {row['child_product_names']} (type: {type(row['child_product_names'])})")
                print("   ---")
            
            # Create parent-child mapping from canonical master
            parent_child_mapping = {}
            
            for _, row in df.iterrows():
                parent_product_id = row['parent_product_id']
                parent_name = row['parent_product_name']
                child_ids = row['child_product_ids']
                child_names = row['child_product_names']
                
                # Parse child data - handle PostgreSQL array format
                if isinstance(child_ids, str):
                    try:
                        # Handle PostgreSQL array format {item1,item2,item3}
                        if child_ids.startswith('{') and child_ids.endswith('}'):
                            # Remove curly braces and split by comma
                            child_ids = child_ids[1:-1].split(',')
                            # Clean up any extra whitespace
                            child_ids = [id.strip() for id in child_ids if id.strip()]
                        else:
                            # Try Python list format
                            child_ids = ast.literal_eval(child_ids)
                    except:
                        child_ids = []
                elif not isinstance(child_ids, list):
                    child_ids = []
                
                if isinstance(child_names, str):
                    try:
                        # Handle PostgreSQL array format {item1,item2,item3}
                        if child_names.startswith('{') and child_names.endswith('}'):
                            # Remove curly braces and split by comma
                            child_names = child_names[1:-1].split(',')
                            # Clean up any extra whitespace and quotes
                            child_names = [name.strip().strip('"') for name in child_names if name.strip()]
                        else:
                            # Try Python list format
                            child_names = ast.literal_eval(child_names)
                    except:
                        child_names = []
                elif not isinstance(child_names, list):
                    child_names = []
                
                # Create mapping for each child from canonical master
                for child_id, child_name in zip(child_ids, child_names):
                    parent_child_mapping[str(child_id)] = {
                        'parent_name': parent_name,
                        'parent_id': parent_product_id,
                        'child_name': child_name
                    }
                    parent_child_mapping[child_name.lower().strip()] = {
                        'parent_name': parent_name,
                        'parent_id': parent_product_id,
                        'child_name': child_name
                    }
                
                print(f"   📋 {parent_name}: {len(child_ids)} child products")
            
            print(f"✅ Created parent-child mapping for {len(parent_child_mapping)} relationships from canonical master")
            return parent_child_mapping
            
    except Exception as e:
        print(f"❌ Error loading canonical master relationships: {e}")
        return {}

def create_complete_verification_table(all_products_df, parent_child_mapping):
    """Create complete verification table with all products and parent assignments from canonical master"""
    
    print("🔄 Creating complete verification table using canonical master relationships...")
    
    verification_data = []
    
    for _, row in all_products_df.iterrows():
        product_id = row['raw_product_id']
        product_name = row['raw_product_name']
        source_table = row['source_table']
        table_type = row['table_type']
        
        # Find parent-child relationship from canonical master
        relationship = parent_child_mapping.get(str(product_id))
        if not relationship:
            relationship = parent_child_mapping.get(product_name.lower().strip())
        
        # Determine if product has parent based on canonical master
        if relationship and relationship.get('parent_name'):
            parent_name = relationship['parent_name']
            has_parent = "✅"
            verification_status = "HAS_PARENT_FROM_CANONICAL"
        else:
            parent_name = "NO_PARENT_IN_CANONICAL"
            has_parent = "❌"
            verification_status = "NO_PARENT_IN_CANONICAL"
        
        verification_data.append({
            'all_products': product_name,
            'parent_assign': parent_name,
            'checkbox': has_parent,
            'product_id': product_id,
            'source_table': source_table,
            'table_type': table_type,
            'verification_status': verification_status
        })
    
    verification_df = pd.DataFrame(verification_data)
    print(f"✅ Created verification table with {len(verification_df)} products using canonical master relationships")
    return verification_df

def save_verification_table(verification_df):
    """Save verification table to local database"""
    
    print("💾 Saving verification table to local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        # Create verification table
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            conn.execute(text("DROP TABLE IF EXISTS public.product_verification_complete CASCADE;"))
            
            # Create new table
            create_query = """
            CREATE TABLE public.product_verification_complete (
                id SERIAL PRIMARY KEY,
                all_products TEXT NOT NULL,
                parent_assign TEXT NOT NULL,
                checkbox TEXT NOT NULL,
                product_id TEXT,
                source_table TEXT,
                table_type TEXT,
                verification_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("✅ Created product_verification_complete table")
        
        # Insert data
        verification_df.to_sql(
            name='product_verification_complete',
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
            FROM public.product_verification_complete
            GROUP BY verification_status
            ORDER BY count DESC
            """
            status_df = pd.read_sql(status_query, conn)
            print("\n📊 Verification Summary:")
            for _, row in status_df.iterrows():
                print(f"   {row['verification_status']}: {row['count']} products")
            
            # Count by table type
            table_query = """
            SELECT 
                table_type,
                COUNT(*) as count
            FROM public.product_verification_complete
            GROUP BY table_type
            ORDER BY count DESC
            """
            table_df = pd.read_sql(table_query, conn)
            print("\n📊 Products by Source:")
            for _, row in table_df.iterrows():
                print(f"   {row['table_type']}: {row['count']} products")
            
            # Count by checkbox status
            checkbox_query = """
            SELECT 
                checkbox,
                COUNT(*) as count
            FROM public.product_verification_complete
            GROUP BY checkbox
            ORDER BY count DESC
            """
            checkbox_df = pd.read_sql(checkbox_query, conn)
            print("\n📊 Products by Checkbox Status:")
            for _, row in checkbox_df.iterrows():
                print(f"   {row['checkbox']}: {row['count']} products")
        
    except Exception as e:
        print(f"❌ Error saving verification table: {e}")

def export_verification_to_csv(verification_df):
    """Export verification table to CSV for manual review"""
    
    print("📄 Exporting verification table to CSV...")
    
    try:
        csv_filename = "product_verification_complete.csv"
        verification_df.to_csv(csv_filename, index=False)
        print(f"✅ Verification table exported to {csv_filename}")
        
        # Show sample data
        print("\n📋 Sample verification data:")
        sample_df = verification_df.head(10)
        for _, row in sample_df.iterrows():
            print(f"   {row['all_products']} → {row['parent_assign']} {row['checkbox']}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("🚀 Creating Complete Product Verification Table")
    print("=" * 60)
    print("📥 Source: All 7 tables (ClickHouse + 6 Supabase)")
    print("🎯 Goal: Create verification table with all products and parent assignments")
    print("=" * 60)
    
    try:
        # Step 1: Load all products from all tables
        all_products_df = get_all_products_from_all_tables()
        if all_products_df.empty:
            print("❌ No products loaded!")
            return
        
        # Step 2: Load parent-child relationships from canonical master
        parent_child_mapping = get_canonical_master_relationships()
        if not parent_child_mapping:
            print("❌ No parent-child relationships found in canonical master!")
            return
        
        # Step 3: Create complete verification table using canonical master relationships
        verification_df = create_complete_verification_table(all_products_df, parent_child_mapping)
        
        # Step 4: Save to database
        save_verification_table(verification_df)
        
        # Step 5: Export to CSV
        export_verification_to_csv(verification_df)
        
        print("\n✅ Complete verification table creation completed!")
        print("🎯 You can now review the verification table in your database or CSV file!")
        print("📊 The table shows ALL products with their parent assignments and checkboxes!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
