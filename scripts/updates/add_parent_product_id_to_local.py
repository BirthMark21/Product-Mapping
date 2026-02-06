#!/usr/bin/env python3
"""
Script to add parent_product_id to LOCAL database tables
Uses the canonical_products_master table to get parent_product_id values
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_local_db_engine():
    """Create database engine for local PostgreSQL"""
    local_url = f"postgresql://{os.getenv('HUB_PG_USER')}:{os.getenv('HUB_PG_PASSWORD')}@{os.getenv('HUB_PG_HOST')}:{os.getenv('HUB_PG_PORT')}/{os.getenv('HUB_PG_DB_NAME')}"
    return create_engine(local_url)

def get_canonical_master_data(engine):
    """Get parent_product_id mapping from canonical_products_master table"""
    
    print("🔍 Getting parent_product_id mapping from canonical_products_master...")
    
    try:
        with engine.connect() as conn:
            # Get all data from canonical_products_master
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
                print("❌ No data found in canonical_products_master table!")
                return None
            
            print(f"✅ Found {len(df)} parent groups in canonical_products_master")
            return df
            
    except Exception as e:
        print(f"❌ Error getting canonical master data: {e}")
        return None

def create_parent_mapping_from_canonical(canonical_df):
    """Create parent_product_id mapping from canonical data"""
    
    print("🔄 Creating parent_product_id mapping...")
    
    parent_mapping = {}
    
    for _, row in canonical_df.iterrows():
        parent_product_id = row['parent_product_id']
        parent_name = row['parent_product_name']
        child_ids = row['child_product_ids']
        child_names = row['child_product_names']
        
        # Add mapping for each child product ID
        if isinstance(child_ids, list):
            for child_id in child_ids:
                parent_mapping[str(child_id)] = {
                    'parent_product_id': parent_product_id,
                    'parent_name': parent_name
                }
        
        # Also add mapping by product name (in case we need it)
        if isinstance(child_names, list):
            for child_name in child_names:
                parent_mapping[child_name.lower().strip()] = {
                    'parent_product_id': parent_product_id,
                    'parent_name': parent_name
                }
    
    print(f"✅ Created mapping for {len(parent_mapping)} child products")
    return parent_mapping

def add_parent_product_id_columns(engine):
    """Add parent_product_id columns to all LOCAL tables"""
    
    print("🏗️  Adding parent_product_id columns to LOCAL tables...")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    for table in tables:
        try:
            with engine.connect() as conn:
                # Add parent_product_id column if it doesn't exist
                alter_query = f"""
                ALTER TABLE public.{table} 
                ADD COLUMN IF NOT EXISTS parent_product_id UUID;
                """
                conn.execute(text(alter_query))
                conn.commit()
                print(f"✅ Added parent_product_id column to {table}")
                
        except Exception as e:
            print(f"❌ Error adding column to {table}: {e}")

def populate_parent_product_ids(engine, parent_mapping):
    """Populate parent_product_id values based on canonical master table"""
    
    print("📝 Populating parent_product_id values in LOCAL database...")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    for table in tables:
        try:
            print(f"🔄 Processing {table}...")
            
            with engine.connect() as conn:
                # Get all records to update
                select_query = f"SELECT id, product_name FROM public.{table}"
                df = pd.read_sql(select_query, conn)
                
                if df.empty:
                    print(f"   ⚠️  No records in {table}")
                    continue
                
                updated_count = 0
                
                for _, row in df.iterrows():
                    record_id = str(row['id'])
                    product_name = str(row['product_name']).lower().strip()
                    
                    # Try to find parent_product_id by record ID first
                    parent_info = parent_mapping.get(record_id)
                    
                    # If not found by ID, try by product name
                    if not parent_info:
                        parent_info = parent_mapping.get(product_name)
                    
                    if parent_info:
                        # Update the record with parent_product_id
                        update_query = f"""
                        UPDATE public.{table} 
                        SET parent_product_id = '{parent_info['parent_product_id']}'
                        WHERE id = '{record_id}';
                        """
                        conn.execute(text(update_query))
                        updated_count += 1
                
                conn.commit()
                print(f"   ✅ Updated {updated_count:,} records in {table}")
                
        except Exception as e:
            print(f"❌ Error populating {table}: {e}")

def verify_parent_product_ids(engine):
    """Verify that parent_product_id population worked correctly"""
    
    print("🔍 Verifying parent_product_id population in LOCAL database...")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    total_with_parent = 0
    total_records = 0
    
    for table in tables:
        try:
            with engine.connect() as conn:
                # Count records with parent_product_id
                count_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(parent_product_id) as records_with_parent
                FROM public.{table}
                """
                result = conn.execute(text(count_query))
                row = result.fetchone()
                
                if row:
                    table_total = row[0]
                    table_with_parent = row[1]
                    
                    print(f"📊 {table}: {table_with_parent:,}/{table_total:,} records have parent_product_id ({table_with_parent/table_total*100:.1f}%)")
                    
                    total_records += table_total
                    total_with_parent += table_with_parent
                    
        except Exception as e:
            print(f"❌ Error verifying {table}: {e}")
    
    print(f"\n📈 Overall: {total_with_parent:,}/{total_records:,} records have parent_product_id ({total_with_parent/total_records*100:.1f}%)")

def show_sample_mappings(engine):
    """Show sample parent_product_id mappings"""
    
    print("📋 Sample parent_product_id mappings:")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    for table in tables:
        try:
            with engine.connect() as conn:
                # Get sample records with parent_product_id
                sample_query = f"""
                SELECT id, product_name, parent_product_id 
                FROM public.{table} 
                WHERE parent_product_id IS NOT NULL 
                LIMIT 3
                """
                df = pd.read_sql(sample_query, conn)
                
                if not df.empty:
                    print(f"\n📊 {table} samples:")
                    for _, row in df.iterrows():
                        print(f"   ID: {row['id'][:8]}... | Product: {row['product_name'][:30]}... | Parent ID: {row['parent_product_id'][:8]}...")
                        
        except Exception as e:
            print(f"❌ Error showing samples for {table}: {e}")

def main():
    """Main function"""
    print("🚀 Adding Parent Product ID to LOCAL Database Tables")
    print("=" * 60)
    print("📥 Source: Local canonical_products_master table")
    print("🎯 Goal: Add parent_product_id columns and populate them")
    print("=" * 60)
    
    try:
        # Create local database connection
        engine = get_local_db_engine()
        
        # Step 1: Get canonical master data
        canonical_df = get_canonical_master_data(engine)
        if canonical_df is None:
            print("❌ Cannot proceed without canonical master data!")
            print("💡 Make sure you have run the ETL pipeline first to create canonical_products_master table")
            return
        
        # Step 2: Create parent mapping from canonical data
        parent_mapping = create_parent_mapping_from_canonical(canonical_df)
        
        # Step 3: Add parent_product_id columns to all tables
        add_parent_product_id_columns(engine)
        
        # Step 4: Populate parent_product_id values
        populate_parent_product_ids(engine, parent_mapping)
        
        # Step 5: Verify the results
        verify_parent_product_ids(engine)
        
        # Step 6: Show sample mappings
        show_sample_mappings(engine)
        
        print("\n✅ Parent Product ID addition completed successfully!")
        print("🎯 Your LOCAL tables now have parent_product_id relationships!")
        print("📊 All parent_product_id values come from your canonical_products_master table!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
