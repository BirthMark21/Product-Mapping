#!/usr/bin/env python3
"""
Script to create verification table directly from canonical master table
Shows all parent-child relationships from the canonical master table
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

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

def create_verification_table_from_master(canonical_df):
    """Create verification table directly from canonical master table"""
    
    print("🔄 Creating verification table from canonical master...")
    
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
            'all_products': parent_name,
            'parent_name': parent_name,
            'product_id': parent_id,
            'product_type': 'PARENT',
            'created_at': created_at,
            'notes': f'Parent group with {len(child_ids)} child products'
        })
        
        # Create verification rows for each child
        for child_id, child_name in zip(child_ids, child_names):
            verification_data.append({
                'all_products': child_name,
                'parent_name': parent_name,
                'product_id': child_id,
                'product_type': 'CHILD',
                'created_at': created_at,
                'notes': f'Child of {parent_name}'
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
            conn.execute(text("DROP TABLE IF EXISTS public.product_verification_master CASCADE;"))
            
            # Create new table
            create_query = """
            CREATE TABLE public.product_verification_master (
                id SERIAL PRIMARY KEY,
                all_products TEXT NOT NULL,
                parent_name TEXT NOT NULL,
                product_id TEXT,
                product_type TEXT,
                created_at TIMESTAMP,
                notes TEXT,
                created_at_verification TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("✅ Created product_verification_master table")
        
        # Insert data
        verification_df.to_sql(
            name='product_verification_master',
            con=local_engine,
            schema='public',
            if_exists='append',
            index=False
        )
        
        print("✅ Verification table saved to local database")
        
        # Show summary
        with local_engine.connect() as conn:
            # Count by product type
            type_query = """
            SELECT 
                product_type,
                COUNT(*) as count
            FROM public.product_verification_master
            GROUP BY product_type
            ORDER BY count DESC
            """
            type_df = pd.read_sql(type_query, conn)
            print("\n📊 Products by Type:")
            for _, row in type_df.iterrows():
                print(f"   {row['product_type']}: {row['count']} products")
            
            # Count by parent name (top 10)
            parent_query = """
            SELECT 
                parent_name,
                COUNT(*) as count
            FROM public.product_verification_master
            WHERE product_type = 'CHILD'
            GROUP BY parent_name
            ORDER BY count DESC
            LIMIT 10
            """
            parent_df = pd.read_sql(parent_query, conn)
            print("\n📊 Top 10 Parent Names (by child count):")
            for _, row in parent_df.iterrows():
                print(f"   {row['parent_name']}: {row['count']} children")
        
    except Exception as e:
        print(f"❌ Error saving verification table: {e}")

def export_verification_to_csv(verification_df):
    """Export verification table to CSV for manual review"""
    
    print("📄 Exporting verification table to CSV...")
    
    try:
        csv_filename = "product_verification_master.csv"
        verification_df.to_csv(csv_filename, index=False)
        print(f"✅ Verification table exported to {csv_filename}")
        
        # Show sample data
        print("\n📋 Sample verification data:")
        sample_df = verification_df.head(10)
        for _, row in sample_df.iterrows():
            print(f"   {row['all_products']} → {row['parent_name']} ({row['product_type']})")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    """Main function"""
    print("🚀 Creating Verification Table from Canonical Master")
    print("=" * 60)
    print("📥 Source: Canonical Master Table")
    print("🎯 Goal: Create verification table with all parent-child relationships")
    print("=" * 60)
    
    try:
        # Step 1: Load canonical master data
        canonical_df = get_canonical_master_data()
        if canonical_df is None:
            print("❌ No canonical master data!")
            return
        
        # Step 2: Create verification table from canonical master
        verification_df = create_verification_table_from_master(canonical_df)
        
        # Step 3: Save to database
        save_verification_table(verification_df)
        
        # Step 4: Export to CSV
        export_verification_to_csv(verification_df)
        
        print("\n✅ Verification table creation completed!")
        print("🎯 You can now review the verification table in your database or CSV file!")
        print("📊 The table shows all parent-child relationships from the canonical master table!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
