#!/usr/bin/env python3
"""
Simple check of unassigned products
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def check_unassigned():
    """Simple check of unassigned products"""
    
    print("Checking unassigned products...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Get unassigned products with all details
            query = """
            SELECT 
                all_products,
                product_id,
                source_table,
                table_type,
                parent_assign,
                verification_status
            FROM public.product_verification_complete
            WHERE verification_status = 'NO_PARENT_IN_CANONICAL'
            ORDER BY table_type, source_table
            """
            
            df = pd.read_sql(query, conn)
            
            print(f"Found {len(df)} unassigned products:")
            
            for i, (_, row) in enumerate(df.iterrows(), 1):
                print(f"\n{i}. Product: '{row['all_products']}'")
                print(f"   ID: {row['product_id']}")
                print(f"   Source Table: {row['source_table']}")
                print(f"   Table Type: {row['table_type']}")
                print(f"   Parent Assign: {row['parent_assign']}")
                
            # Count by table type
            if len(df) > 0:
                clickhouse_count = len(df[df['table_type'] == 'ClickHouse'])
                supabase_count = len(df[df['table_type'] == 'Supabase'])
                
                print(f"\nSummary:")
                print(f"From ClickHouse: {clickhouse_count}")
                print(f"From Supabase: {supabase_count}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_unassigned()
