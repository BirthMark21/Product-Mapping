#!/usr/bin/env python3
"""
Debug script to check what's in the canonical master table
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def debug_canonical_master():
    """Debug the canonical master table structure"""
    
    print("Debugging canonical master table...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Get table structure
            structure_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'canonical_products_master' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            structure_df = pd.read_sql(structure_query, conn)
            print("Table Structure:")
            for _, row in structure_df.iterrows():
                print(f"   {row['column_name']}: {row['data_type']} ({'NULL' if row['is_nullable'] == 'YES' else 'NOT NULL'})")
            
            # Get sample data
            sample_query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_ids,
                child_product_names,
                created_at
            FROM public.canonical_products_master
            LIMIT 5
            """
            sample_df = pd.read_sql(sample_query, conn)
            
            print(f"\nSample data ({len(sample_df)} rows):")
            for i, (_, row) in enumerate(sample_df.iterrows()):
                print(f"\n--- Row {i+1} ---")
                print(f"Parent ID: {row['parent_product_id']}")
                print(f"Parent Name: {row['parent_product_name']}")
                print(f"Child IDs: {row['child_product_ids']} (type: {type(row['child_product_ids'])})")
                print(f"Child Names: {row['child_product_names']} (type: {type(row['child_product_names'])})")
                print(f"Created At: {row['created_at']}")
                
                # Try to parse the child data
                if isinstance(row['child_product_ids'], str):
                    try:
                        import ast
                        parsed_ids = ast.literal_eval(row['child_product_ids'])
                        print(f"Parsed Child IDs: {parsed_ids} (length: {len(parsed_ids)})")
                    except Exception as e:
                        print(f"Error parsing child IDs: {e}")
                
                if isinstance(row['child_product_names'], str):
                    try:
                        import ast
                        parsed_names = ast.literal_eval(row['child_product_names'])
                        print(f"Parsed Child Names: {parsed_names} (length: {len(parsed_names)})")
                    except Exception as e:
                        print(f"Error parsing child names: {e}")
            
            # Count total records
            count_query = "SELECT COUNT(*) as total FROM public.canonical_products_master"
            count_result = pd.read_sql(count_query, conn)
            print(f"\nTotal records in canonical master: {count_result['total'].iloc[0]}")
            
            # Check for non-empty child data
            non_empty_query = """
            SELECT COUNT(*) as non_empty_count
            FROM public.canonical_products_master
            WHERE child_product_ids IS NOT NULL 
            AND child_product_ids != '[]'
            AND child_product_ids != ''
            """
            non_empty_result = pd.read_sql(non_empty_query, conn)
            print(f"Records with non-empty child IDs: {non_empty_result['non_empty_count'].iloc[0]}")
            
    except Exception as e:
        print(f"Error debugging canonical master: {e}")

if __name__ == "__main__":
    debug_canonical_master()
