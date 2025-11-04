#!/usr/bin/env python3
"""
Detailed analysis of unassigned products to identify exact source tables
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def detailed_unassigned_analysis():
    """Detailed analysis of unassigned products with exact source information"""
    
    print("Detailed analysis of unassigned products...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Get detailed information about unassigned products
            unassigned_query = """
            SELECT 
                all_products,
                parent_assign,
                checkbox,
                product_id,
                source_table,
                table_type,
                verification_status
            FROM public.product_verification_complete
            WHERE parent_assign = 'NO_PARENT_IN_CANONICAL'
            OR verification_status = 'NO_PARENT_IN_CANONICAL'
            ORDER BY all_products, source_table
            """
            
            unassigned_df = pd.read_sql(unassigned_query, conn)
            
            print(f"Total unassigned products: {len(unassigned_df)}")
            
            if len(unassigned_df) > 0:
                print("\nDetailed breakdown of unassigned products:")
                for i, (_, row) in enumerate(unassigned_df.iterrows(), 1):
                    print(f"\n--- Product {i} ---")
                    print(f"Product Name: '{row['all_products']}'")
                    print(f"Product ID: {row['product_id']}")
                    print(f"Source Table: {row['source_table']}")
                    print(f"Table Type: {row['table_type']}")
                    print(f"Parent Assign: {row['parent_assign']}")
                    print(f"Checkbox: {row['checkbox']}")
                    print(f"Verification Status: {row['verification_status']}")
                
                # Check if any come from ClickHouse
                clickhouse_unassigned = unassigned_df[unassigned_df['table_type'] == 'ClickHouse']
                supabase_unassigned = unassigned_df[unassigned_df['table_type'] == 'Supabase']
                
                print(f"\nSummary:")
                print(f"From ClickHouse: {len(clickhouse_unassigned)} products")
                print(f"From Supabase: {len(supabase_unassigned)} products")
                
                if len(clickhouse_unassigned) > 0:
                    print(f"\nClickHouse unassigned products:")
                    for _, row in clickhouse_unassigned.iterrows():
                        print(f"  - '{row['all_products']}' (ID: {row['product_id']})")
                
                if len(supabase_unassigned) > 0:
                    print(f"\nSupabase unassigned products:")
                    for _, row in supabase_unassigned.iterrows():
                        print(f"  - '{row['all_products']}' (ID: {row['product_id']}) from {row['source_table']}")
                
            else:
                print("No unassigned products found!")
                
    except Exception as e:
        print(f"Error in detailed analysis: {e}")

def check_clickhouse_products():
    """Check what products are in ClickHouse that might be '0'"""
    
    print("\n" + "="*60)
    print("CHECKING CLICKHOUSE PRODUCTS")
    print("="*60)
    
    try:
        # Connect to ClickHouse via Superset API
        import requests
        import json
        import uuid
        
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"

        # Query for products with name '0' or similar
        sql_query = '''
        SELECT id, name, created_at 
        FROM "chipchip"."product_names" 
        WHERE name = '0' OR name LIKE '%0%' OR name = ''
        ORDER BY name
        '''

        unique_client_id = f"debug_{uuid.uuid4().hex[:6]}"

        payload = {
            "client_id": unique_client_id,
            "database_id": 1,
            "json": True,
            "runAsync": False,
            "schema": "chipchip",
            "sql": sql_query,
            "tab": "",
            "expand_data": True
        }

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'User-Agent': 'chipchip/bot'
        }

        response = requests.post(
            f"{superset_url}/api/v1/sqllab/execute/",
            headers=headers,
            json=payload,
            verify=False
        )

        if response.status_code == 200:
            result = response.json()
            if 'data' in result and result['data']:
                df = pd.DataFrame(result['data'], columns=['id', 'name', 'created_at'])
                print(f"Found {len(df)} products in ClickHouse with '0' or similar names:")
                for _, row in df.iterrows():
                    print(f"  - ID: {row['id']}, Name: '{row['name']}', Created: {row['created_at']}")
            else:
                print("No products with '0' found in ClickHouse")
        else:
            print(f"Error querying ClickHouse: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error checking ClickHouse: {e}")

def main():
    """Main function"""
    print("Detailed Analysis of Unassigned Products")
    print("=" * 60)
    
    # First do detailed analysis of unassigned products
    detailed_unassigned_analysis()
    
    # Then check ClickHouse specifically for '0' products
    check_clickhouse_products()
    
    print("\nDetailed analysis complete!")

if __name__ == "__main__":
    main()
