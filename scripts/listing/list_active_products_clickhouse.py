#!/usr/bin/env python3
"""
Script to fetch and list all ACTIVE products from ClickHouse.
Joins products table (status = 'Active') with product_names table via name_id.
"""

import pandas as pd
import requests
import uuid
import os
import traceback
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
# Suppress InsecureRequestWarning
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

def fetch_active_products():
    """Fetches all active products from ClickHouse with their product names."""
    
    print("Fetching ACTIVE products from ClickHouse...")
    
    try:
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"

        # Query to join products table with product_names table
        # Filter for status = 'Active'
        sql_query = '''
        SELECT 
            p.id AS product_id,
            p.name_id,
            p.status,
            p.vendor_id,
            pn.id AS product_name_id,
            pn.name AS product_name,
            pn.measuring_unit,
            pn.category_id,
            toString(p.created_at) AS product_created_at,
            toString(p.updated_at) AS product_updated_at
        FROM "chipchip"."products" p
        INNER JOIN "chipchip"."product_names" pn ON p.name_id = pn.id
        WHERE p.status = 'ACTIVE'
          AND p._peerdb_is_deleted = 0
          AND pn._peerdb_is_deleted = 0
          AND pn.name IS NOT NULL 
          AND pn.name != ''
          AND pn.name != '0'
        ORDER BY pn.name ASC
        '''

        payload = {
            "client_id": f"ch_{uuid.uuid4().hex[:6]}",
            "database_id": 1,
            "json": True,
            "runAsync": False,
            "schema": "chipchip",
            "sql": sql_query,
            "expand_data": True
        }

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'User-Agent': 'chipchip/bot'
        }

        print("Executing query to ClickHouse...")
        response = requests.post(
            f"{superset_url}/api/v1/sqllab/execute/",
            headers=headers,
            json=payload,
            verify=False,
            timeout=300
        )

        if response.status_code == 200:
            result = response.json()
            if 'data' in result and result['data']:
                df = pd.DataFrame(result['data'])
                
                print(f"Successfully fetched {len(df)} active products from ClickHouse")
                return df
            else:
                print("No data returned from ClickHouse")
                return pd.DataFrame()
        else:
            print(f"Error fetching from ClickHouse: {response.status_code} - {response.text}")
            return pd.DataFrame()

    except Exception as e:
        print(f"An exception occurred: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def display_active_products(df):
    """Display active products in the terminal"""
    
    if df.empty:
        print("\nNo active products found.")
        return
    
    print("\n" + "=" * 80)
    print("ACTIVE PRODUCTS FROM CLICKHOUSE")
    print("=" * 80)
    print(f"\nTotal active products: {len(df)}\n")
    
    # Get unique product names (since one product_name can have multiple product entries)
    unique_products = df.groupby(['product_name_id', 'product_name']).agg({
        'product_id': lambda x: list(x),
        'status': 'first',
        'measuring_unit': 'first'
    }).reset_index()
    
    print(f"Unique product names (with Active status): {len(unique_products)}\n")
    
    # List all active products
    for index, row in df.iterrows():
        try:
            product_id = str(row['product_id'])
            name_id = str(row['name_id'])
            product_name = str(row['product_name']).strip()
            status = str(row['status'])
            measuring_unit = str(row['measuring_unit']) if pd.notna(row['measuring_unit']) else 'N/A'
            
            print(f"{index + 1:4d}. Product ID: {product_id} | Name ID: {name_id}")
            print(f"     Name: {product_name}")
            print(f"     Status: {status} | Unit: {measuring_unit}")
            print()
        except UnicodeEncodeError:
            # Handle encoding errors for special characters
            product_id = str(row['product_id'])
            name_id = str(row['name_id'])
            product_name = str(row['product_name']).encode('ascii', 'replace').decode('ascii').strip()
            status = str(row['status'])
            measuring_unit = str(row['measuring_unit']) if pd.notna(row['measuring_unit']) else 'N/A'
            
            print(f"{index + 1:4d}. Product ID: {product_id} | Name ID: {name_id}")
            print(f"     Name: {product_name}")
            print(f"     Status: {status} | Unit: {measuring_unit}")
            print()
    
    print("=" * 80)
    print(f"Total: {len(df)} active products listed")
    print(f"Unique product names: {len(unique_products)}")
    print("=" * 80)
    
    return unique_products

def main():
    """Main function"""
    print("=" * 80)
    print("CLICKHOUSE ACTIVE PRODUCTS LISTER")
    print("=" * 80)
    print()
    
    # Fetch active products
    df = fetch_active_products()
    
    if df.empty:
        print("\nNo active products were found or an error occurred.")
        return
    
    # Display in terminal
    unique_products = display_active_products(df)
    
    # Export to CSV
    os.makedirs('fmcg_exports', exist_ok=True)
    output_file = 'fmcg_exports/active_products_clickhouse.csv'
    
    # Export full details
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nExported full details to: {output_file}")
    
    # Export unique products summary
    unique_output = 'fmcg_exports/active_products_unique_summary.csv'
    unique_products_export = unique_products[['product_name_id', 'product_name', 'measuring_unit', 'status']].copy()
    unique_products_export['product_count'] = unique_products['product_id'].apply(len)
    unique_products_export.to_csv(unique_output, index=False, encoding='utf-8-sig')
    print(f"Exported unique products summary to: {unique_output}")

if __name__ == "__main__":
    main()

