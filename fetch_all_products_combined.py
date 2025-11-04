#!/usr/bin/env python3
"""
Script to fetch all products from all sources (ClickHouse + Supabase) with detailed source information
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
import requests
import json
import uuid

# Load environment variables
load_dotenv()

def fetch_clickhouse_products():
    """Fetch products from ClickHouse"""
    
    print("Fetching products from ClickHouse...")
    
    try:
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"

        sql_query = '''
        SELECT 
            id AS product_id,
            name AS product_name,
            toString(created_at) AS created_at,
            category_id,
            created_by,
            measuring_unit,
            toString(updated_at) AS updated_at,
            toString(deleted_at) AS deleted_at,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."product_names"
        ORDER BY name
        '''

        unique_client_id = f"c_{uuid.uuid4().hex[:4]}"

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
                df = pd.DataFrame(result['data'], columns=[
                    'product_id', 'product_name', 'created_at', 
                    'category_id', 'created_by', 'measuring_unit',
                    'updated_at', 'deleted_at', '_peerdb_synced_at',
                    '_peerdb_is_deleted', '_peerdb_version'
                ])
                
                # Add source information
                df['source_table'] = 'clickhouse_product_names'
                df['source_type'] = 'ClickHouse'
                df['database'] = 'chipchip'
                df['schema'] = 'chipchip'
                
                print(f"  ClickHouse: {len(df)} products")
                return df
            else:
                print("  ClickHouse: No data returned")
                return pd.DataFrame()
        else:
            print(f"  ClickHouse: Error {response.status_code}")
            return pd.DataFrame()

    except Exception as e:
        print(f"  ClickHouse: Error - {e}")
        return pd.DataFrame()

def fetch_supabase_products():
    """Fetch products from all Supabase tables"""
    
    print("Fetching products from Supabase...")
    
    try:
        supabase_engine = get_db_engine('supabase')
        supabase_tables = settings.SUPABASE_TABLES
        
        all_products = []
        
        for table in supabase_tables:
            try:
                query = f"""
                SELECT 
                    id::text AS product_id,
                    product_name,
                    created_at,
                    '{table}' AS source_table,
                    'Supabase' AS source_type,
                    'postgres' AS database,
                    '{settings.SUPABASE_SCHEMA}' AS schema
                FROM {settings.SUPABASE_SCHEMA}.{table}
                ORDER BY product_name
                """
                
                with supabase_engine.connect() as conn:
                    df = pd.read_sql(query, conn)
                    
                    if not df.empty:
                        print(f"  {table}: {len(df)} products")
                        all_products.append(df)
                    else:
                        print(f"  {table}: 0 products")
                        
            except Exception as e:
                print(f"  {table}: Error - {e}")
        
        if all_products:
            combined_df = pd.concat(all_products, ignore_index=True)
            print(f"  Total Supabase: {len(combined_df)} products")
            return combined_df
        else:
            print("  Supabase: No products found")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"  Supabase: Error - {e}")
        return pd.DataFrame()

def analyze_products(df, source_name):
    """Analyze products and show statistics"""
    
    if df.empty:
        print(f"No {source_name} products to analyze")
        return
    
    print(f"\n{source_name} Analysis:")
    print(f"  Total products: {len(df)}")
    print(f"  Products with names: {len(df[df['product_name'].notna()])}")
    print(f"  Products with empty names: {len(df[df['product_name'].isna() | (df['product_name'] == '')])}")
    print(f"  Products with name '0': {len(df[df['product_name'] == '0'])}")
    
    # Show '0' products if any
    zero_products = df[df['product_name'] == '0']
    if len(zero_products) > 0:
        print(f"  '0' products details:")
        for _, row in zero_products.iterrows():
            print(f"    - ID: {row['product_id']}, Source: {row['source_table']}")
    
    # Show breakdown by source table if multiple tables
    if 'source_table' in df.columns:
        table_counts = df['source_table'].value_counts()
        if len(table_counts) > 1:
            print(f"  Breakdown by table:")
            for table, count in table_counts.items():
                print(f"    {table}: {count} products")

def main():
    """Main function"""
    print("All Products Fetcher - Combined Analysis")
    print("=" * 60)
    
    # Fetch from ClickHouse
    clickhouse_df = fetch_clickhouse_products()
    
    # Fetch from Supabase
    supabase_df = fetch_supabase_products()
    
    # Analyze each source
    analyze_products(clickhouse_df, "ClickHouse")
    analyze_products(supabase_df, "Supabase")
    
    # Combine all products
    all_products = []
    if not clickhouse_df.empty:
        all_products.append(clickhouse_df)
    if not supabase_df.empty:
        all_products.append(supabase_df)
    
    if all_products:
        combined_df = pd.concat(all_products, ignore_index=True)
        
        print(f"\nCombined Analysis:")
        print(f"  Total products from all sources: {len(combined_df)}")
        
        # Show breakdown by source type
        source_counts = combined_df['source_type'].value_counts()
        print(f"  Breakdown by source:")
        for source, count in source_counts.items():
            print(f"    {source}: {count} products")
        
        # Export combined data
        csv_filename = "all_products_combined.csv"
        combined_df.to_csv(csv_filename, index=False)
        print(f"\nExported combined data to {csv_filename}")
        
        # Show sample data
        print(f"\nSample products from all sources:")
        sample_df = combined_df.head(10)
        for _, row in sample_df.iterrows():
            print(f"  - {row['product_name']} (from {row['source_type']} - {row['source_table']})")
        
        if len(combined_df) > 10:
            print(f"  ... and {len(combined_df) - 10} more products")
    else:
        print("\nNo products found from any source")

if __name__ == "__main__":
    main()
