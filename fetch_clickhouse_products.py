#!/usr/bin/env python3
"""
Script to fetch ALL UNIQUE product names from the 'product_names' table in ClickHouse
and export them to a CSV file containing only the names.
"""

import pandas as pd
import requests
import uuid
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
# Suppress InsecureRequestWarning for environments with self-signed certificates
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

def fetch_all_unique_product_names():
    """Fetches all unique product names from ClickHouse and exports only the names to CSV."""
    
    print("Fetching all unique product names from ClickHouse...")
    
    try:
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"

        # OPTIMIZED QUERY: Select only the distinct names you need.
        # This is more efficient and also filters out null or empty names at the database level.
        sql_query = '''
        SELECT DISTINCT
            name AS product_name
        FROM "chipchip"."product_names"
        WHERE name IS NOT NULL AND name != '' AND name != '0'
        ORDER BY product_name ASC
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
                # SIMPLIFIED: Let pandas create the DataFrame with the single column returned.
                df = pd.DataFrame(result['data'])
                
                print(f"Successfully fetched {len(df)} unique product names from ClickHouse")
                
                # Show sample data
                print("\nSample of unique product names:")
                sample_df = df.head(10)
                for _, row in sample_df.iterrows():
                    print(f"  - {row['product_name']}")
                
                if len(df) > 10:
                    print(f"  ... and {len(df) - 10} more products")
                
                # Export ONLY the names to CSV
                csv_filename = "clickhouse_product_names_only.csv"
                df.to_csv(csv_filename, index=False)
                print(f"\n📁 Exported all unique names to {csv_filename}")
                
                # Show statistics
                print(f"\nStatistics:")
                print(f"  Total unique product names: {len(df)}")
                
                return df
            else:
                print("No data returned from ClickHouse")
                return pd.DataFrame()
        else:
            print(f"Error fetching from ClickHouse: {response.status_code} - {response.text}")
            return pd.DataFrame()

    except Exception as e:
        print(f"An exception occurred: {e}")
        return pd.DataFrame()

def main():
    """Main function"""
    print("Unique Product Name Exporter")
    print("=" * 50)
    
    df = fetch_all_unique_product_names()
    
    if not df.empty:
        print(f"\nSuccessfully processed {len(df)} unique product names.")
    else:
        print("\nNo product names were found or an error occurred.")

if __name__ == "__main__":
    main()