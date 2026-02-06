"""
List ALL products from ClickHouse (No filtering, no uniqueness)
Combined list from 'product_names' and 'products' tables.
"""
import pandas as pd
import requests
import uuid
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress InsecureRequestWarning
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

def fetch_ch(table):
    """Fetches raw names from a ClickHouse table via API"""
    superset_url = "http://64.227.129.135:8088"
    access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}

    payload = {
        "client_id": f"p_{uuid.uuid4().hex[:6]}", "database_id": 1, "json": True, "runAsync": False,
        "schema": "chipchip", "sql": f'SELECT name FROM "chipchip"."{table}"',
        "expand_data": True
    }
    
    try:
        res = requests.post(f"{superset_url}/api/v1/sqllab/execute/", headers=headers, json=payload, verify=False)
        if res.status_code == 200:
            data = res.json().get('data', [])
            return [row['name'] for row in data if row.get('name')]
        return []
    except Exception as e:
        print(f"Error fetching from {table}: {e}")
        return []

def list_clickhouse_products():
    """List ALL product names from ClickHouse tables"""
    print("\n🌐 FETCHING ALL PRODUCTS FROM CLICKHOUSE (Full List)...")
    
    # ClickHouse names come from two tables in the pipeline
    names_from_product_names = fetch_ch("product_names")
    names_from_products = fetch_ch("products")
    
    all_names = names_from_product_names + names_from_products
    all_names.sort()

    print(f"✅ Found {len(all_names)} total rows in ClickHouse (product_names + products).\n")
    
    print("=" * 60)
    print(f"{'#':<4} | {'PRODUCT NAME'}")
    print("-" * 60)
    
    for i, name in enumerate(all_names, 1):
        print(f"{i:<4} | {name}")
    
    print("=" * 60)
    print(f"📊 TOTAL: {len(all_names)} Products listed.")
    print("-" * 60)

if __name__ == "__main__":
    list_clickhouse_products()
