
import pandas as pd
import os
import sys
import requests
import json
import uuid

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.db_connector import get_db_engine
import config.setting as settings

def list_all_remote_entries():
    """
    Lists EVERY product entry from remote sources without deduplication.
    """
    try:
        all_names = []

        # 1. Fetch from Remote Supabase (products table) - COMMENTED OUT
        # print("\n🌐 FETCHING ALL ROWS FROM REMOTE SUPABASE (products)...")
        # supabase_engine = get_db_engine('supabase')
        # with supabase_engine.connect() as conn:
        #     df_sup = pd.read_sql("SELECT name FROM products", conn)
        #     sup_list = df_sup['name'].tolist()
        #     all_names.extend(sup_list)
        # print(f"✅ Found {len(sup_list)} rows in Supabase.")
        sup_list = []

        # 2. Fetch from Remote ClickHouse (via Superset API)
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Table A: product_names
        print(f"\n🌐 FETCHING ALL ROWS FROM REMOTE CLICKHOUSE (product_names)...")
        payload_names = {
            "client_id": f"p_{uuid.uuid4().hex[:6]}",
            "database_id": 1,
            "json": True,
            "runAsync": False,
            "schema": "chipchip",
            "sql": 'SELECT name FROM "chipchip"."product_names"',
            "expand_data": True
        }
        res_names = requests.post(f"{superset_url}/api/v1/sqllab/execute/", headers=headers, json=payload_names, verify=False)
        ch_names_count = 0
        if res_names.status_code == 200:
            data = res_names.json().get('data', [])
            names = [row['name'] for row in data if 'name' in row]
            all_names.extend(names)
            ch_names_count = len(names)
        print(f"✅ Found {ch_names_count} rows in ClickHouse (product_names).")

        # Table B: products
        print(f"🌐 FETCHING ALL ROWS FROM REMOTE CLICKHOUSE (products)...")
        payload_prod = {
            "client_id": f"p_{uuid.uuid4().hex[:6]}",
            "database_id": 1,
            "json": True,
            "runAsync": False,
            "schema": "chipchip",
            "sql": 'SELECT name FROM "chipchip"."products"',
            "expand_data": True
        }
        res_prod = requests.post(f"{superset_url}/api/v1/sqllab/execute/", headers=headers, json=payload_prod, verify=False)
        ch_prod_count = 0
        if res_prod.status_code == 200:
            data = res_prod.json().get('data', [])
            names = [row['name'] for row in data if 'name' in row]
            all_names.extend(names)
            ch_prod_count = len(names)
        print(f"✅ Found {ch_prod_count} rows in ClickHouse (products).")

        # 3. Output EVERYTHING
        print("\n" + "="*60)
        print(f"CLICKHOUSE TABLES - ALL ENTRIES (NON-UNIQUE)")
        print("="*60)
        
        # Sort for easier reading
        all_names.sort(key=lambda x: str(x).lower())
        
        for name in all_names:
            print(name)

        print("-" * 60)
        print(f"📊 SUMMARY:")
        print(f"   - ClickHouse (product_names): {ch_names_count}")
        print(f"   - ClickHouse (products): {ch_prod_count}")
        print(f"✅ TOTAL ROWS LISTED: {len(all_names)}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    list_all_remote_entries()
