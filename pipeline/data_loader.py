# pipeline/data_loader.py

import pandas as pd
from sqlalchemy import text
import clickhouse_connect
import config.setting as settings

def load_all_product_data_from_clickhouse(engine) -> pd.DataFrame:
    """
    Fetches product names and IDs from the ClickHouse database.
    The table name is read from the settings file.
    """
    table_name = settings.CLICKHOUSE_TABLE_NAME
    print(f"--- Loading data from ClickHouse table: {table_name} ---")

    try:
        import requests
        import json
        
        # Superset API configuration
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"
        
        # SQL query to get product data
        sql_query = f'SELECT id AS raw_product_id, name AS raw_product_name, toString(created_at) AS created_at FROM "chipchip"."{table_name}"'
        
        # Prepare the request payload with unique client_id (shorter for database constraint)
        import uuid
        unique_client_id = f"p_{uuid.uuid4().hex[:6]}"
        
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
        
        # Headers for the API request
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'User-Agent': 'chipchip/bot'
        }
        
        # Make the API request
        response = requests.post(
            f"{superset_url}/api/v1/sqllab/execute/",
            headers=headers,
            json=payload,
            verify=False  # --insecure equivalent
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Extract data from the response
            if 'data' in result and result['data']:
                df = pd.DataFrame(result['data'], columns=['raw_product_id', 'raw_product_name', 'created_at'])
                
                if not df.empty:
                    # Ensure the ID column is treated as a string to match Supabase UUIDs.
                    df['raw_product_id'] = df['raw_product_id'].astype(str)
                    print(f"SUCCESS: Successfully loaded a total of {len(df)} product records from ClickHouse via Superset API.")
                    return df
                else:
                    print("SUCCESS: ClickHouse query executed successfully but returned no data.")
                    return pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'created_at'])
            else:
                print("SUCCESS: ClickHouse query executed successfully but returned no data.")
                return pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'created_at'])
        else:
            print(f"ERROR: Superset API request failed with status {response.status_code}: {response.text}")
            return pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'created_at'])
            
    except Exception as e:
        print(f"ERROR: Failed to load data from ClickHouse via Superset API. Error: {e}")
        # Return an empty DataFrame on failure to allow the pipeline to continue.
        return pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'created_at'])


def load_all_product_data_from_supabase(engine) -> pd.DataFrame:
    """
    Fetches product names and IDs from all specified tables in the Supabase database.
    The list of tables is read from the settings file.
    """
    supabase_tables = settings.SUPABASE_TABLES
    print(f"--- Loading data from {len(supabase_tables)} Supabase tables ---")

    # --- Dynamically build the UNION ALL query from the settings ---
    # This makes the code cleaner and easier to manage.
    union_queries = []
    for table in supabase_tables:
        # Each SELECT statement casts the UUID `id` to text for consistent data types.
        union_queries.append(
            f"SELECT id::text AS raw_product_id, product_name AS raw_product_name, created_at FROM {settings.SUPABASE_SCHEMA}.{table}"
        )

    # Join all the individual SELECT statements with 'UNION ALL'
    full_query = text(" \nUNION ALL\n ".join(union_queries))

    try:
        with engine.connect() as connection:
            df = pd.read_sql(full_query, connection)
            print(f"SUCCESS: Successfully loaded a total of {len(df)} product records from Supabase.")
            return df
    except Exception as e:
        print(f"ERROR: Failed to load data from Supabase. Error: {e}")
        raise