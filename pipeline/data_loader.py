# pipeline/data_loader.py

import pandas as pd
from sqlalchemy import text
import clickhouse_connect
import os
import config.setting as settings

def load_product_data_from_staging(engine) -> pd.DataFrame:
    """
    Fetches product data from Staging PostgreSQL (replacing ClickHouse/Superset).
    Queries both 'products' and 'product_names' tables.
    """
    print(f"--- Loading data from Staging PostgreSQL ---")
    
    try:
        # Query 1: Fetch from 'products' table
        query_products = """
            SELECT id::text AS raw_product_id, name AS raw_product_name, created_at, 'products' as source_table
            FROM products
        """
        
        # Query 2: Fetch from 'product_names' table
        query_names = """
            SELECT id::text AS raw_product_id, name AS raw_product_name, created_at, 'product_names' as source_table
            FROM product_names
        """
        
        # Combine queries with UNION ALL for efficiency
        full_query = f"{query_products} UNION ALL {query_names}"
        
        with engine.connect() as conn:
            df = pd.read_sql(text(full_query), conn)
            
            if not df.empty:
                print(f"SUCCESS: Loaded {len(df)} records from Staging DB (products + product_names).")
                return df
            else:
                print("SUCCESS: Staging DB query returned no data.")
                return pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'created_at'])

    except Exception as e:
        print(f"ERROR: Failed to load data from Staging DB. Error: {e}")
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