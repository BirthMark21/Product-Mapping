# main.py

import pandas as pd
import config.setting as settings
from utils.db_connector import get_db_engine
from pipeline.data_loader import load_all_product_data_from_clickhouse, load_all_product_data_from_supabase
from pipeline.standardization import create_parent_child_master_table
from pipeline.db_writer import write_to_db # Assuming you have this file

def run_unified_pipeline():
    """
    Executes a single, unified ETL pipeline that combines data from
    both ClickHouse and Supabase before standardization and loading.
    """
    print("--- Starting Unified ETL Pipeline for ALL Data Sources ---")

    try:
        # --- Step 1: Connect to ALL databases ---
        print("\nConnecting to all databases...")
        clickhouse_engine = get_db_engine(db_type='clickhouse')
        supabase_engine = get_db_engine(db_type='supabase')
        hub_engine = get_db_engine(db_type='hub')

        # --- Step 2: Extract data from BOTH sources ---
        print("\nExtracting data from ClickHouse...")
        clickhouse_df = load_all_product_data_from_clickhouse(clickhouse_engine)

        print("\nExtracting data from Supabase...")
        supabase_df = load_all_product_data_from_supabase(supabase_engine)

        # --- Step 3: Combine all raw data into a single DataFrame ---
        print("\nCombining data from all sources...")
        combined_raw_data_df = pd.concat([clickhouse_df, supabase_df], ignore_index=True)
        print(f" -> Total records from ClickHouse: {len(clickhouse_df)}")
        print(f" -> Total records from Supabase: {len(supabase_df)}")
        print(f" -> Total combined records: {len(combined_raw_data_df)}")

        print("\nStep 4: Preparing the combined raw data...")
        combined_raw_data_df.dropna(subset=['raw_product_id', 'raw_product_name'], inplace=True)
        print(f" -> Total valid records to be processed: {len(combined_raw_data_df)}")

        # --- Step 5: Apply the standardization logic ---
        # The create_parent_child_master_table function now operates on the complete, combined dataset.
        parent_child_master_df = create_parent_child_master_table(combined_raw_data_df)

        print("\n--- Sample of the Final UNIFIED Canonical Product Table ---")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(parent_child_master_df.head(10))

        # --- Step 6: Load final unified table into the destination database ---
        print("\nLoading final table into the destination database...")
        write_to_db(
            df=parent_child_master_df,
            engine=hub_engine,
            table_name=settings.HUB_CANONICAL_TABLE_NAME,
            schema=settings.HUB_SCHEMA,
            if_exists='replace' # This will drop the table and recreate it each time.
        )

        print("\n" + "="*80)
        print("--- UNIFIED ETL PIPELINE FINISHED SUCCESSFULLY ---")
        print("Data from both ClickHouse and Supabase has been processed together.")
        print(f"The '{settings.HUB_CANONICAL_TABLE_NAME}' table in schema '{settings.HUB_SCHEMA}' has been created/replaced.")
        print("="*80)

    except Exception as e:
        print(f"\n--- PIPELINE FAILED ---")
        print(f"An error occurred: {e}")
        # In a production environment, you would log this error.
        raise

if __name__ == "__main__":
    run_unified_pipeline()