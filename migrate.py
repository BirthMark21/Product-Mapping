import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# --- Configuration ---
# Load environment variables from the .env file
load_dotenv()

# Define the table names
SOURCE_TABLE_NAME = 'canonical_products_master'
DESTINATION_TABLE_NAME = 'parent_products'

def migrate_table():
    """
    Connects to source and destination databases, reads a table from the source,
    and writes it to a new table in the destination, failing if it already exists.
    """
    print("--- Starting Table Migration ---")
    print(f"Source: local table '{SOURCE_TABLE_NAME}'")
    print(f"Destination: remote table '{DESTINATION_TABLE_NAME}'")

    try:
        # --- Step 1: Establish connections using credentials from .env ---

        # --- Load SOURCE (Local) Credentials ---
        print("\nConnecting to source (local) database...")
        local_user = os.getenv("HUB_PG_USER")
        local_pass = os.getenv("HUB_PG_PASSWORD")
        local_host = os.getenv("HUB_PG_HOST")
        local_port = os.getenv("HUB_PG_PORT")
        local_db = os.getenv("HUB_PG_DB_NAME")
        if not all([local_user, local_pass, local_host, local_port, local_db]):
            raise ValueError("Missing one or more HUB_PG_* variables in the .env file.")
        local_conn_str = f"postgresql://{local_user}:{local_pass}@{local_host}:{local_port}/{local_db}"
        source_engine = create_engine(local_conn_str)


        # --- Load DESTINATION (Remote) Credentials ---
        print("Connecting to destination (remote) database...")
        remote_user = os.getenv("REMOTE_DB_USER")
        remote_pass = os.getenv("REMOTE_DB_PASSWORD")
        remote_host = os.getenv("REMOTE_DB_HOST")
        remote_port = os.getenv("REMOTE_DB_PORT")
        remote_db = os.getenv("REMOTE_DB_NAME")
        if not all([remote_user, remote_pass, remote_host, remote_port, remote_db]):
            raise ValueError("Missing one or more REMOTE_DB_* variables in the .env file.")
        remote_conn_str = f"postgresql://{remote_user}:{remote_pass}@{remote_host}:{remote_port}/{remote_db}"
        
        # Added a 30-second connection timeout for robustness
        destination_engine = create_engine(
            remote_conn_str,
            connect_args={'connect_timeout': 30}
        )


        # --- Step 2: Read data from the source table ---
        print(f"\nReading data from '{SOURCE_TABLE_NAME}'...")
        with source_engine.connect() as connection:
            source_df = pd.read_sql_table(SOURCE_TABLE_NAME, connection)

        if source_df.empty:
            print(f"Warning: The source table '{SOURCE_TABLE_NAME}' is empty. No data will be migrated.")
            return

        print(f" -> Successfully loaded {len(source_df)} records.")


        # --- Step 3: Write data to the destination table ---
        print(f"Writing {len(source_df)} records to new table '{DESTINATION_TABLE_NAME}'...")
        source_df.to_sql(
            name=DESTINATION_TABLE_NAME,
            con=destination_engine,
            index=False,
            if_exists='fail' # Safety check to prevent overwriting data
        )

        print("\n--- Migration Successful! ---")
        print(f"The table '{DESTINATION_TABLE_NAME}' was created in the '{remote_db}' database.")

    except Exception as e:
        print("\n--- AN ERROR OCCURRED ---")
        print(f"Error details: {e}")
        print("\nThe operation was stopped safely. No changes were made to the destination database.")

if __name__ == "__main__":
    migrate_table()