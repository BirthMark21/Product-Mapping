# utils/db_connector.py

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Although settings.py also loads this, it's good practice for this
# self-contained utility to ensure environment variables are loaded.
load_dotenv()

def get_db_engine(db_type: str):
    """
    Creates and configures a SQLAlchemy engine for the specified database.
    It reads credentials from environment variables (loaded from .env file).

    Args:
        db_type (str): The type of database to connect to.
                       Valid options are 'supabase', 'clickhouse', or 'hub'.
    """
    # This dictionary maps the db_type to the correct environment variable prefixes.
    # This keeps the code clean and avoids repetition.
    DB_CONFIGS = {
        'supabase':   {'prefix': 'PG',         'log_name': 'SOURCE (Supabase/PostgreSQL)'},
        'clickhouse': {'prefix': 'CLICKHOUSE', 'log_name': 'SOURCE (ClickHouse)'},
        'hub':        {'prefix': 'HUB_PG',     'log_name': 'DESTINATION (PostgreSQL Hub)'}
    }

    if db_type not in DB_CONFIGS:
        raise ValueError(f"Invalid db_type '{db_type}'. Choose from {list(DB_CONFIGS.keys())}.")

    config = DB_CONFIGS[db_type]
    prefix = config['prefix']
    log_name = config['log_name']

    print(f"Configuring connection to {log_name} database...")

    try:
        engine = None
        # --- Logic for PostgreSQL connections (Supabase and Hub) ---
        if db_type in ('supabase', 'hub'):
            host = os.getenv(f"{prefix}_HOST")
            port = os.getenv(f"{prefix}_PORT")
            db_name = os.getenv(f"{prefix}_DB_NAME")
            user = os.getenv(f"{prefix}_USER")
            password = os.getenv(f"{prefix}_PASSWORD")

            if not all([host, port, db_name, user, password]):
                raise ValueError(f"Missing one or more PostgreSQL environment variables for '{log_name}'.")

            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
            engine = create_engine(conn_str)

        # --- Logic for ClickHouse connection ---
        elif db_type == 'clickhouse':
            # For ClickHouse, we return None since we handle it directly in data_loader
            # This is a workaround since we use clickhouse-connect directly
            print(f"SUCCESS: ClickHouse connection will be handled directly in data loader.")
            return None

        print(f"SUCCESS: Engine for '{log_name}' configured successfully.")
        return engine

    except Exception as e:
        print(f"ERROR: Error creating database engine for '{log_name}': {e}")
        raise