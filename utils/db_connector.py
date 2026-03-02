import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_db_engine(db_type: str):
    DB_CONFIGS = {
        'supabase':   {'prefix': 'PG',         'log_name': 'SOURCE (Supply Chain Supabase/PostgreSQL)'},
        'b2b':        {'prefix': 'SUPABASE_PG', 'log_name': 'SOURCE (B2B Supabase/PostgreSQL)'},
        'clickhouse': {'prefix': 'CLICKHOUSE', 'log_name': 'SOURCE (ClickHouse)'},
        'hub':        {'prefix': 'HUB_PG',     'log_name': 'DESTINATION (PostgreSQL Hub)'},
        'staging':    {'prefix': 'STAGING_PG', 'log_name': 'SOURCE (Staging PostgreSQL)'}
    }

    if db_type not in DB_CONFIGS:
        raise ValueError(f"Invalid db_type '{db_type}'. Choose from {list(DB_CONFIGS.keys())}.")

    config = DB_CONFIGS[db_type]
    prefix = config['prefix']
    log_name = config['log_name']

    print(f"Configuring connection to {log_name} database...")

    try:
        engine = None
        if db_type in ('supabase', 'b2b', 'hub', 'staging'):
            host = os.getenv(f"{prefix}_HOST")
            port = os.getenv(f"{prefix}_PORT")
            db_name = os.getenv(f"{prefix}_DB_NAME")
            user = os.getenv(f"{prefix}_USER")
            password = os.getenv(f"{prefix}_PASSWORD")

            if not all([host, port, db_name, user, password]):
                raise ValueError(f"Missing PostgreSQL environment variables for '{log_name}'.")

            encoded_password = quote_plus(password)
            conn_str = f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}"
            engine = create_engine(conn_str)

        elif db_type == 'clickhouse':
            print(f"SUCCESS: ClickHouse connection will be handled directly in data loader.")
            return None

        print(f"SUCCESS: Engine for '{log_name}' configured successfully.")
        return engine

    except Exception as e:
        print(f"ERROR: Error creating database engine for '{log_name}': {e}")
        raise