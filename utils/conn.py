
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

 # ClickHouse Connection Configuration
def get_clickhouse_connection():
    """
    Creates and configures a SQLAlchemy engine for ClickHouse database.
    """
    load_dotenv()
    
    # ClickHouse specific configuration
    prefix = 'CLICKHOUSE'
    
    # Required environment variables for ClickHouse
    required_vars = [f"{prefix}_HOST", f"{prefix}_PORT_STR", f"{prefix}_USER", f"{prefix}_DATABASE"]
    if any(not os.getenv(var) for var in required_vars):
        raise ValueError(f"Missing ClickHouse environment variables.")
    
    connect_args = { 'connect_timeout': 200 }

    # ClickHouse connection string
    conn_str = (
        f"clickhouse+native://{os.getenv(f'{prefix}_USER')}:{os.getenv(f'{prefix}_PASSWORD', '')}"
        f"@{os.getenv(f'{prefix}_HOST')}:{os.getenv(f'{prefix}_PORT_STR')}/{os.getenv(f'{prefix}_DATABASE')}"
    )
    
    # Security settings
    secure = os.getenv(f'{prefix}_SECURE_STR', 'false').lower() in ('true', '1', 't')
    verify = os.getenv(f'{prefix}_VERIFY_STR', 'true').lower() in ('true', '1', 't')
    conn_str += f"?secure={'true' if secure else 'false'}&verify={'true' if verify else 'false'}"
    
    engine = create_engine(conn_str, connect_args=connect_args)
    return engine