#!/usr/bin/env python3
import os
import logging
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DatabaseError

load_dotenv()
logger = logging.getLogger(__name__)


class ResilientDBConnector:
    
    @staticmethod
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((OperationalError, DatabaseError, ConnectionError)),
        reraise=True
    )
    def get_engine_with_retry(db_type: str):
        DB_CONFIGS = {
            'supabase': {'prefix': 'PG', 'log_name': 'Supply Chain Supabase PostgreSQL'},
            'b2b': {'prefix': 'SUPABASE_PG', 'log_name': 'B2B Supabase PostgreSQL'},
            'prod_postgres': {'prefix': 'PROD_PG', 'log_name': 'Production PostgreSQL'},
            'hub': {'prefix': 'HUB_PG', 'log_name': 'Local Hub PostgreSQL'},
            'staging': {'prefix': 'STAGING_PG', 'log_name': 'Staging PostgreSQL'}
        }
        
        if db_type not in DB_CONFIGS:
            raise ValueError(f"Invalid db_type '{db_type}'. Choose from {list(DB_CONFIGS.keys())}")
        
        config = DB_CONFIGS[db_type]
        prefix = config['prefix']
        log_name = config['log_name']
        
        logger.info(f"Connecting to {log_name}...")
        
        try:
            # Get credentials
            host = os.getenv(f"{prefix}_HOST")
            port = os.getenv(f"{prefix}_PORT")
            db_name = os.getenv(f"{prefix}_DB_NAME")
            user = os.getenv(f"{prefix}_USER")
            password = os.getenv(f"{prefix}_PASSWORD")
            
            if not all([host, port, db_name, user, password]):
                raise ValueError(f"Missing environment variables for {log_name}")
            
            # Create connection string
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
            
            # Create engine with connection pooling
            engine = create_engine(
                conn_str,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600,   # Recycle connections after 1 hour
                echo=False
            )
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"✅ Connected to {log_name}")
            return engine
            
        except Exception as e:
            logger.warning(f"⚠️  Connection attempt to {log_name} failed: {e}")
            raise
    
    @staticmethod
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((OperationalError, DatabaseError)),
        reraise=True
    )
    def execute_with_retry(engine, query, params=None):
        try:
            with engine.connect() as conn:
                if params:
                    result = conn.execute(query, params)
                else:
                    result = conn.execute(query)
                return result
        except Exception as e:
            logger.warning(f"⚠️  Query execution failed, retrying: {e}")
            raise
