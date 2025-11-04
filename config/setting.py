# config/settings.py

import os
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Project Root Path ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --- SOURCE: Supabase / PostgreSQL Credentials ---
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB_NAME = os.getenv("PG_DB_NAME")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


# --- SOURCE: ClickHouse Credentials ---
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT_STR = os.getenv('CLICKHOUSE_PORT_STR')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE')
CLICKHOUSE_SECURE_STR = os.getenv('CLICKHOUSE_SECURE_STR', 'True')
CLICKHOUSE_VERIFY_STR = os.getenv('CLICKHOUSE_VERIFY_STR', 'False')


# --- DESTINATION: Local PostgreSQL Hub Credentials ---
HUB_PG_HOST = os.getenv("HUB_PG_HOST")
HUB_PG_PORT = os.getenv("HUB_PG_PORT")
HUB_PG_USER = os.getenv("HUB_PG_USER")
HUB_PG_PASSWORD = os.getenv("HUB_PG_PASSWORD")
HUB_PG_DB_NAME = os.getenv("HUB_PG_DB_NAME")


# --- DESTINATION: Remote PostgreSQL Database Credentials ---
REMOTE_DB_HOST = os.getenv("REMOTE_DB_HOST")
REMOTE_DB_PORT = os.getenv("REMOTE_DB_PORT")
REMOTE_DB_USER = os.getenv("REMOTE_DB_USER")
REMOTE_DB_PASSWORD = os.getenv("REMOTE_DB_PASSWORD")
REMOTE_DB_NAME = os.getenv("REMOTE_DB_NAME")


# --- Pipeline Source & Destination Configuration ---

# Source Tables
SUPABASE_SCHEMA = 'public'
SUPABASE_TABLES = [
    "farm_prices",
    "supermarket_prices",
    "distribution_center_prices",
    "local_shop_prices",
    "ecommerce_prices",
    "sunday_market_prices"
]
CLICKHOUSE_TABLE_NAME = 'product_names'

# Destination Table Configuration
HUB_SCHEMA = 'public'
HUB_CANONICAL_TABLE_NAME = 'canonical_products_master'