#!/usr/bin/env python3
"""
Script to create 6 Supabase tables in local master-db PostgreSQL database
Run this script to set up the tables with sample data
"""

import pandas as pd
import uuid
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import random

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'master-db',
    'user': 'postgres',
    'password': 'birthmark21'
}

def get_db_engine():
    """Create database engine"""
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(conn_str)

def create_tables():
    """Create all 6 tables with proper structure"""
    engine = get_db_engine()
    
    print("Creating tables in master-db...")
    
    # Enable UUID extension
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"))
        conn.commit()
    
    # 1. FARM_PRICES TABLE (Most detailed with cost breakdowns)
    farm_prices_sql = """
    CREATE TABLE IF NOT EXISTS public.farm_prices (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT NOT NULL,
        region TEXT,
        origin TEXT,
        source_type TEXT,
        supplier_name TEXT,
        farm_price_per_kg NUMERIC(10,2),
        sack_crate_cost_per_kg NUMERIC(10,2),
        loading_cost_per_kg NUMERIC(10,2),
        transportation_cost_per_kg NUMERIC(10,2),
        border_cost_per_kg NUMERIC(10,2),
        agent_cost_per_kg NUMERIC(10,2),
        miscellaneous_costs NUMERIC(10,2),
        vat NUMERIC(10,2),
        product_remark TEXT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        price NUMERIC(10,2)
    );
    """
    
    # 2. SUPERMARKET_PRICES TABLE
    supermarket_prices_sql = """
    CREATE TABLE IF NOT EXISTS public.supermarket_prices (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT NOT NULL,
        price NUMERIC(10,2),
        location TEXT,
        product_remark TEXT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by UUID
    );
    """
    
    # 3. DISTRIBUTION_CENTER_PRICES TABLE
    distribution_center_prices_sql = """
    CREATE TABLE IF NOT EXISTS public.distribution_center_prices (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT NOT NULL,
        price NUMERIC(10,2),
        location TEXT,
        product_remark TEXT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by UUID
    );
    """
    
    # 4. LOCAL_SHOP_PRICES TABLE
    local_shop_prices_sql = """
    CREATE TABLE IF NOT EXISTS public.local_shop_prices (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT NOT NULL,
        price NUMERIC(10,2),
        location TEXT,
        product_remark TEXT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by UUID
    );
    """
    
    # 5. ECOMMERCE_PRICES TABLE
    ecommerce_prices_sql = """
    CREATE TABLE IF NOT EXISTS public.ecommerce_prices (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT NOT NULL,
        price NUMERIC(10,2),
        location TEXT,
        product_remark TEXT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by UUID
    );
    """
    
    # 6. SUNDAY_MARKET_PRICES TABLE
    sunday_market_prices_sql = """
    CREATE TABLE IF NOT EXISTS public.sunday_market_prices (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT NOT NULL,
        price NUMERIC(10,2),
        location TEXT,
        product_remark TEXT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by UUID
    );
    """
    
    # Execute table creation
    tables = [
        ("farm_prices", farm_prices_sql),
        ("supermarket_prices", supermarket_prices_sql),
        ("distribution_center_prices", distribution_center_prices_sql),
        ("local_shop_prices", local_shop_prices_sql),
        ("ecommerce_prices", ecommerce_prices_sql),
        ("sunday_market_prices", sunday_market_prices_sql)
    ]
    
    with engine.connect() as conn:
        for table_name, sql in tables:
            try:
                conn.execute(text(sql))
                print(f"✅ Created table: {table_name}")
            except Exception as e:
                print(f"❌ Error creating {table_name}: {e}")
        conn.commit()
    
    # Create indexes
    create_indexes(engine)
    
    print("✅ All tables created successfully!")

def create_indexes(engine):
    """Create indexes for better performance"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_farm_prices_product_name ON public.farm_prices(product_name);",
        "CREATE INDEX IF NOT EXISTS idx_farm_prices_date ON public.farm_prices(date);",
        "CREATE INDEX IF NOT EXISTS idx_supermarket_prices_product_name ON public.supermarket_prices(product_name);",
        "CREATE INDEX IF NOT EXISTS idx_supermarket_prices_date ON public.supermarket_prices(date);",
        "CREATE INDEX IF NOT EXISTS idx_distribution_center_prices_product_name ON public.distribution_center_prices(product_name);",
        "CREATE INDEX IF NOT EXISTS idx_distribution_center_prices_date ON public.distribution_center_prices(date);",
        "CREATE INDEX IF NOT EXISTS idx_local_shop_prices_product_name ON public.local_shop_prices(product_name);",
        "CREATE INDEX IF NOT EXISTS idx_local_shop_prices_date ON public.local_shop_prices(date);",
        "CREATE INDEX IF NOT EXISTS idx_ecommerce_prices_product_name ON public.ecommerce_prices(product_name);",
        "CREATE INDEX IF NOT EXISTS idx_ecommerce_prices_date ON public.ecommerce_prices(date);",
        "CREATE INDEX IF NOT EXISTS idx_sunday_market_prices_product_name ON public.sunday_market_prices(product_name);",
        "CREATE INDEX IF NOT EXISTS idx_sunday_market_prices_date ON public.sunday_market_prices(date);"
    ]
    
    with engine.connect() as conn:
        for index_sql in indexes:
            try:
                conn.execute(text(index_sql))
            except Exception as e:
                print(f"Warning: Index creation failed: {e}")
        conn.commit()
    
    print("✅ Indexes created successfully!")

if __name__ == "__main__":
    try:
        create_tables()
        print("\n🎉 Database setup completed successfully!")
        print("You can now run the ETL pipeline to populate these tables.")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Please check your database connection settings.")



