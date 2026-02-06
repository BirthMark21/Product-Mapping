# Database Migrations (Python Scripts)

This folder contains Python migration scripts to set up the production-ready product standardization schema.

## Migration Scripts

### 1. `001_create_canonical_master.py`
**Creates:** `canonical_products_master` table  
**Runs on:** All 3 databases (Supabase, Production PostgreSQL, Local Hub)

```bash
python migrations/001_create_canonical_master.py
```

### 2. `002_add_parent_columns_to_products.py`
**Adds:** `parent_product_id` and `parent_product_name` columns to `products` table  
**Runs on:** Supabase PostgreSQL AND Production PostgreSQL

```bash
python migrations/002_add_parent_columns_to_products.py
```

### 3. `003_add_parent_columns_to_product_names.py`
**Adds:** `parent_product_id` and `parent_product_name` columns to `product_names` table  
**Runs on:** Production PostgreSQL ONLY

```bash
python migrations/003_add_parent_columns_to_product_names.py
```

## Run All Migrations

To run all migrations in the correct order:

```bash
python migrations/run_all_migrations.py
```

## Prerequisites

1. Update your `.env` file with Production PostgreSQL credentials:

```bash
PROD_PG_HOST=64.226.73.158
PROD_PG_PORT=4554
PROD_PG_DB_NAME=chipchip
PROD_PG_USER=postgres
PROD_PG_PASSWORD=your_password_here
```

2. Ensure all database connections are working:

```bash
python -c "from utils.db_connector import get_db_engine; get_db_engine('supabase')"
python -c "from utils.db_connector import get_db_engine; get_db_engine('prod_postgres')"
python -c "from utils.db_connector import get_db_engine; get_db_engine('hub')"
```

## What Gets Created

### All Databases (Supabase, Production PostgreSQL, Local Hub):
- ✅ `canonical_products_master` table
  - `parent_product_id` (UUID, Primary Key)
  - `parent_name` (VARCHAR, Unique)
  - `created_at` (TIMESTAMP)
  - `updated_at` (TIMESTAMP, auto-updated)

### Supabase + Production PostgreSQL:
- ✅ `products.parent_product_id` (UUID, Foreign Key)
- ✅ `products.parent_product_name` (VARCHAR)
- ✅ Indexes on parent columns
- ✅ Foreign key constraint to canonical_products_master

### Production PostgreSQL Only:
- ✅ `product_names.parent_product_id` (UUID, Foreign Key)
- ✅ `product_names.parent_product_name` (VARCHAR)
- ✅ Indexes on parent columns
- ✅ Foreign key constraint to canonical_products_master

## After Migrations

Once migrations complete:

1. **PeerDB Auto-Sync**: Production PostgreSQL changes will automatically sync to ClickHouse
2. **Run ETL Pipeline**: Execute the standardization pipeline to populate parent IDs
3. **Verify**: Check that all tables have the new columns

## Verification

Check if migrations were successful:

```python
from utils.db_connector import get_db_engine
from sqlalchemy import text

# Check Supabase
engine = get_db_engine('supabase')
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM canonical_products_master LIMIT 1"))
    print("Supabase:", result.fetchone())

# Check Production PostgreSQL
engine = get_db_engine('prod_postgres')
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM canonical_products_master LIMIT 1"))
    print("Production PostgreSQL:", result.fetchone())
```

## Rollback

If you need to undo the migrations, run these commands in each database:

```python
from utils.db_connector import get_db_engine
from sqlalchemy import text

engine = get_db_engine('supabase')  # or 'prod_postgres' or 'hub'

with engine.begin() as conn:
    # Remove columns from products
    conn.execute(text("ALTER TABLE products DROP COLUMN IF EXISTS parent_product_id CASCADE"))
    conn.execute(text("ALTER TABLE products DROP COLUMN IF EXISTS parent_product_name"))
    
    # Remove columns from product_names (Production PostgreSQL only)
    conn.execute(text("ALTER TABLE product_names DROP COLUMN IF EXISTS parent_product_id CASCADE"))
    conn.execute(text("ALTER TABLE product_names DROP COLUMN IF EXISTS parent_product_name"))
    
    # Drop canonical_products_master table
    conn.execute(text("DROP TABLE IF EXISTS canonical_products_master CASCADE"))
    conn.execute(text("DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE"))
```
