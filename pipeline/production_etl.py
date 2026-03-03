#!/usr/bin/env python3
import sys
import os
import io
import traceback
import pandas as pd
import re
import uuid
import logging
from sqlalchemy import text
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_db_engine
from utils.resilient_db import ResilientDBConnector
from utils.data_validator import ProductDataValidator
from utils.fuzzy_matcher import FuzzyProductMatcher
from utils.transaction_manager import DistributedTransactionManager
from pipeline.standardization import PARENT_CHILD_MAPPING, CHILD_TO_PARENT_MAP, _generate_stable_uuid

os.makedirs('logs', exist_ok=True)

if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ProductionETLPipeline:
    
    SOURCE_TABLES = {
        'distribution_center_prices': os.getenv('UPDATE_DISTRIBUTION_CENTER', 'false').lower() == 'true',
        'ecommerce_prices': os.getenv('UPDATE_ECOMMERCE', 'false').lower() == 'true',
        'farm_prices': os.getenv('UPDATE_FARM_PRICES', 'false').lower() == 'true',
        'local_shop_prices': os.getenv('UPDATE_LOCAL_SHOP', 'false').lower() == 'true',
        'sunday_market_prices': os.getenv('UPDATE_SUNDAY_MARKET', 'false').lower() == 'true',
        'supermarket_prices': os.getenv('UPDATE_SUPERMARKET', 'false').lower() == 'true',
    }
    
    def __init__(self, enable_fuzzy_matching: bool = True, fuzzy_dry_run: bool = True):
        self.supabase_engine = None
        self.b2b_engine = None
        self.staging_engine = None
        self.hub_engine = None
        
        self.data_validator = ProductDataValidator()
        self.fuzzy_matcher = None
        self.enable_fuzzy_matching = enable_fuzzy_matching
        self.fuzzy_dry_run = fuzzy_dry_run
        
        if self.enable_fuzzy_matching:
            self.fuzzy_matcher = FuzzyProductMatcher(
                PARENT_CHILD_MAPPING,
                CHILD_TO_PARENT_MAP,
                threshold=85,
                dry_run=fuzzy_dry_run
            )
        
        self.stats = {
            'start_time': datetime.now(),
            'supabase_products': 0,
            'b2b_products': 0,
            'staging_products': 0,
            'staging_product_names': 0,
            'total_products': 0,
            'parent_products': 0,
            'mapped_products': 0,
            'unmapped_products': 0,
            'validation_removed': 0,
            'fuzzy_matched': 0
        }
    
    
    def connect_to_databases(self):
        logger.info("=" * 80)
        logger.info("CONNECTING TO DATABASES (with retry logic)")
        logger.info("=" * 80)
        
        try:
            self.supabase_engine = ResilientDBConnector.get_engine_with_retry('supabase')
        except Exception as e:
            logger.error(f"Failed to connect to Supply Chain Supabase after retries: {e}")
            raise
        
        try:
            self.b2b_engine = ResilientDBConnector.get_engine_with_retry('b2b')
        except Exception as e:
            logger.error(f"Failed to connect to B2B Supabase after retries: {e}")
            raise
        
        try:
            self.staging_engine = ResilientDBConnector.get_engine_with_retry('staging')
        except Exception as e:
            logger.error(f"Failed to connect to Staging PostgreSQL after retries: {e}")
            raise
        
        try:
            self.hub_engine = ResilientDBConnector.get_engine_with_retry('hub')
        except Exception as e:
            logger.error(f"Failed to connect to Local Hub after retries: {e}")
            raise
    
    def extract_from_supabase(self) -> pd.DataFrame:
        logger.info("\nEXTRACTING FROM SUPABASE POSTGRESQL")
        
        try:
            with self.supabase_engine.connect() as conn:
                df = pd.read_sql("""
                    SELECT *
                    FROM public.products
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                      AND TRIM(name) != '0'
                """, conn)
                
                df['id'] = df['id'].astype(str)
            
            self.stats['supabase_products'] = len(df)
            logger.info(f"   Extracted {len(df)} products from Supply Chain Supabase with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"   Failed to extract from Supply Chain Supabase: {e}")
            raise
    
    def extract_from_b2b(self) -> pd.DataFrame:
        logger.info("\nEXTRACTING FROM B2B SUPABASE POSTGRESQL")
        
        try:
            with self.b2b_engine.connect() as conn:
                df = pd.read_sql("""
                    SELECT *
                    FROM public.products
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                      AND TRIM(name) != '0'
                """, conn)
                
                df['id'] = df['id'].astype(str)
            
            self.stats['b2b_products'] = len(df)
            logger.info(f"   Extracted {len(df)} products from B2B Supabase with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"   Failed to extract from B2B Supabase: {e}")
            raise
    
    def extract_from_staging(self):
        if not self.staging_engine:
            logger.warning("Staging engine not initialized. Skipping Staging extraction.")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info("\nEXTRACTING FROM STAGING POSTGRESQL (chipchip)")
        
        try:
            with self.staging_engine.connect() as conn:
                logger.info("   → Fetching from 'product_names' table...")
                df_names = pd.read_sql(text("""
                    SELECT *
                    FROM public.product_names
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                """), conn)
                
                df_names['id'] = df_names['id'].astype(str)
                
                logger.info("   → Fetching from 'products' table...")
                df_products = pd.read_sql(text("""
                    SELECT *
                    FROM public.products
                    WHERE deleted_at IS NULL
                """), conn)
                
                df_products['id'] = df_products['id'].astype(str)
                
                self.stats['staging_products'] = len(df_products)
                self.stats['staging_product_names'] = len(df_names)
                
                logger.info(f"   Extracted {len(df_names)} names ({len(df_names.columns)} cols) and {len(df_products)} products ({len(df_products.columns)} cols) from Staging")
                return df_products, df_names
                
        except Exception as e:
            logger.error(f"   Failed to extract from Staging PostgreSQL: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def transform_and_standardize(self, df_supabase, df_b2b, df_staging_products, df_staging_names) -> pd.DataFrame:
        logger.info("\nCREATING PARENT PRODUCTS TABLE")
        
        all_names = []
        
        # Collect all product names from all sources
        if not df_supabase.empty and 'name' in df_supabase.columns:
            all_names.extend(df_supabase['name'].dropna().tolist())
        
        if not df_b2b.empty and 'name' in df_b2b.columns:
            all_names.extend(df_b2b['name'].dropna().tolist())
        
        if not df_staging_names.empty and 'name' in df_staging_names.columns:
            all_names.extend(df_staging_names['name'].dropna().tolist())
        
        logger.info(f"   Collected {len(all_names)} product names from all sources")
        
        # Filter blacklist
        blacklist = ["Product name", "Item name", "product name", "item name", "White Onion", "white onion", "White Onion A", "White Onion B", "White Onion C"]
        all_names = [n for n in all_names if n.lower() not in [b.lower() for b in blacklist]]
        
        # Map to parent names
        parent_names = set()
        for name in all_names:
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
            parent_names.add(parent_name)
        
        # Create parent products dataframe
        parent_data = []
        for parent_name in parent_names:
            parent_id = _generate_stable_uuid(parent_name)
            parent_data.append({
                'parent_id': parent_id,
                'parent_product_name': parent_name,
                'created_at': pd.Timestamp.now(tz='UTC')
            })
        
        canonical_df = pd.DataFrame(parent_data)
        canonical_df = canonical_df.sort_values('parent_product_name').reset_index(drop=True)
        
        self.stats['parent_products'] = len(canonical_df)
        self.stats['mapped_products'] = len(all_names)
        
        logger.info(f"   Created {len(canonical_df):,} parent products")
        
        return canonical_df
    
    
    def _map_dataframe_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        def get_parent_id(name):
            if not name:
                return None
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
            parent_id = _generate_stable_uuid(parent_name)
            return parent_id
            
        temp_df = df.copy()
        if 'name' not in temp_df.columns:
            return temp_df
            
        temp_df['parent_id'] = temp_df['name'].apply(get_parent_id)
        return temp_df

    def upsert_canonical_master(self, canonical_df: pd.DataFrame, engine, db_name: str):
        logger.info(f"\nUPSERTING CANONICAL MASTER TO {db_name}")
        
        if canonical_df.empty:
            logger.warning(f"   ⚠️  No data to upsert to {db_name}")
            return
        
        df_to_upsert = canonical_df.rename(columns={
            'parent_id': 'id',
            'parent_name': 'name',
            'parent_product_name': 'name'
        })

        expected_cols = ['id', 'name', 'created_at']
        df_to_upsert = df_to_upsert[[c for c in expected_cols if c in df_to_upsert.columns]]
        
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS parent_products (
                        id UUID PRIMARY KEY,
                        name VARCHAR(255),
                        created_at TIMESTAMP WITH TIME ZONE
                    )
                """))
                
                df_to_upsert.to_sql(
                    'parent_products_temp',
                    conn,
                    if_exists='replace',
                    index=False
                )
                
                conn.execute(text("""
                    INSERT INTO parent_products 
                        (id, name, created_at)
                    SELECT 
                        id::uuid, 
                        name, 
                        created_at
                    FROM parent_products_temp
                    ON CONFLICT (id) 
                    DO UPDATE SET 
                        name = EXCLUDED.name,
                        created_at = EXCLUDED.created_at
                """))
                
                conn.execute(text("DROP TABLE parent_products_temp"))
            
            logger.info(f"   Upserted {len(df_to_upsert)} parent products to {db_name} (table: parent_products)")
            
        except Exception as e:
            logger.error(f"   Failed to upsert to {db_name}: {e}")
            raise
    
    def save_parent_products_to_remote(self, canonical_master):
        """Create or update parent_products table in each remote database"""
        logger.info("\nCREATING/UPDATING PARENT_PRODUCTS TABLE IN REMOTE DATABASES")
        
        standard_df = canonical_master.rename(columns={
            'parent_id': 'id',
            'parent_product_name': 'name'
        })
        standard_df = standard_df[['id', 'name', 'created_at']]
        
        # Save to Supply Chain Supabase
        logger.info("   [1/3] Upserting parent_products in Supply Chain Supabase...")
        try:
            with self.supabase_engine.begin() as conn:
                # Create table if not exists
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS parent_products (
                        id UUID PRIMARY KEY,
                        name VARCHAR(255),
                        created_at TIMESTAMP WITH TIME ZONE
                    )
                """))
                
                # Upsert via temp table
                standard_df.to_sql('temp_parent_products', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    INSERT INTO parent_products (id, name, created_at)
                    SELECT id::uuid, name, created_at
                    FROM temp_parent_products
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        created_at = EXCLUDED.created_at
                """))
                conn.execute(text("DROP TABLE temp_parent_products"))
                logger.info(f"         ✓ Upserted {len(standard_df)} parent products")
        except Exception as e:
            logger.error(f"         ❌ Failed: {e}")
        
        # Save to B2B Supabase
        logger.info("   [2/3] Upserting parent_products in B2B Supabase...")
        try:
            with self.b2b_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS parent_products (
                        id UUID PRIMARY KEY,
                        name VARCHAR(255),
                        created_at TIMESTAMP WITH TIME ZONE
                    )
                """))
                
                standard_df.to_sql('temp_parent_products', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    INSERT INTO parent_products (id, name, created_at)
                    SELECT id::uuid, name, created_at
                    FROM temp_parent_products
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        created_at = EXCLUDED.created_at
                """))
                conn.execute(text("DROP TABLE temp_parent_products"))
                logger.info(f"         ✓ Upserted {len(standard_df)} parent products")
        except Exception as e:
            logger.error(f"         ❌ Failed: {e}")
        
        # Save to Staging PostgreSQL
        logger.info("   [3/3] Upserting parent_products in Staging PostgreSQL...")
        try:
            with self.staging_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS parent_products (
                        id UUID PRIMARY KEY,
                        name VARCHAR(255),
                        created_at TIMESTAMP WITH TIME ZONE
                    )
                """))
                
                standard_df.to_sql('temp_parent_products', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    INSERT INTO parent_products (id, name, created_at)
                    SELECT id::uuid, name, created_at
                    FROM temp_parent_products
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        created_at = EXCLUDED.created_at
                """))
                conn.execute(text("DROP TABLE temp_parent_products"))
                logger.info(f"         ✓ Upserted {len(standard_df)} parent products")
        except Exception as e:
            logger.error(f"         ❌ Failed: {e}")
    
    def add_parent_id_to_remote_tables(self):
        """Add parent_id column to remote database tables"""
        logger.info("\nADDING PARENT_ID TO REMOTE TABLES")
        
        # 1. Supply Chain Supabase - products table
        logger.info("   [1/4] Updating Supply Chain Supabase products...")
        try:
            with self.supabase_engine.begin() as conn:
                # Add column if not exists
                conn.execute(text("""
                    ALTER TABLE products 
                    ADD COLUMN IF NOT EXISTS parent_id UUID
                """))
                
                # Get all products
                df = pd.read_sql("SELECT id, name FROM products WHERE name IS NOT NULL", conn)
                
                # Calculate parent_id
                def get_parent_id(name):
                    if not name:
                        return None
                    cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
                    parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
                    return _generate_stable_uuid(parent_name)
                
                df['parent_id'] = df['name'].apply(get_parent_id)
                
                # Update via temp table
                df[['id', 'parent_id']].to_sql('temp_parent_updates', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    UPDATE products p
                    SET parent_id = t.parent_id::uuid
                    FROM temp_parent_updates t
                    WHERE p.id = t.id::uuid
                """))
                conn.execute(text("DROP TABLE temp_parent_updates"))
                
                # Create index
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_products_parent_id 
                    ON products(parent_id)
                """))
                
                updated = len(df[df['parent_id'].notna()])
                logger.info(f"         ✓ Updated {updated}/{len(df)} records")
        except Exception as e:
            logger.error(f"         ❌ Failed: {e}")
        
        # 2. B2B Supabase - products table
        logger.info("   [2/4] Updating B2B Supabase products...")
        try:
            with self.b2b_engine.begin() as conn:
                conn.execute(text("""
                    ALTER TABLE products 
                    ADD COLUMN IF NOT EXISTS parent_id UUID
                """))
                
                df = pd.read_sql("SELECT id, name FROM products WHERE name IS NOT NULL", conn)
                
                def get_parent_id(name):
                    if not name:
                        return None
                    cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
                    parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
                    return _generate_stable_uuid(parent_name)
                
                df['parent_id'] = df['name'].apply(get_parent_id)
                
                df[['id', 'parent_id']].to_sql('temp_parent_updates', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    UPDATE products p
                    SET parent_id = t.parent_id::uuid
                    FROM temp_parent_updates t
                    WHERE p.id = t.id::uuid
                """))
                conn.execute(text("DROP TABLE temp_parent_updates"))
                
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_products_parent_id 
                    ON products(parent_id)
                """))
                
                updated = len(df[df['parent_id'].notna()])
                logger.info(f"         ✓ Updated {updated}/{len(df)} records")
        except Exception as e:
            logger.error(f"         ❌ Failed: {e}")
        
        # 3. Staging PostgreSQL - product_names table
        logger.info("   [3/4] Updating Staging product_names...")
        try:
            with self.staging_engine.begin() as conn:
                conn.execute(text("""
                    ALTER TABLE product_names 
                    ADD COLUMN IF NOT EXISTS parent_id UUID
                """))
                
                df = pd.read_sql("SELECT id, name FROM product_names WHERE name IS NOT NULL", conn)
                
                def get_parent_id(name):
                    if not name:
                        return None
                    cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
                    parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
                    return _generate_stable_uuid(parent_name)
                
                df['parent_id'] = df['name'].apply(get_parent_id)
                
                df[['id', 'parent_id']].to_sql('temp_parent_updates', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    UPDATE product_names p
                    SET parent_id = t.parent_id::uuid
                    FROM temp_parent_updates t
                    WHERE p.id = t.id::uuid
                """))
                conn.execute(text("DROP TABLE temp_parent_updates"))
                
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_product_names_parent_id 
                    ON product_names(parent_id)
                """))
                
                updated = len(df[df['parent_id'].notna()])
                logger.info(f"         ✓ Updated {updated}/{len(df)} records")
        except Exception as e:
            logger.error(f"         ❌ Failed: {e}")
        
        # 4. Staging PostgreSQL - products table (no update, no name column)
        logger.info("   [4/4] Staging products table...")
        logger.info(f"         ⚠️  Skipped (no name column to map)")

    
    def update_parent_ids_batch(self, engine, table_name: str, db_name: str):
        logger.info(f"\nUPDATING PARENT IDs IN {db_name}.{table_name}")
        
        try:
            with engine.begin() as conn:
                df = pd.read_sql(f"SELECT id, name FROM {table_name}", conn)
                
                if df.empty:
                    logger.warning(f"   No data in {table_name}")
                    return
                
                def get_parent_id(name):
                    if not name:
                        return None
                    cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
                    parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
                    return _generate_stable_uuid(parent_name)
                
                conn.execute(text(f"""
                    ALTER TABLE {table_name} 
                    ADD COLUMN IF NOT EXISTS parent_id UUID
                """))

                df['parent_id'] = df['name'].apply(lambda x: get_parent_id(x))
                
                df[['id', 'parent_id']].to_sql(
                    f'{table_name}_parent_ids_temp',
                    conn,
                    if_exists='replace',
                    index=False
                )
                
                conn.execute(text(f"""
                    UPDATE {table_name} t
                    SET 
                        parent_id = u.parent_id::uuid
                    FROM {table_name}_parent_ids_temp u
                    WHERE t.id = u.id::uuid
                """))
                
                conn.execute(text(f"DROP TABLE {table_name}_parent_ids_temp"))
                
                updated_count = len(df[df['parent_id'].notna()])
                logger.info(f"   Updated {updated_count}/{len(df)} records in {table_name} (column: parent_id)")
                
        except Exception as e:
            logger.error(f"   ❌ Failed to update {table_name}: {e}")
            raise
    
    def update_source_tables_parent_ids(self):
        logger.info("\n" + "=" * 80)
        logger.info("UPDATING PARENT IDs IN SOURCE TABLES")
        logger.info("=" * 80)
        
        for table_name, enabled in self.SOURCE_TABLES.items():
            if not enabled:
                logger.info(f"   Skipping {table_name} (disabled in config)")
                continue
            
            try:
                logger.info(f"\nProcessing {table_name}...")
                
                with self.supabase_engine.begin() as conn:
                    result = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = '{table_name}'
                        )
                    """))
                    table_exists = result.scalar()
                    
                    if not table_exists:
                        logger.warning(f"   Table {table_name} does not exist - skipping")
                        continue
                    
                    df = pd.read_sql(text(f"""
                        SELECT id, product_name 
                        FROM public.{table_name}
                        WHERE product_name IS NOT NULL
                    """), conn)
                    
                    if df.empty:
                        logger.warning(f"   No data in {table_name}")
                        continue
                    
                    def get_parent_id(name):
                        if not name:
                            return None
                        cleaned = re.sub(r'\s+', ' ', str(name)).replace(''', "'").replace(''', "'").replace('`', "'").strip().lower()
                        parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
                        return _generate_stable_uuid(parent_name)
                    
                    conn.execute(text(f"""
                        ALTER TABLE public.{table_name} 
                        ADD COLUMN IF NOT EXISTS parent_id UUID
                    """))
                    
                    df['parent_id'] = df['product_name'].apply(get_parent_id)
                    
                    temp_table = f'{table_name}_parent_updates'
                    df[['id', 'parent_id']].to_sql(
                        temp_table,
                        conn,
                        if_exists='replace',
                        index=False
                    )
                    
                    conn.execute(text(f"""
                        UPDATE public.{table_name} t
                        SET parent_id = u.parent_id::uuid
                        FROM {temp_table} u
                        WHERE t.id = u.id::uuid
                    """))
                    
                    # Drop temp table
                    conn.execute(text(f"DROP TABLE {temp_table}"))
                    
                    updated_count = len(df[df['parent_id'].notna()])
                    logger.info(f"   Updated {updated_count}/{len(df)} records in {table_name}")
                    
            except Exception as e:
                logger.error(f"   Failed to update {table_name}: {e}")
                continue

    def generate_report(self):
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION REPORT")
        logger.info("=" * 80)
        
        end_time = datetime.now()
        duration = (end_time - self.stats['start_time']).total_seconds()
        
        logger.info(f"\nExecution Time: {duration:.2f} seconds")
        
        logger.info(f"\nData Extraction:")
        logger.info(f"   • Supply Chain Supabase products: {self.stats['supabase_products']:,}")
        logger.info(f"   • B2B Supabase products: {self.stats['b2b_products']:,}")
        logger.info(f"   • Staging PostgreSQL products: {self.stats['staging_products']:,}")
        logger.info(f"   • Staging PostgreSQL product_names: {self.stats['staging_product_names']:,}")
        logger.info(f"   • Total products extracted: {self.stats['total_products']:,}")
        
        logger.info(f"\nData Validation:")
        logger.info(f"   • Records removed (bad data): {self.stats['validation_removed']:,}")
        validation_rate = (self.stats['validation_removed'] / self.stats['total_products'] * 100) if self.stats['total_products'] > 0 else 0
        logger.info(f"   • Removal rate: {validation_rate:.2f}%")
        
        logger.info(f"\nTransformation:")
        logger.info(f"   • Exact mapped products: {self.stats['mapped_products']:,}")
        logger.info(f"   • Fuzzy matched products: {self.stats['fuzzy_matched']:,}")
        logger.info(f"   • Unmapped products (self-mapped): {self.stats['unmapped_products']:,}")
        logger.info(f"   • Parent products created: {self.stats['parent_products']:,}")
        
        mapping_rate = (self.stats['mapped_products'] / self.stats['total_products'] * 100) if self.stats['total_products'] > 0 else 0
        logger.info(f"   • Exact mapping coverage: {mapping_rate:.1f}%")
        
        if self.enable_fuzzy_matching and self.fuzzy_matcher:
            self.fuzzy_matcher.print_stats()
            if self.fuzzy_dry_run:
                logger.info(f"\nFUZZY MATCHING IN DRY RUN MODE")
                logger.info(f"   To enable fuzzy matching, set fuzzy_dry_run=False when initializing pipeline")
        
        logger.info(f"\nData Loading:")
        logger.info(f"   ✓ Updated remote databases:")
        logger.info(f"      • parent_products table created in all 3 databases")
        logger.info(f"      • Supply Chain Supabase: products + parent_id")
        logger.info(f"      • B2B Supabase: products + parent_id")
        logger.info(f"      • Staging PostgreSQL: product_names + parent_id")
        logger.info(f"   ✓ Indexes created on parent_id columns")
        
        logger.info(f"\nDatabase Summary:")
        logger.info(f"   • All remote databases updated with parent_id")
        logger.info(f"   • parent_products table created in each database")
        
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
    
    def run(self):
        try:
            logger.info("\n" + "=" * 80)
            logger.info("PRODUCTION ETL PIPELINE - MULTI-SOURCE PRODUCT STANDARDIZATION")
            logger.info("=" * 80)
            
            self.connect_to_databases()
            
            df_supabase = self.extract_from_supabase()
            df_b2b = self.extract_from_b2b()
            df_staging_products, df_staging_names = self.extract_from_staging()
            
            self.stats['total_products'] = self.stats['supabase_products'] + self.stats['b2b_products'] + self.stats['staging_product_names']
            
            canonical_master = self.transform_and_standardize(df_supabase, df_b2b, df_staging_products, df_staging_names)
            
            if canonical_master.empty:
                logger.error("No canonical master data generated")
                return False
            
            logger.info("\n" + "=" * 80)
            logger.info("💾 UPDATING REMOTE DATABASES")
            logger.info("=" * 80)
            
            # Create parent_products table in all remote databases
            self.save_parent_products_to_remote(canonical_master)
            
            # Add parent_id column to existing tables
            self.add_parent_id_to_remote_tables()
            
            self.generate_report()
            
            return True
            
        except Exception as e:
            logger.error(f"\nPIPELINE FAILED: {e}")
            traceback.print_exc()
            return False


def main():
    os.makedirs('logs', exist_ok=True)
    
    pipeline = ProductionETLPipeline()
    success = pipeline.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
