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
                
                df = df.rename(columns={'id': 'raw_product_id', 'name': 'raw_product_name'})
                df['raw_product_id'] = df['raw_product_id'].astype(str)
                df['source_table'] = 'supabase_products'
                df['source_db'] = 'supabase'
            
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
                
                df = df.rename(columns={'id': 'raw_product_id', 'name': 'raw_product_name'})
                df['raw_product_id'] = df['raw_product_id'].astype(str)
                df['source_table'] = 'b2b_products'
                df['source_db'] = 'b2b'
            
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
                
                df_names = df_names.rename(columns={'id': 'raw_product_id', 'name': 'raw_product_name'})
                df_names['raw_product_id'] = df_names['raw_product_id'].astype(str)
                df_names['source_table'] = 'staging_product_names'
                df_names['source_db'] = 'staging_postgres'
                
                logger.info("   → Fetching from 'products' table (joined)...")
                df_products = pd.read_sql(text("""
                    SELECT 
                        p.*,
                        pn.name as raw_product_name
                    FROM public.products p
                    LEFT JOIN public.product_names pn ON p.name_id = pn.id
                    WHERE pn.name IS NOT NULL
                      AND TRIM(pn.name) != ''
                      AND p.deleted_at IS NULL
                """), conn)
                
                df_products = df_products.rename(columns={'id': 'raw_product_id'})
                df_products['raw_product_id'] = df_products['raw_product_id'].astype(str)
                df_products['source_table'] = 'staging_products'
                df_products['source_db'] = 'staging_postgres'
                
                self.stats['staging_products'] = len(df_products)
                self.stats['staging_product_names'] = len(df_names)
                
                logger.info(f"   Extracted {len(df_names)} names ({len(df_names.columns)} cols) and {len(df_products)} products ({len(df_products.columns)} cols) from Staging")
                return df_products, df_names
                
        except Exception as e:
            logger.error(f"   Failed to extract from Staging PostgreSQL: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def transform_and_standardize(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("\nTRANSFORMING AND STANDARDIZING")
        
        if combined_df.empty:
            logger.warning("   No data to transform")
            return pd.DataFrame()
        
        logger.info("\nSTEP 1: DATA VALIDATION")
        df, validation_stats = self.data_validator.validate_dataframe(combined_df)
        self.stats['validation_removed'] = validation_stats['total_removed']
        
        if df.empty:
            logger.error("No valid data remaining after validation")
            return pd.DataFrame()
        
        logger.info("\nFILTERING BLACKLISTED PRODUCTS")
        blacklist = ["Product name", "Item name", "product name", "item name", "White Onion", "white onion", "White Onion A", "White Onion B", "White Onion C"]
        initial_count = len(df)
        df = df[~df['raw_product_name'].str.lower().isin([b.lower() for b in blacklist])]
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            logger.info(f"   Filtered out {filtered_count} blacklisted products")
        else:
            logger.info(f"   No blacklisted products found")
        
        logger.info("\nSTEP 2: CLEANING PRODUCT NAMES")
        logger.info("   → Normalizing whitespace, apostrophes, and case...")
        df['cleaned_name'] = df['raw_product_name'].apply(
            lambda x: re.sub(r'\s+', ' ', str(x)).replace(''', "'").replace(''', "'").replace('`', "'").strip().lower()
        )
        
        logger.info("\nSTEP 3: APPLYING PARENT-CHILD MAPPING")
        logger.info("   → Trying exact matches first...")
        df['parent_name'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
        
        exact_matched = df['parent_name'].notna().sum()
        logger.info(f"   Exact matches: {exact_matched:,} products")
        
        if self.enable_fuzzy_matching and self.fuzzy_matcher:
            logger.info("\nSTEP 4: APPLYING FUZZY MATCHING")
            
            unmapped_mask = df['parent_name'].isna()
            unmapped_count = unmapped_mask.sum()
            
            if unmapped_count > 0:
                logger.info(f"   → Attempting fuzzy match for {unmapped_count:,} unmapped products...")
                
                df.loc[unmapped_mask, 'parent_name'] = df.loc[unmapped_mask, 'raw_product_name'].apply(
                    self.fuzzy_matcher.find_parent
                )
                
                fuzzy_stats = self.fuzzy_matcher.get_stats()
                self.stats['fuzzy_matched'] = fuzzy_stats['fuzzy_matches']
                
                if not self.fuzzy_dry_run and fuzzy_stats['fuzzy_matches'] > 0:
                    logger.info(f"   Fuzzy matched: {fuzzy_stats['fuzzy_matches']:,} products")
                elif self.fuzzy_dry_run and fuzzy_stats['fuzzy_matches'] > 0:
                    logger.info(f"   [DRY RUN] Would fuzzy match: {fuzzy_stats['fuzzy_matches']:,} products")
                    logger.info(f"      Set fuzzy_dry_run=False to enable fuzzy matching")
        else:
            logger.info("\nSTEP 4: FUZZY MATCHING DISABLED")
        
        df['parent_name'] = df['parent_name'].fillna(df['raw_product_name'])
        
        mapped = df['cleaned_name'].isin(CHILD_TO_PARENT_MAP).sum()
        unmapped = len(df) - mapped
        self.stats['mapped_products'] = mapped
        self.stats['unmapped_products'] = unmapped
        
        logger.info(f"\nMAPPING SUMMARY:")
        logger.info(f"   • Total products: {len(df):,}")
        logger.info(f"   • Exact matches: {mapped:,}")
        logger.info(f"   • Fuzzy matches: {self.stats['fuzzy_matched']:,}")
        logger.info(f"   • Unmapped (self-mapped): {unmapped:,}")
        
        mapping_rate = (mapped / len(df) * 100) if len(df) > 0 else 0
        logger.info(f"   • Exact match rate: {mapping_rate:.1f}%")
        
        logger.info("\nSTEP 5: CONVERTING TIMESTAMPS")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        
        logger.info("\nSTEP 6: AGGREGATING BY PARENT PRODUCT")
        parent_groups = df.groupby('parent_name').agg({
            'created_at': lambda x: x.dropna().min() if not x.dropna().empty else None,
            'source_db': lambda x: x.iloc[0] if len(x) > 0 else None
        }).reset_index()
        
        logger.info("   → Generating stable parent product IDs...")
        parent_groups['parent_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
        
        parent_groups = parent_groups.rename(columns={'parent_name': 'parent_product_name'})
        
        canonical_df = parent_groups[[
            'parent_id',
            'parent_product_name',
            'created_at'
        ]].copy()
        
        canonical_df = canonical_df.sort_values('parent_product_name').reset_index(drop=True)
        
        self.stats['parent_products'] = len(canonical_df)
        logger.info(f"   Created {len(canonical_df):,} parent products")
        
        return canonical_df
    
    
    def _map_dataframe_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        def get_parent_info(name):
            if not name:
                return None, None
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
            parent_id = _generate_stable_uuid(parent_name)
            return parent_id, parent_name
            
        temp_df = df.copy()
        if 'raw_product_name' in temp_df.columns:
            name_col = 'raw_product_name'
        elif 'name' in temp_df.columns:
            name_col = 'name'
        else:
            return temp_df
            
        temp_df[['parent_id', 'parent_product_name']] = temp_df[name_col].apply(
            lambda x: pd.Series(get_parent_info(x))
        )
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
        logger.info(f"   Canonical master updated in 3 databases")
        logger.info(f"   Parent IDs updated in source tables")
        
        logger.info(f"\nPeerDB Sync:")
        logger.info(f"   Production PostgreSQL -> ClickHouse (automatic)")
        
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
            
            logger.info("\nCOMBINING DATA FROM ALL SOURCES")
            combined_df = pd.concat([df_supabase, df_b2b, df_staging_products, df_staging_names], ignore_index=True)
            self.stats['total_products'] = len(combined_df)
            logger.info(f"   Combined {len(combined_df)} total products")
            
            canonical_master = self.transform_and_standardize(combined_df)
            
            if canonical_master.empty:
                logger.error("No canonical master data generated")
                return False
            
            logger.info("\n" + "=" * 80)
            logger.info("💾 LOADING CANONICAL MASTER TO DATABASES")
            logger.info("=" * 80)
            
            logger.info("   🔓 READ-WRITE MODE: Updating Supabase and Staging")
            
            self.upsert_canonical_master(canonical_master, self.supabase_engine, "Supply Chain Supabase PostgreSQL")
            self.upsert_canonical_master(canonical_master, self.b2b_engine, "B2B Supabase PostgreSQL")
            self.upsert_canonical_master(canonical_master, self.staging_engine, "Staging PostgreSQL")
            
            self.update_parent_ids_batch(self.supabase_engine, 'products', "Supply Chain Supabase PostgreSQL")
            self.update_parent_ids_batch(self.b2b_engine, 'products', "B2B Supabase PostgreSQL")
            
            self.update_parent_ids_batch(self.staging_engine, 'product_names', "Staging PostgreSQL")
            
            self.update_source_tables_parent_ids()

            
            if self.enable_fuzzy_matching and self.fuzzy_matcher:
                logger.info("\n" + "=" * 80)
                logger.info("EXPORTING FUZZY MATCHES FOR REVIEW")
                logger.info("=" * 80)
                self.fuzzy_matcher.export_fuzzy_matches('logs/fuzzy_matches_review.csv')
            
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
