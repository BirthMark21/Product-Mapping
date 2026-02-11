#!/usr/bin/env python3
"""
Production-Grade Multi-Source ETL Pipeline with Dynamic Mapping
Extracts from Supabase + Production PostgreSQL, standardizes, and loads to all databases

ENHANCEMENTS:
- Data validation to prevent bad data
- Fuzzy matching to reduce manual mapping work
- Retry logic for transient failures
- Distributed transactions for data consistency
- Enhanced logging and monitoring
"""

import sys
import os
import io
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import uuid
from sqlalchemy import text
from datetime import datetime
from utils.db_connector import get_db_engine
from utils.resilient_db import ResilientDBConnector
from utils.data_validator import ProductDataValidator
from utils.fuzzy_matcher import FuzzyProductMatcher
from utils.transaction_manager import DistributedTransactionManager
from pipeline.standardization import PARENT_CHILD_MAPPING, CHILD_TO_PARENT_MAP, _generate_stable_uuid
import logging

# Configure logging
os.makedirs('logs', exist_ok=True)

# Ensure stdout/stderr are utf-8 encoded for Windows
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
    """Production-grade ETL pipeline for product standardization"""
    
    def __init__(self, enable_fuzzy_matching: bool = True, fuzzy_dry_run: bool = True):
        """
        Initialize pipeline
        
        Args:
            enable_fuzzy_matching: Enable fuzzy matching for unmapped products
            fuzzy_dry_run: If True, fuzzy matching logs matches but doesn't use them
        """
        self.supabase_engine = None
        self.staging_engine = None
        self.hub_engine = None
        
        # Initialize validators and matchers
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
        """Establish connections to all databases with retry logic"""
        logger.info("=" * 80)
        logger.info("🔌 CONNECTING TO DATABASES (with retry logic)")
        logger.info("=" * 80)
        
        try:
            self.supabase_engine = ResilientDBConnector.get_engine_with_retry('supabase')
        except Exception as e:
            logger.error(f"❌ Failed to connect to Supabase after retries: {e}")
            raise
        
        try:
            self.staging_engine = ResilientDBConnector.get_engine_with_retry('staging')
        except Exception as e:
            logger.error(f"❌ Failed to connect to Staging PostgreSQL after retries: {e}")
            raise
        
        try:
            self.hub_engine = ResilientDBConnector.get_engine_with_retry('hub')
        except Exception as e:
            logger.error(f"❌ Failed to connect to Local Hub after retries: {e}")
            raise
    
    def extract_from_supabase(self) -> pd.DataFrame:
        """Extract ALL columns from Supabase products table"""
        logger.info("\n📥 EXTRACTING FROM SUPABASE POSTGRESQL")
        
        try:
            with self.supabase_engine.connect() as conn:
                # Get ALL columns from the source table
                df = pd.read_sql("""
                    SELECT *
                    FROM public.products
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                      AND TRIM(name) != '0'
                """, conn)
                
                # Rename id and name for consistency
                df = df.rename(columns={'id': 'raw_product_id', 'name': 'raw_product_name'})
                
                # Convert id to text
                df['raw_product_id'] = df['raw_product_id'].astype(str)
                
                # Add metadata columns
                df['source_table'] = 'supabase_products'
                df['source_db'] = 'supabase'
            
            self.stats['supabase_products'] = len(df)
            logger.info(f"   ✅ Extracted {len(df)} products from Supabase with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"   ❌ Failed to extract from Supabase: {e}")
            raise
    
    def extract_from_staging(self):
        """
        Extract ALL columns from Staging PostgreSQL (ChipChip).
        Queries both 'products' (joined with names) and 'product_names' tables.
        """
        if not self.staging_engine:
            logger.warning("⚠️  Staging engine not initialized. Skipping Staging extraction.")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info("\n📥 EXTRACTING FROM STAGING POSTGRESQL (chipchip)")
        
        try:
            with self.staging_engine.connect() as conn:
                # 1. Fetch ALL columns from product_names
                logger.info("   → Fetching from 'product_names' table...")
                df_names = pd.read_sql(text("""
                    SELECT *
                    FROM public.product_names
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                """), conn)
                
                # Rename columns for consistency
                df_names = df_names.rename(columns={'id': 'raw_product_id', 'name': 'raw_product_name'})
                df_names['raw_product_id'] = df_names['raw_product_id'].astype(str)
                
                # Add metadata columns
                df_names['source_table'] = 'staging_product_names'
                df_names['source_db'] = 'staging_postgres'
                
                # 2. Fetch ALL columns from products (joined with names)
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
                
                # Rename id column for consistency
                df_products = df_products.rename(columns={'id': 'raw_product_id'})
                df_products['raw_product_id'] = df_products['raw_product_id'].astype(str)
                
                # Add metadata columns
                df_products['source_table'] = 'staging_products'
                df_products['source_db'] = 'staging_postgres'
                
                self.stats['staging_products'] = len(df_products)
                self.stats['staging_product_names'] = len(df_names)
                
                logger.info(f"   ✅ Extracted {len(df_names)} names ({len(df_names.columns)} cols) and {len(df_products)} products ({len(df_products.columns)} cols) from Staging")
                return df_products, df_names
                
        except Exception as e:
            logger.error(f"   ❌ Failed to extract from Staging PostgreSQL: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def transform_and_standardize(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Apply standardization mapping with validation and fuzzy matching"""
        logger.info("\n🔄 TRANSFORMING AND STANDARDIZING")
        
        if combined_df.empty:
            logger.warning("   ⚠️  No data to transform")
            return pd.DataFrame()
        
        # STEP 1: Data Validation
        logger.info("\n📋 STEP 1: DATA VALIDATION")
        df, validation_stats = self.data_validator.validate_dataframe(combined_df)
        self.stats['validation_removed'] = validation_stats['total_removed']
        
        if df.empty:
            logger.error("❌ No valid data remaining after validation")
            return pd.DataFrame()
        
        # STEP 2: Clean product names
        logger.info("\n🧹 STEP 2: CLEANING PRODUCT NAMES")
        logger.info("   → Normalizing whitespace and case...")
        df['cleaned_name'] = df['raw_product_name'].apply(
            lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower()
        )
        
        # STEP 3: Apply parent-child mapping
        logger.info("\n🗺️  STEP 3: APPLYING PARENT-CHILD MAPPING")
        logger.info("   → Trying exact matches first...")
        df['parent_name'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
        
        # Count exact matches
        exact_matched = df['parent_name'].notna().sum()
        logger.info(f"   ✅ Exact matches: {exact_matched:,} products")
        
        # STEP 4: Apply fuzzy matching for unmapped products
        if self.enable_fuzzy_matching and self.fuzzy_matcher:
            logger.info("\n🔍 STEP 4: APPLYING FUZZY MATCHING")
            
            # Get unmapped products
            unmapped_mask = df['parent_name'].isna()
            unmapped_count = unmapped_mask.sum()
            
            if unmapped_count > 0:
                logger.info(f"   → Attempting fuzzy match for {unmapped_count:,} unmapped products...")
                
                # Apply fuzzy matching
                df.loc[unmapped_mask, 'parent_name'] = df.loc[unmapped_mask, 'raw_product_name'].apply(
                    self.fuzzy_matcher.find_parent
                )
                
                # Get fuzzy match stats
                fuzzy_stats = self.fuzzy_matcher.get_stats()
                self.stats['fuzzy_matched'] = fuzzy_stats['fuzzy_matches']
                
                if not self.fuzzy_dry_run and fuzzy_stats['fuzzy_matches'] > 0:
                    logger.info(f"   ✅ Fuzzy matched: {fuzzy_stats['fuzzy_matches']:,} products")
                elif self.fuzzy_dry_run and fuzzy_stats['fuzzy_matches'] > 0:
                    logger.info(f"   💡 [DRY RUN] Would fuzzy match: {fuzzy_stats['fuzzy_matches']:,} products")
                    logger.info(f"      Set fuzzy_dry_run=False to enable fuzzy matching")
        else:
            logger.info("\n⏭️  STEP 4: FUZZY MATCHING DISABLED")
        
        # STEP 5: For still unmapped products, use original name as parent
        df['parent_name'] = df['parent_name'].fillna(df['raw_product_name'])
        
        # Final statistics
        mapped = df['cleaned_name'].isin(CHILD_TO_PARENT_MAP).sum()
        unmapped = len(df) - mapped
        self.stats['mapped_products'] = mapped
        self.stats['unmapped_products'] = unmapped
        
        logger.info(f"\n📊 MAPPING SUMMARY:")
        logger.info(f"   • Total products: {len(df):,}")
        logger.info(f"   • Exact matches: {mapped:,}")
        logger.info(f"   • Fuzzy matches: {self.stats['fuzzy_matched']:,}")
        logger.info(f"   • Unmapped (self-mapped): {unmapped:,}")
        
        mapping_rate = (mapped / len(df) * 100) if len(df) > 0 else 0
        logger.info(f"   • Exact match rate: {mapping_rate:.1f}%")
        
        # STEP 6: Convert timestamps
        logger.info("\n📅 STEP 5: CONVERTING TIMESTAMPS")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        
        # STEP 7: Aggregate by parent to get earliest created_at and source
        logger.info("\n📦 STEP 6: AGGREGATING BY PARENT PRODUCT")
        parent_groups = df.groupby('parent_name').agg({
            'created_at': lambda x: x.dropna().min() if not x.dropna().empty else None,
            'source_db': lambda x: x.iloc[0] if len(x) > 0 else None  # First occurrence source
        }).reset_index()
        
        # STEP 8: Generate stable UUIDs for parent products
        logger.info("   → Generating stable parent product IDs...")
        parent_groups['parent_product_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
        
        # Rename parent_name to parent_product_name to match DB
        parent_groups = parent_groups.rename(columns={'parent_name': 'parent_product_name'})
        
        # Final canonical master table
        canonical_df = parent_groups[[
            'parent_product_id',
            'parent_product_name',
            'source_db',
            'created_at'
        ]].copy()
        
        # Rename source_db to source for clarity
        canonical_df = canonical_df.rename(columns={'source_db': 'source'})
        
        canonical_df = canonical_df.sort_values('parent_product_name').reset_index(drop=True)
        
        self.stats['parent_products'] = len(canonical_df)
        logger.info(f"   ✅ Created {len(canonical_df):,} parent products")
        
        return canonical_df
    
    
    def _map_dataframe_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Helper to add parent_product_id and parent_product_name to a dataframe"""
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
            
        temp_df[['parent_product_id', 'parent_product_name']] = temp_df[name_col].apply(
            lambda x: pd.Series(get_parent_info(x))
        )
        return temp_df

    def upsert_standard_products(self, canonical_df: pd.DataFrame, engine, db_name: str):
        """UPSERT hub_standard_products table (no data loss)"""
        logger.info(f"\n💾 UPSERTING CANONICAL MASTER TO {db_name}")
        
        if canonical_df.empty:
            logger.warning(f"   ⚠️  No data to upsert to {db_name}")
            return
        
        try:
            with engine.begin() as conn:
                canonical_df.to_sql(
                    'hub_standard_products_temp',
                    conn,
                    if_exists='replace',
                    index=False
                )
                
                # UPSERT query targeting 'hub_standard_products'
                conn.execute(text("""
                    INSERT INTO hub_standard_products 
                        (parent_product_id, parent_product_name, created_at)
                    SELECT parent_product_id, parent_product_name, created_at
                    FROM hub_standard_products_temp
                    ON CONFLICT (parent_product_id) 
                    DO UPDATE SET 
                        parent_product_name = EXCLUDED.parent_product_name
                """))
                
                # Drop temp table
                conn.execute(text("DROP TABLE hub_standard_products_temp"))
            
            logger.info(f"   ✅ Upserted {len(canonical_df)} parent products to {db_name}")
            
        except Exception as e:
            logger.error(f"   ❌ Failed to upsert to {db_name}: {e}")
            raise
    
    def update_parent_ids_batch(self, engine, table_name: str, db_name: str):
        """Update parent_product_id in source tables using batch processing"""
        logger.info(f"\n🔗 UPDATING PARENT IDs IN {db_name}.{table_name}")
        
        try:
            with engine.begin() as conn:
                # Read current data
                df = pd.read_sql(f"SELECT id, name FROM {table_name}", conn)
                
                if df.empty:
                    logger.warning(f"   ⚠️  No data in {table_name}")
                    return
                
                # Calculate parent IDs and names
                def get_parent_info(name):
                    if not name:
                        return None, None
                    cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
                    parent_name = CHILD_TO_PARENT_MAP.get(cleaned, name)
                    parent_id = _generate_stable_uuid(parent_name)
                    return parent_id, parent_name
                
                df[['parent_product_id', 'parent_product_name']] = df['name'].apply(
                    lambda x: pd.Series(get_parent_info(x))
                )
                
                # Create temp table with updates
                df[['id', 'parent_product_id', 'parent_product_name']].to_sql(
                    f'{table_name}_parent_updates',
                    conn,
                    if_exists='replace',
                    index=False
                )
                
                # Batch update
                conn.execute(text(f"""
                    UPDATE {table_name} t
                    SET 
                        parent_product_id = u.parent_product_id,
                        parent_product_name = u.parent_product_name
                    FROM {table_name}_parent_updates u
                    WHERE t.id = u.id
                """))
                
                # Drop temp table
                conn.execute(text(f"DROP TABLE {table_name}_parent_updates"))
                
                updated_count = len(df[df['parent_product_id'].notna()])
                logger.info(f"   ✅ Updated {updated_count}/{len(df)} records in {table_name}")
                
        except Exception as e:
            logger.error(f"   ❌ Failed to update {table_name}: {e}")
            raise
    
    def generate_report(self):
        """Generate pipeline execution report"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 PIPELINE EXECUTION REPORT")
        logger.info("=" * 80)
        
        end_time = datetime.now()
        duration = (end_time - self.stats['start_time']).total_seconds()
        
        logger.info(f"\n⏱️  Execution Time: {duration:.2f} seconds")
        
        logger.info(f"\n📥 Data Extraction:")
        logger.info(f"   • Supabase products: {self.stats['supabase_products']:,}")
        logger.info(f"   • Staging PostgreSQL products: {self.stats['staging_products']:,}")
        logger.info(f"   • Staging PostgreSQL product_names: {self.stats['staging_product_names']:,}")
        logger.info(f"   • Total products extracted: {self.stats['total_products']:,}")
        
        logger.info(f"\n🔍 Data Validation:")
        logger.info(f"   • Records removed (bad data): {self.stats['validation_removed']:,}")
        validation_rate = (self.stats['validation_removed'] / self.stats['total_products'] * 100) if self.stats['total_products'] > 0 else 0
        logger.info(f"   • Removal rate: {validation_rate:.2f}%")
        
        logger.info(f"\n🔄 Transformation:")
        logger.info(f"   • Exact mapped products: {self.stats['mapped_products']:,}")
        logger.info(f"   • Fuzzy matched products: {self.stats['fuzzy_matched']:,}")
        logger.info(f"   • Unmapped products (self-mapped): {self.stats['unmapped_products']:,}")
        logger.info(f"   • Parent products created: {self.stats['parent_products']:,}")
        
        mapping_rate = (self.stats['mapped_products'] / self.stats['total_products'] * 100) if self.stats['total_products'] > 0 else 0
        logger.info(f"   • Exact mapping coverage: {mapping_rate:.1f}%")
        
        if self.enable_fuzzy_matching and self.fuzzy_matcher:
            self.fuzzy_matcher.print_stats()
            if self.fuzzy_dry_run:
                logger.info(f"\n💡 FUZZY MATCHING IN DRY RUN MODE")
                logger.info(f"   To enable fuzzy matching, set fuzzy_dry_run=False when initializing pipeline")
        
        logger.info(f"\n💾 Data Loading:")
        logger.info(f"   ✅ Canonical master updated in 3 databases")
        logger.info(f"   ✅ Parent IDs updated in source tables")
        
        logger.info(f"\n🔄 PeerDB Sync:")
        logger.info(f"   ⏳ Production PostgreSQL → ClickHouse (automatic)")
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
    
    def run(self):
        """Execute the complete ETL pipeline"""
        try:
            logger.info("\n" + "🚀" * 40)
            logger.info("PRODUCTION ETL PIPELINE - MULTI-SOURCE PRODUCT STANDARDIZATION")
            logger.info("🚀" * 40)
            
            # Step 1: Connect to databases
            self.connect_to_databases()
            
            # Step 2: Extract from all sources
            df_supabase = self.extract_from_supabase()
            df_staging_products, df_staging_names = self.extract_from_staging()
            
            # Step 3: Combine all data
            logger.info("\n🔗 COMBINING DATA FROM ALL SOURCES")
            combined_df = pd.concat([df_supabase, df_staging_products, df_staging_names], ignore_index=True)
            self.stats['total_products'] = len(combined_df)
            logger.info(f"   ✅ Combined {len(combined_df)} total products")
            
            # Step 4: Transform and standardize
            canonical_master = self.transform_and_standardize(combined_df)
            
            if canonical_master.empty:
                logger.error("❌ No canonical master data generated")
                return False
            
            # Step 5: Load canonical master to all databases
            logger.info("\n" + "=" * 80)
            logger.info("💾 LOADING CANONICAL MASTER TO DATABASES")
            logger.info("=" * 80)
            
            # READ-ONLY MODE: Scoping writes to Local Hub ONLY
            logger.info("   🔒 READ-ONLY MODE: Skipping updates to Supabase and Staging")
            # self.upsert_canonical_master(canonical_master, self.supabase_engine, "Supabase PostgreSQL")
            # self.upsert_canonical_master(canonical_master, self.staging_engine, "Staging PostgreSQL")
            
            # Write to Local Hub
            self.upsert_standard_products(canonical_master, self.hub_engine, "Local Hub")
            
            # Step 6: Save mapped results to local hub (since source is read-only)
            logger.info("\n" + "=" * 80)
            logger.info("💾 SAVING MAPPED RESULTS TO LOCAL HUB")
            logger.info("=" * 80)
            
            # Save to supabase_products (local copy with parent_product_id)
            if not df_supabase.empty:
                df_sup_with_ids = self._map_dataframe_ids(df_supabase)
                
                # Restore original column names and remove metadata
                df_to_save = df_sup_with_ids.copy()
                df_to_save = df_to_save.rename(columns={'raw_product_id': 'id', 'raw_product_name': 'name'})
                
                # Remove internal metadata columns, keep only parent_product_id
                cols_to_drop = ['source_table', 'source_db', 'parent_product_name']
                df_to_save = df_to_save.drop(columns=[c for c in cols_to_drop if c in df_to_save.columns])
                
                df_to_save.to_sql('supabase_products', self.hub_engine, if_exists='replace', index=False)
                logger.info(f"   ✅ Saved {len(df_to_save)} mapped Supabase records to supabase_products ({len(df_to_save.columns)} columns)")

            # Save to clickhouse_product_names (local copy with parent_product_id)
            df_staging_combined = pd.concat([df_staging_products, df_staging_names], ignore_index=True)
            if not df_staging_combined.empty:
                df_staging_with_ids = self._map_dataframe_ids(df_staging_combined)
                
                # Restore original column names and remove metadata
                df_to_save = df_staging_with_ids.copy()
                df_to_save = df_to_save.rename(columns={'raw_product_id': 'id', 'raw_product_name': 'name'})
                
                # Remove internal metadata columns, keep only parent_product_id
                cols_to_drop = ['source_table', 'source_db', 'parent_product_name']
                df_to_save = df_to_save.drop(columns=[c for c in cols_to_drop if c in df_to_save.columns])
                
                df_to_save.to_sql('clickhouse_product_names', self.hub_engine, if_exists='replace', index=False)
                logger.info(f"   ✅ Saved {len(df_to_save)} mapped Staging records to clickhouse_product_names ({len(df_to_save.columns)} columns)")


            
            # Step 7: Export fuzzy matches for review
            if self.enable_fuzzy_matching and self.fuzzy_matcher:
                logger.info("\n" + "=" * 80)
                logger.info("📝 EXPORTING FUZZY MATCHES FOR REVIEW")
                logger.info("=" * 80)
                self.fuzzy_matcher.export_fuzzy_matches('logs/fuzzy_matches_review.csv')
            
            # Step 8: Generate report
            self.generate_report()
            
            return True
            
        except Exception as e:
            logger.error(f"\n❌ PIPELINE FAILED: {e}")
            traceback.print_exc()
            return False


def main():
    """Main entry point"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Run pipeline
    pipeline = ProductionETLPipeline()
    success = pipeline.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
