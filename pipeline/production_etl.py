#!/usr/bin/env python3
"""
Production-Grade Multi-Source ETL Pipeline with Dynamic Mapping
Extracts from Supabase + Production PostgreSQL, standardizes, and loads to all databases
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import uuid
from sqlalchemy import text
from datetime import datetime
from utils.db_connector import get_db_engine
from pipeline.standardization import PARENT_CHILD_MAPPING, CHILD_TO_PARENT_MAP, _generate_stable_uuid
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProductionETLPipeline:
    """Production-grade ETL pipeline for product standardization"""
    
    def __init__(self):
        self.supabase_engine = None
        self.prod_pg_engine = None
        self.hub_engine = None
        self.stats = {
            'start_time': datetime.now(),
            'supabase_products': 0,
            'prod_pg_products': 0,
            'prod_pg_product_names': 0,
            'total_products': 0,
            'parent_products': 0,
            'mapped_products': 0,
            'unmapped_products': 0
        }
    
    def connect_to_databases(self):
        """Establish connections to all databases"""
        logger.info("=" * 80)
        logger.info("🔌 CONNECTING TO DATABASES")
        logger.info("=" * 80)
        
        try:
            self.supabase_engine = get_db_engine('supabase')
            logger.info("✅ Connected to Supabase PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Supabase: {e}")
            raise
        
        try:
            self.prod_pg_engine = get_db_engine('prod_postgres')
            logger.info("✅ Connected to Production PostgreSQL (chipchip)")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Production PostgreSQL: {e}")
            raise
        
        try:
            self.hub_engine = get_db_engine('hub')
            logger.info("✅ Connected to Local Hub PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Local Hub: {e}")
            raise
    
    def extract_from_supabase(self) -> pd.DataFrame:
        """Extract products from Supabase PostgreSQL"""
        logger.info("\n📥 EXTRACTING FROM SUPABASE POSTGRESQL")
        
        try:
            with self.supabase_engine.connect() as conn:
                df = pd.read_sql("""
                    SELECT 
                        id::text as raw_product_id,
                        name as raw_product_name,
                        created_at,
                        'supabase_products' as source_table,
                        'supabase' as source_db
                    FROM public.products
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                      AND TRIM(name) != '0'
                """, conn)
            
            self.stats['supabase_products'] = len(df)
            logger.info(f"   ✅ Extracted {len(df)} products from Supabase")
            return df
            
        except Exception as e:
            logger.error(f"   ❌ Failed to extract from Supabase: {e}")
            raise
    
    def extract_from_prod_postgres(self) -> tuple:
        """Extract products and product_names from Production PostgreSQL"""
        logger.info("\n📥 EXTRACTING FROM PRODUCTION POSTGRESQL (chipchip)")
        
        try:
            with self.prod_pg_engine.connect() as conn:
                # Extract products
                df_products = pd.read_sql("""
                    SELECT 
                        id::text as raw_product_id,
                        name as raw_product_name,
                        created_at,
                        'prod_pg_products' as source_table,
                        'prod_postgres' as source_db
                    FROM public.products
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                      AND TRIM(name) != '0'
                """, conn)
                
                # Extract product_names
                df_product_names = pd.read_sql("""
                    SELECT 
                        id::text as raw_product_id,
                        name as raw_product_name,
                        created_at,
                        'prod_pg_product_names' as source_table,
                        'prod_postgres' as source_db
                    FROM public.product_names
                    WHERE name IS NOT NULL 
                      AND TRIM(name) != ''
                      AND TRIM(name) != '0'
                """, conn)
            
            self.stats['prod_pg_products'] = len(df_products)
            self.stats['prod_pg_product_names'] = len(df_product_names)
            
            logger.info(f"   ✅ Extracted {len(df_products)} from products table")
            logger.info(f"   ✅ Extracted {len(df_product_names)} from product_names table")
            
            return df_products, df_product_names
            
        except Exception as e:
            logger.error(f"   ❌ Failed to extract from Production PostgreSQL: {e}")
            raise
    
    def transform_and_standardize(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Apply standardization mapping to create canonical master"""
        logger.info("\n🔄 TRANSFORMING AND STANDARDIZING")
        
        if combined_df.empty:
            logger.warning("   ⚠️  No data to transform")
            return pd.DataFrame()
        
        df = combined_df.copy()
        
        # Clean product names
        logger.info("   → Cleaning product names...")
        df['cleaned_name'] = df['raw_product_name'].apply(
            lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower()
        )
        
        # Apply parent-child mapping
        logger.info("   → Applying parent-child mapping...")
        df['parent_name'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
        
        # For unmapped products, use original name as parent
        df['parent_name'] = df['parent_name'].fillna(df['raw_product_name'])
        
        # Count mapped vs unmapped
        mapped = df['cleaned_name'].isin(CHILD_TO_PARENT_MAP).sum()
        unmapped = len(df) - mapped
        self.stats['mapped_products'] = mapped
        self.stats['unmapped_products'] = unmapped
        
        logger.info(f"   ✅ Mapped: {mapped} products")
        logger.info(f"   ⚠️  Unmapped: {unmapped} products (will use original name)")
        
        # Convert timestamps
        logger.info("   → Converting timestamps...")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        
        # Aggregate by parent to get earliest created_at and source
        logger.info("   → Aggregating by parent product...")
        parent_groups = df.groupby('parent_name').agg({
            'created_at': lambda x: x.dropna().min() if not x.dropna().empty else None,
            'source_db': lambda x: x.iloc[0] if len(x) > 0 else None  # First occurrence source
        }).reset_index()
        
        # Generate stable UUIDs for parent products
        logger.info("   → Generating parent product IDs...")
        parent_groups['parent_product_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
        
        # Final canonical master table
        canonical_df = parent_groups[[
            'parent_product_id',
            'parent_name',
            'source_db',
            'created_at'
        ]].copy()
        
        # Rename source_db to source for clarity
        canonical_df = canonical_df.rename(columns={'source_db': 'source'})
        
        canonical_df = canonical_df.sort_values('parent_name').reset_index(drop=True)
        
        self.stats['parent_products'] = len(canonical_df)
        logger.info(f"   ✅ Created {len(canonical_df)} parent products")
        
        return canonical_df
    
    def upsert_canonical_master(self, canonical_df: pd.DataFrame, engine, db_name: str):
        """UPSERT canonical master table (no data loss)"""
        logger.info(f"\n💾 UPSERTING CANONICAL MASTER TO {db_name}")
        
        if canonical_df.empty:
            logger.warning(f"   ⚠️  No data to upsert to {db_name}")
            return
        
        try:
            with engine.begin() as conn:
                # Create temp table
                canonical_df.to_sql(
                    'canonical_products_master_temp',
                    conn,
                    if_exists='replace',
                    index=False
                )
                
                # UPSERT query
                conn.execute(text("""
                    INSERT INTO canonical_products_master 
                        (parent_product_id, parent_name, created_at)
                    SELECT parent_product_id, parent_name, created_at
                    FROM canonical_products_master_temp
                    ON CONFLICT (parent_product_id) 
                    DO UPDATE SET 
                        parent_name = EXCLUDED.parent_name,
                        updated_at = NOW()
                """))
                
                # Drop temp table
                conn.execute(text("DROP TABLE canonical_products_master_temp"))
            
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
        logger.info(f"   • Production PostgreSQL products: {self.stats['prod_pg_products']:,}")
        logger.info(f"   • Production PostgreSQL product_names: {self.stats['prod_pg_product_names']:,}")
        logger.info(f"   • Total products extracted: {self.stats['total_products']:,}")
        
        logger.info(f"\n🔄 Transformation:")
        logger.info(f"   • Mapped products: {self.stats['mapped_products']:,}")
        logger.info(f"   • Unmapped products: {self.stats['unmapped_products']:,}")
        logger.info(f"   • Parent products created: {self.stats['parent_products']:,}")
        
        mapping_rate = (self.stats['mapped_products'] / self.stats['total_products'] * 100) if self.stats['total_products'] > 0 else 0
        logger.info(f"   • Mapping coverage: {mapping_rate:.1f}%")
        
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
            df_prod_products, df_prod_names = self.extract_from_prod_postgres()
            
            # Step 3: Combine all data
            logger.info("\n🔗 COMBINING DATA FROM ALL SOURCES")
            combined_df = pd.concat([df_supabase, df_prod_products, df_prod_names], ignore_index=True)
            self.stats['total_products'] = len(combined_df)
            logger.info(f"   ✅ Combined {len(combined_df)} total products")
            
            # Step 4: Transform and standardize
            canonical_master = self.transform_and_standardize(combined_df)
            
            if canonical_master.empty:
                logger.error("❌ No canonical master data generated")
                return False
            
            # Step 5: Load canonical master to all databases
            logger.info("\n" + "=" * 80)
            logger.info("💾 LOADING CANONICAL MASTER TO ALL DATABASES")
            logger.info("=" * 80)
            
            self.upsert_canonical_master(canonical_master, self.supabase_engine, "Supabase PostgreSQL")
            self.upsert_canonical_master(canonical_master, self.prod_pg_engine, "Production PostgreSQL")
            self.upsert_canonical_master(canonical_master, self.hub_engine, "Local Hub")
            
            # Step 6: Update parent IDs in source tables
            logger.info("\n" + "=" * 80)
            logger.info("🔗 UPDATING PARENT IDs IN SOURCE TABLES")
            logger.info("=" * 80)
            
            self.update_parent_ids_batch(self.supabase_engine, 'products', 'Supabase')
            self.update_parent_ids_batch(self.prod_pg_engine, 'products', 'Production PostgreSQL')
            self.update_parent_ids_batch(self.prod_pg_engine, 'product_names', 'Production PostgreSQL')
            
            # Step 7: Generate report
            self.generate_report()
            
            return True
            
        except Exception as e:
            logger.error(f"\n❌ PIPELINE FAILED: {e}")
            import traceback
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
