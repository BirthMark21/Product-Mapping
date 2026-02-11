#!/usr/bin/env python3
"""
Script to sync remote Supabase local_shop_prices table to local PostgreSQL database
Reads from remote Supabase and writes to local master-db database with table name 'local_shop_supa'
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys
import traceback
from datetime import datetime

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__)))
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

# Table names
REMOTE_TABLE = 'local_shop_prices'  # Remote Supabase table
LOCAL_TABLE = 'local-shop-supa'     # Local PostgreSQL table name (with hyphens, will be quoted)
LOCAL_DB_NAME = 'master-db'         # Local database name

def get_local_db_engine():
    """Create database engine for local PostgreSQL master-db"""
    # Use HUB_PG env variables from .env file (lines 23-27 as mentioned)
    host = os.getenv('HUB_PG_HOST', 'localhost')
    port = os.getenv('HUB_PG_PORT', '5432')
    db_name = os.getenv('HUB_PG_DB_NAME', 'master-db')
    user = os.getenv('HUB_PG_USER', 'postgres')
    password = os.getenv('HUB_PG_PASSWORD')
    
    if not password:
        print("⚠️  Warning: HUB_PG_PASSWORD not found in .env file")
        print("   Using default connection settings...")
    
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(conn_str)

def fetch_from_remote_supabase(remote_engine):
    """Fetch all data from remote Supabase local_shop_prices table"""
    print(f"\n📥 Fetching data from remote Supabase table: {REMOTE_TABLE}")
    print("-" * 70)
    
    try:
        with remote_engine.connect() as conn:
            query = f"""
            SELECT 
                id,
                product_name,
                price,
                location,
                product_remark,
                date,
                created_at,
                updated_at,
                created_by
            FROM public.{REMOTE_TABLE}
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print(f"❌ No data found in remote table {REMOTE_TABLE}")
                return pd.DataFrame()
            
            print(f"✅ Successfully fetched {len(df):,} records from remote Supabase")
            print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"   Unique products: {df['product_name'].nunique()}")
            print(f"   Unique locations: {df['location'].nunique()}")
            
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def create_local_table(local_engine, table_name):
    """Create local table with same structure as remote table"""
    print(f"\n🔨 Creating local table: {table_name}")
    print("-" * 70)
    
    try:
        with local_engine.connect() as conn:
            # Drop existing table if it exists (quote table name if it has hyphens)
            table_name_quoted = f'"{table_name}"' if '-' in table_name else table_name
            drop_query = text(f'DROP TABLE IF EXISTS public.{table_name_quoted} CASCADE')
            conn.execute(drop_query)
            conn.commit()
            print(f"   ✅ Dropped existing table if it existed")
            
            # Create table with same structure as remote Supabase table
            create_query = text(f"""
            CREATE TABLE public.{table_name_quoted} (
                id UUID PRIMARY KEY,
                product_name TEXT NOT NULL,
                price NUMERIC(10,2),
                location TEXT,
                product_remark TEXT,
                date DATE,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                created_by UUID
            );
            """)
            
            conn.execute(create_query)
            conn.commit()
            print(f"   ✅ Created table '{table_name}' successfully")
            
            # Create indexes for better query performance
            # Use sanitized table name for index names (no hyphens)
            index_table_name = table_name.replace('-', '_')
            indexes = [
                f'CREATE INDEX IF NOT EXISTS idx_{index_table_name}_product_name ON public.{table_name_quoted}(product_name);',
                f'CREATE INDEX IF NOT EXISTS idx_{index_table_name}_location ON public.{table_name_quoted}(location);',
                f'CREATE INDEX IF NOT EXISTS idx_{index_table_name}_date ON public.{table_name_quoted}(date);'
            ]
            
            for idx_query in indexes:
                try:
                    conn.execute(text(idx_query))
                    conn.commit()
                except Exception as e:
                    print(f"   ⚠️  Warning: Could not create index: {e}")
            
            print(f"   ✅ Created indexes for table '{table_name}'")
            
            return True
            
    except Exception as e:
        print(f"❌ Error creating local table: {e}")
        traceback.print_exc()
        return False

def insert_data_to_local(local_engine, df, table_name, batch_size=1000):
    """Insert data into local PostgreSQL table in batches"""
    print(f"\n💾 Inserting data into local table: {table_name}")
    print("-" * 70)
    
    if df.empty:
        print("❌ No data to insert")
        return False
    
    try:
        total_records = len(df)
        num_batches = (total_records + batch_size - 1) // batch_size
        
        print(f"   Total records: {total_records:,}")
        print(f"   Batch size: {batch_size}")
        print(f"   Number of batches: {num_batches}")
        
        inserted_count = 0
        
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_records)
            batch_df = df.iloc[start_idx:end_idx]
            
            try:
                # pandas to_sql handles quoted table names automatically
                # But we need to ensure the table exists first (it should from create_local_table)
                batch_df.to_sql(
                    name=table_name,
                    con=local_engine,
                    schema='public',
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=batch_size
                )
                
                inserted_count += len(batch_df)
                print(f"   ✅ Batch {batch_num + 1}/{num_batches}: Inserted {len(batch_df):,} records "
                      f"({inserted_count:,}/{total_records:,} total)")
                
            except Exception as e:
                print(f"   ❌ Batch {batch_num + 1}/{num_batches}: Error - {str(e)[:100]}")
                traceback.print_exc()
        
        print(f"\n✅ Successfully inserted {inserted_count:,} out of {total_records:,} records")
        return inserted_count == total_records
        
    except Exception as e:
        print(f"❌ Error during insertion: {e}")
        traceback.print_exc()
        return False

def verify_local_table(local_engine, table_name):
    """Verify that data was inserted correctly"""
    print(f"\n🔍 Verifying local table: {table_name}")
    print("-" * 70)
    
    try:
        with local_engine.connect() as conn:
            # Quote table name if it has hyphens
            table_name_quoted = f'"{table_name}"' if '-' in table_name else table_name
            
            # Get total count
            count_query = text(f'SELECT COUNT(*) FROM public.{table_name_quoted}')
            result = conn.execute(count_query)
            total_count = result.scalar()
            
            print(f"✅ Total records in local table: {total_count:,}")
            
            # Get statistics
            stats_query = text(f"""
            SELECT 
                COUNT(DISTINCT product_name) as unique_products,
                COUNT(DISTINCT location) as unique_locations,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price
            FROM public.{table_name_quoted}
            """)
            
            stats_df = pd.read_sql(stats_query, conn)
            
            if not stats_df.empty:
                stats = stats_df.iloc[0]
                print(f"\n📊 Table Statistics:")
                print(f"   Unique products: {int(stats['unique_products']):,}")
                print(f"   Unique locations: {int(stats['unique_locations']):,}")
                print(f"   Date range: {stats['earliest_date']} to {stats['latest_date']}")
                print(f"   Price range: ${float(stats['min_price']):.2f} - ${float(stats['max_price']):.2f}")
                print(f"   Average price: ${float(stats['avg_price']):.2f}")
            
            # Get sample records
            sample_query = text(f"""
            SELECT id, product_name, price, location, date
            FROM public.{table_name_quoted}
            ORDER BY created_at DESC
            LIMIT 5
            """)
            
            sample_df = pd.read_sql(sample_query, conn)
            
            if not sample_df.empty:
                print(f"\n📋 Sample records (most recent):")
                print("-" * 70)
                for idx, row in sample_df.iterrows():
                    try:
                        product = str(row['product_name'])[:40]
                        location = str(row['location'])[:35] if pd.notna(row['location']) else 'N/A'
                        print(f"   {product:40s} | ${row['price']:>8.2f} | {location:35s} | {row['date']}")
                    except:
                        print(f"   {str(row['product_name'])[:40]:40s} | ${row['price']:>8.2f}")
            
            return True
            
    except Exception as e:
        print(f"❌ Error verifying local table: {e}")
        traceback.print_exc()
        return False

def main():
    """Main function"""
    print("=" * 70)
    print("🚀 SYNC REMOTE SUPABASE LOCAL_SHOP_PRICES TO LOCAL POSTGRESQL")
    print("=" * 70)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Remote table: {REMOTE_TABLE}")
    print(f"📊 Local table: {LOCAL_TABLE}")
    print(f"📊 Local database: {LOCAL_DB_NAME}")
    print("=" * 70)
    
    try:
        # Step 1: Connect to remote Supabase
        print("\n🔌 Connecting to remote Supabase...")
        remote_engine = get_db_engine('supabase')
        print("✅ Connected to remote Supabase")
        
        # Step 2: Connect to local PostgreSQL
        print("\n🔌 Connecting to local PostgreSQL...")
        local_engine = get_local_db_engine()
        print("✅ Connected to local PostgreSQL")
        
        # Step 3: Fetch data from remote Supabase
        df = fetch_from_remote_supabase(remote_engine)
        if df.empty:
            print("\n❌ Cannot proceed - no data fetched from remote")
            return
        
        # Step 4: Show summary
        print(f"\n📊 Data Summary:")
        print("-" * 70)
        print(f"   Total records: {len(df):,}")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
        
        # Step 5: Create local table
        if not create_local_table(local_engine, LOCAL_TABLE):
            print("\n❌ Cannot proceed - failed to create local table")
            return
        
        # Step 6: Insert data
        success = insert_data_to_local(local_engine, df, LOCAL_TABLE)
        
        if not success:
            print("\n⚠️  Some records may not have been inserted. Check errors above.")
        
        # Step 7: Verify insertion
        verify_local_table(local_engine, LOCAL_TABLE)
        
        print("\n" + "=" * 70)
        print("✅ SYNC COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"📅 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Remote source: Supabase {REMOTE_TABLE}")
        print(f"📊 Local destination: PostgreSQL {LOCAL_DB_NAME}.{LOCAL_TABLE}")
        print(f"📊 Total records synced: {len(df):,}")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error in main process: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

