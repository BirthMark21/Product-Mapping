#!/usr/bin/env python3
"""
Script to fetch all 6 Supabase tables and create them locally in hub database
This script will:
1. Drop existing local tables if they exist
2. Fetch all data from remote Supabase tables
3. Create local tables in hub database
4. Insert all data with proper error handling
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys

# Add the parent directory to the path so we can import config and utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

# List of all 6 Supabase tables to fetch
SUPABASE_TABLES = [
    "farm_prices",
    "supermarket_prices", 
    "distribution_center_prices",
    "local_shop_prices",
    "ecommerce_prices",
    "sunday_market_prices"
]

def drop_existing_local_tables():
    """Drop all existing local tables if they exist"""
    
    print("🗑️ Dropping existing local tables...")
    print("=" * 50)
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            for table in SUPABASE_TABLES:
                try:
                    drop_query = f"DROP TABLE IF EXISTS public.{table} CASCADE"
                    conn.execute(text(drop_query))
                    conn.commit()
                    print(f"  ✅ Dropped {table} table if it existed")
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not drop {table}: {e}")
            
            print(f"✅ Successfully dropped all existing local tables")
            
    except Exception as e:
        print(f"❌ Error dropping existing tables: {e}")
        raise

def get_table_structure(table_name):
    """Get the table structure from remote Supabase table"""
    
    print(f"🔍 Getting table structure for {table_name}...")
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        if not supabase_engine:
            print(f"❌ Failed to get Supabase database engine for {table_name}")
            return None
        
        with supabase_engine.connect() as conn:
            # Get column information
            structure_query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            structure_df = pd.read_sql(structure_query, conn)
            
            if structure_df.empty:
                print(f"❌ Could not get structure for {table_name}")
                return None
            
            print(f"✅ Found {len(structure_df)} columns in {table_name}")
            return structure_df
            
    except Exception as e:
        print(f"❌ Error getting table structure for {table_name}: {e}")
        return None

def fetch_table_from_supabase(table_name):
    """Fetch all data from a specific Supabase table"""
    
    print(f"📥 Fetching data from remote Supabase {table_name} table...")
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        if not supabase_engine:
            print(f"❌ Failed to get Supabase database engine for {table_name}")
            return pd.DataFrame()
        
        with supabase_engine.connect() as conn:
            # Fetch all columns (SELECT * to get exact structure)
            query = f"SELECT * FROM public.{table_name} ORDER BY id"
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print(f"⚠️  No data found in remote Supabase {table_name} table")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} records in remote Supabase {table_name} table")
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase {table_name}: {e}")
        print("Trying to reconnect...")
        try:
            # Retry with a fresh connection
            supabase_engine = get_db_engine('supabase')
            with supabase_engine.connect() as conn:
                df = pd.read_sql(query, conn)
                if not df.empty:
                    print(f"✅ Retry successful: Found {len(df)} records in {table_name}")
                    return df
        except Exception as retry_e:
            print(f"❌ Retry also failed for {table_name}: {retry_e}")
        return pd.DataFrame()

def create_local_table(table_name, products_df, table_structure):
    """Create a local table with exact same structure as remote table"""
    
    print(f"💾 Creating local table {table_name} with exact remote structure...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Build CREATE TABLE statement based on remote structure
            create_columns = []
            for _, col in table_structure.iterrows():
                col_name = col['column_name']
                data_type = col['data_type']
                is_nullable = col['is_nullable'] == 'YES'
                column_default = col['column_default']
                char_max_length = col['character_maximum_length']
                
                # Map PostgreSQL data types
                if data_type == 'character varying':
                    if char_max_length:
                        pg_type = f"VARCHAR({char_max_length})"
                    else:
                        pg_type = "TEXT"
                elif data_type == 'timestamp with time zone':
                    pg_type = "TIMESTAMP WITH TIME ZONE"
                elif data_type == 'timestamp without time zone':
                    pg_type = "TIMESTAMP"
                elif data_type == 'integer':
                    pg_type = "INTEGER"
                elif data_type == 'bigint':
                    pg_type = "BIGINT"
                elif data_type == 'boolean':
                    pg_type = "BOOLEAN"
                elif data_type == 'numeric':
                    pg_type = "NUMERIC"
                elif data_type == 'text':
                    pg_type = "TEXT"
                elif data_type == 'uuid':
                    pg_type = "UUID"
                else:
                    pg_type = "TEXT"  # Default fallback
                
                # Build column definition
                col_def = f"{col_name} {pg_type}"
                
                # Add NOT NULL constraint
                if not is_nullable:
                    col_def += " NOT NULL"
                
                # Add default value if exists
                if column_default and column_default != 'NULL':
                    col_def += f" DEFAULT {column_default}"
                
                create_columns.append(col_def)
            
            # Create the table
            create_query = f"CREATE TABLE public.{table_name} (\n    " + ",\n    ".join(create_columns) + "\n);"
            
            conn.execute(text(create_query))
            conn.commit()
            print(f"  ✅ Created {table_name} table with exact remote structure")
            
            if products_df.empty:
                print(f"  ⚠️  No data to insert for {table_name}")
                return True
            
            # Insert data using pandas to_sql for better compatibility
            print(f"  📝 Inserting {len(products_df)} records into {table_name}...")
            
            # Use pandas to_sql for data insertion
            products_df.to_sql(
                name=table_name,
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
            
            print(f"  ✅ Successfully inserted {len(products_df)} records to {table_name}")
            
            # Verify insertion
            verify_query = f"SELECT COUNT(*) as count FROM public.{table_name}"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(products_df):
                print(f"  ✅ Verification: {inserted_count} records inserted successfully in {table_name}")
            else:
                print(f"  ❌ Verification failed for {table_name}: Expected {len(products_df)}, got {inserted_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating local table {table_name}: {e}")
        return False

def export_table_to_csv(table_name, products_df):
    """Export table data to CSV file"""
    
    if products_df.empty:
        print(f"  ⚠️  No data to export for {table_name}")
        return
    
    try:
        csv_filename = f"{table_name}_products.csv"
        products_df.to_csv(csv_filename, index=False)
        print(f"  📄 Exported {len(products_df)} products to {csv_filename}")
        
        # Also export unique products only
        unique_products = products_df.drop_duplicates(subset=['product_name'])
        unique_csv_filename = f"{table_name}_unique_products.csv"
        unique_products.to_csv(unique_csv_filename, index=False)
        print(f"  📄 Exported {len(unique_products)} unique products to {unique_csv_filename}")
        
    except Exception as e:
        print(f"  ❌ Error exporting {table_name} to CSV: {e}")

def process_single_table(table_name):
    """Process a single table: fetch structure, fetch data, create local table, and export"""
    
    print(f"\n🔄 Processing {table_name}...")
    print("-" * 40)
    
    try:
        # Step 1: Get table structure from remote Supabase
        table_structure = get_table_structure(table_name)
        if table_structure is None:
            print(f"❌ Could not get table structure for {table_name}")
            return False
        
        # Step 2: Fetch data from remote Supabase
        products_df = fetch_table_from_supabase(table_name)
        if products_df.empty:
            print(f"⚠️  No data found for {table_name}, creating empty table with correct structure")
            # Create empty DataFrame with correct column structure
            columns = table_structure['column_name'].tolist()
            products_df = pd.DataFrame(columns=columns)
        
        # Step 3: Create local table with exact remote structure
        success = create_local_table(table_name, products_df, table_structure)
        if not success:
            print(f"❌ Failed to create local table for {table_name}")
            return False
        
        # Step 4: Export to CSV
        export_table_to_csv(table_name, products_df)
        
        print(f"✅ Successfully processed {table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error processing {table_name}: {e}")
        return False

def show_summary(results):
    """Show summary of all processed tables"""
    
    print(f"\n📊 PROCESSING SUMMARY")
    print("=" * 50)
    
    successful_tables = [table for table, success in results.items() if success]
    failed_tables = [table for table, success in results.items() if not success]
    
    print(f"✅ Successfully processed: {len(successful_tables)} tables")
    for table in successful_tables:
        print(f"   - {table}")
    
    if failed_tables:
        print(f"❌ Failed to process: {len(failed_tables)} tables")
        for table in failed_tables:
            print(f"   - {table}")
    
    print(f"\n📁 Local database tables created:")
    for table in successful_tables:
        print(f"   - public.{table}")
    
    print(f"\n📄 CSV files created:")
    for table in successful_tables:
        print(f"   - {table}_products.csv")
        print(f"   - {table}_unique_products.csv")

def main():
    """Main function to process all Supabase tables"""
    
    print("🚀 CREATE ALL TABLES FROM SUPABASE")
    print("=" * 60)
    print("📥 Source: Remote Supabase (6 tables)")
    print("🎯 Goal: Create local tables in hub database")
    print("=" * 60)
    
    try:
        # Step 1: Drop existing local tables
        drop_existing_local_tables()
        
        # Step 2: Process each table
        results = {}
        for table_name in SUPABASE_TABLES:
            results[table_name] = process_single_table(table_name)
        
        # Step 3: Show summary
        show_summary(results)
        
        # Final status
        successful_count = sum(1 for success in results.values() if success)
        total_count = len(SUPABASE_TABLES)
        
        if successful_count == total_count:
            print(f"\n🎯 ALL TABLES CREATED SUCCESSFULLY!")
            print(f"📊 Processed: {successful_count}/{total_count} tables")
            print(f"🔗 Source: Remote Supabase (6 tables)")
            print(f"💾 Destination: Local hub database")
        else:
            print(f"\n⚠️  PARTIAL SUCCESS!")
            print(f"📊 Processed: {successful_count}/{total_count} tables")
            print(f"❌ Some tables failed to process")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
