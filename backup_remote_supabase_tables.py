#!/usr/bin/env python3
"""
Script to backup all 6 remote Supabase tables to local master database
Tables will be renamed with _backup suffix in local database
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def backup_remote_table_to_local(remote_engine, local_engine, remote_table_name):
    """Backup a single remote Supabase table to local database with _backup suffix"""
    
    local_table_name = f"{remote_table_name}_backup"
    
    print(f"\nBacking up remote table: {remote_table_name} -> local table: {local_table_name}")
    
    try:
        # Step 1: Get data from remote table
        print(f"  📥 Fetching data from remote {remote_table_name}...")
        
        with remote_engine.connect() as remote_conn:
            # Get all data from remote table
            remote_query = f"""
            SELECT * FROM public.{remote_table_name}
            ORDER BY id
            """
            remote_df = pd.read_sql(remote_query, remote_conn)
            
            if remote_df.empty:
                print(f"  📝 No data found in remote {remote_table_name}")
                return True
            
            print(f"  ✅ Fetched {len(remote_df)} records from remote {remote_table_name}")
            
            # Show sample data
            print(f"  📋 Sample data from {remote_table_name}:")
            sample_df = remote_df.head(3)
            for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                print(f"    {i}. ID: {row.get('id', 'N/A')}, Name: {row.get('product_name', 'N/A')}")
            
            if len(remote_df) > 3:
                print(f"    ... and {len(remote_df) - 3} more records")
        
        # Step 2: Save to local database with _backup suffix
        print(f"  💾 Saving to local table: {local_table_name}...")
        
        with local_engine.connect() as local_conn:
            # Drop existing backup table if it exists
            drop_query = f"DROP TABLE IF EXISTS public.{local_table_name} CASCADE"
            local_conn.execute(text(drop_query))
            local_conn.commit()
            print(f"    🗑️ Removed existing backup table if it existed")
            
            # Save data to local database
            remote_df.to_sql(
                name=local_table_name,
                con=local_engine,
                schema='public',
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            print(f"    ✅ Successfully saved {len(remote_df)} records to local {local_table_name}")
            
            # Verify backup
            verify_query = f"SELECT COUNT(*) as count FROM public.{local_table_name}"
            verify_df = pd.read_sql(verify_query, local_conn)
            backup_count = verify_df['count'].iloc[0]
            
            if backup_count == len(remote_df):
                print(f"    ✅ Verification: {backup_count} records backed up successfully")
            else:
                print(f"    ❌ Verification failed: Expected {len(remote_df)}, got {backup_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error backing up {remote_table_name}: {e}")
        return False

def show_backup_summary(local_engine, supabase_tables):
    """Show summary of all backup tables"""
    
    print(f"\n📊 BACKUP SUMMARY")
    print("=" * 50)
    
    try:
        with local_engine.connect() as conn:
            for table in supabase_tables:
                backup_table_name = f"{table}_backup"
                
                # Check if backup table exists
                check_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
                """
                exists_df = pd.read_sql(check_query, conn, params=(backup_table_name,))
                table_exists = exists_df.iloc[0, 0]
                
                if table_exists:
                    # Get record count
                    count_query = f"SELECT COUNT(*) as count FROM public.{backup_table_name}"
                    count_df = pd.read_sql(count_query, conn)
                    record_count = count_df['count'].iloc[0]
                    
                    # Get table structure
                    structure_query = """
                    SELECT column_name, data_type
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    AND table_schema = 'public'
                    ORDER BY ordinal_position
                    """
                    structure_df = pd.read_sql(structure_query, conn, params=(backup_table_name,))
                    
                    print(f"  ✅ {backup_table_name}:")
                    print(f"    Records: {record_count}")
                    print(f"    Columns: {len(structure_df)}")
                    print(f"    Columns: {', '.join(structure_df['column_name'].tolist())}")
                else:
                    print(f"  ❌ {backup_table_name}: Table not found")
                
                print()  # Empty line for readability
                
    except Exception as e:
        print(f"❌ Error showing backup summary: {e}")

def main():
    """Main function"""
    print("Backing up all 6 remote Supabase tables to local master database")
    print("Tables will be renamed with _backup suffix")
    print("=" * 70)
    
    try:
        # Step 1: Get database engines
        remote_engine = get_db_engine('supabase')
        local_engine = get_db_engine('hub')
        
        # Step 2: Get list of Supabase tables
        supabase_tables = settings.SUPABASE_TABLES
        print(f"Backing up {len(supabase_tables)} remote Supabase tables:")
        for table in supabase_tables:
            print(f"  - {table} -> {table}_backup")
        
        print(f"\nRemote table names from settings.py:")
        print(f"  - farm_prices -> farm_prices_backup")
        print(f"  - supermarket_prices -> supermarket_prices_backup") 
        print(f"  - distribution_center_prices -> distribution_center_prices_backup")
        print(f"  - local_shop_prices -> local_shop_prices_backup")
        print(f"  - ecommerce_prices -> ecommerce_prices_backup")
        print(f"  - sunday_market_prices -> sunday_market_prices_backup")
        
        # Step 3: Backup each table
        successful_backups = []
        failed_backups = []
        
        for table in supabase_tables:
            success = backup_remote_table_to_local(remote_engine, local_engine, table)
            if success:
                successful_backups.append(table)
            else:
                failed_backups.append(table)
        
        # Step 4: Show backup summary
        show_backup_summary(local_engine, supabase_tables)
        
        # Step 5: Final summary
        print(f"🎯 BACKUP COMPLETION SUMMARY")
        print("=" * 50)
        print(f"  Successfully backed up: {len(successful_backups)} tables")
        print(f"  Failed backups: {len(failed_backups)} tables")
        
        if successful_backups:
            print(f"  ✅ Successful backups:")
            for table in successful_backups:
                print(f"    - {table} -> {table}_backup")
        
        if failed_backups:
            print(f"  ❌ Failed backups:")
            for table in failed_backups:
                print(f"    - {table}")
        
        print(f"\n🏁 Remote Supabase tables backup completed!")
        print(f"📁 All backup tables are stored in your local master database")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
