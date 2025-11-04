# --- PASTE ALL OF THIS INTO ONE CELL AND RUN IT ---

#!/usr/bin/env python3
"""
Script to fetch all 6 tables from Supabase and export them as CSV files
into a local folder named 'data_dynamic_suba'.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
from config.setting import *

def get_supabase_engine():
    """Create Supabase database engine"""
    conn_str = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"
    return create_engine(conn_str)

def fetch_and_export_tables_to_csv():
    """Fetch all 6 tables from Supabase and export them to a local folder"""
    
    print("🔄 Connecting to Supabase database...")
    supabase_engine = get_supabase_engine()
    
    # Define the output folder
    output_folder = "data_dynamic_suba"
    
    # Create the output folder if it doesn't exist
    try:
        os.makedirs(output_folder, exist_ok=True)
        print(f"📂 Output folder '{output_folder}' is ready.")
    except Exception as e:
        print(f"❌ Error creating directory '{output_folder}': {e}")
        return

    # List of the 6 tables to fetch
    tables = [
        "farm_prices",
        "supermarket_prices", 
        "distribution_center_prices",
        "local_shop_prices",
        "ecommerce_prices",
        "sunday_market_prices"
    ]
    
    print(f"\n📋 Fetching {len(tables)} tables from Supabase to export as CSVs...")
    
    exported_files = []

    for table_name in tables:
        try:
            print(f"\n🔄 Processing table: {table_name}")
            
            # Step 1: Fetch data from Supabase
            print(f"   📥 Fetching data from Supabase...")
            with supabase_engine.connect() as conn:
                query = f"SELECT * FROM public.{table_name}"
                df = pd.read_sql(query, conn)
                print(f"   ✅ Fetched {len(df)} records from Supabase")
            
            if df.empty:
                print(f"   ⚠️  Table '{table_name}' is empty in Supabase, skipping export...")
                continue
            
            # Step 2: Export the DataFrame to a CSV file
            csv_file_path = os.path.join(output_folder, f"{table_name}.csv")
            print(f"   📤 Exporting {len(df)} records to '{csv_file_path}'...")
            
            df.to_csv(csv_file_path, index=False)
            
            print(f"   ✅ Successfully exported '{table_name}.csv'")
            exported_files.append(f"{table_name}.csv")
            
        except Exception as e:
            print(f"   ❌ Error processing {table_name}: {e}")
            continue
    
    print(f"\n🎉 Data export completed!")
    print(f"📊 Summary:")
    
    # Show summary of exported files
    if exported_files:
        print(f"   The following {len(exported_files)} files have been created in the '{output_folder}' directory:")
        for file in exported_files:
            print(f"   - {file}")
    else:
        print("   No files were exported. Please check the logs for errors.")

def main():
    """Main function"""
    print("🚀 Starting Supabase to Local CSV Export")
    print("=" * 60)
    print("📥 Source: Supabase (aws-0-eu-central-1.pooler.supabase.com)")
    print(f"📤 Destination: Local folder 'data_dynamic_suba'")
    print("=" * 60)
    
    try:
        fetch_and_export_tables_to_csv()
        print("\n✅ Export completed successfully!")
        print("🎯 Your 'data_dynamic_suba' folder now has all the Supabase data as CSV files!")
        
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        print("Please check your database connection and file permissions.")

if __name__ == "__main__":
    main()