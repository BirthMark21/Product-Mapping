#!/usr/bin/env python3
"""
Fetch 2-month data (June 20, 2025 – August 20, 2025)
from selected Supabase tables and export to CSV files
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# ---------------------------------------------------------
# 1️⃣  Create Supabase Connection
# ---------------------------------------------------------
def get_supabase_engine():
    """Create Supabase database engine"""
    conn_str = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
    return create_engine(conn_str)

# ---------------------------------------------------------
# 2️⃣  Fetch data for the given date range
# ---------------------------------------------------------
def fetch_table_data(engine, table_name, start_date, end_date):
    """Fetch filtered data from Supabase for a specific table"""
    print(f"\n🔍 Fetching data from {table_name} between {start_date} → {end_date}")
    query = text(f"""
        SELECT * 
        FROM public.{table_name}
        WHERE date BETWEEN :start_date AND :end_date
        ORDER BY date ASC
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})
        print(f"✅ {len(df)} records fetched from {table_name}")
        return df
    except Exception as e:
        print(f"❌ Error fetching {table_name}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 3️⃣  Main Export Function
# ---------------------------------------------------------
def export_to_csv():
    """Fetch 2-month data and export to chipoai folder"""
    print("🚀 Starting 2-Month Data Fetch from Supabase")
    print("=" * 70)
    print("📅 Period: June 20, 2025 → August 20, 2025")
    
    start_date = datetime(2025, 6, 20)
    end_date = datetime(2025, 8, 20)

    # Create engine
    engine = get_supabase_engine()

    # Tables to fetch
    tables = [
        "supermarket_prices",
        "distribution_center_prices",
        "local_shop_prices"
    ]

    # Create export folder if missing
    export_folder = "chipoai_exports"
    os.makedirs(export_folder, exist_ok=True)
    print(f"📁 Export folder: {export_folder}")

    for table_name in tables:
        df = fetch_table_data(engine, table_name, start_date, end_date)
        if not df.empty:
            # Save to CSV with pretty formatting
            export_path = os.path.join(export_folder, f"{table_name}_2025-06-20_to_2025-08-20.csv")
            df.to_csv(export_path, index=False)
            print(f"📤 Exported → {export_path}")
        else:
            print(f"⚠️ No data found for {table_name} in this range.")

    print("\n🎯 All exports completed successfully!")
    print(f"✅ Files are saved in the '{export_folder}' folder.")

# ---------------------------------------------------------
# 4️⃣  Run
# ---------------------------------------------------------
if __name__ == "__main__":
    export_to_csv()
