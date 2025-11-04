#!/usr/bin/env python3
"""
Script to analyze all 6 Supabase tables and show their variable fields (columns)
This will help understand the structure of your remote Supabase data
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_supabase_engine():
    """Create database engine for remote Supabase"""
    supabase_url = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
    return create_engine(supabase_url)

def analyze_table_structure(engine, table_name):
    """Analyze the structure of a single table"""
    
    print(f"\n🔍 Analyzing table: {table_name}")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            # Get table columns and their data types
            columns_query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            ORDER BY ordinal_position;
            """
            
            columns_df = pd.read_sql(columns_query, conn)
            
            if columns_df.empty:
                print(f"❌ Table {table_name} not found or no columns!")
                return None
            
            print(f"📊 Found {len(columns_df)} columns:")
            print("-" * 50)
            
            for _, row in columns_df.iterrows():
                col_name = row['column_name']
                data_type = row['data_type']
                is_nullable = row['is_nullable']
                max_length = row['character_maximum_length']
                default_val = row['column_default']
                
                # Format the output nicely
                nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
                length_str = f"({max_length})" if max_length else ""
                default_str = f" DEFAULT {default_val}" if default_val else ""
                
                print(f"   📋 {col_name:<25} {data_type}{length_str:<15} {nullable_str}{default_str}")
            
            # Get record count
            count_query = f"SELECT COUNT(*) FROM public.{table_name}"
            result = conn.execute(text(count_query))
            record_count = result.scalar()
            print(f"\n📈 Total records: {record_count:,}")
            
            # Get sample data (first 3 records)
            sample_query = f"SELECT * FROM public.{table_name} LIMIT 3"
            sample_df = pd.read_sql(sample_query, conn)
            
            if not sample_df.empty:
                print(f"\n📋 Sample data (first 3 records):")
                print("-" * 50)
                for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                    print(f"   Record {i}:")
                    for col in sample_df.columns:
                        value = str(row[col])[:50] + "..." if len(str(row[col])) > 50 else str(row[col])
                        print(f"     {col}: {value}")
                    print()
            
            return {
                'table_name': table_name,
                'columns': columns_df,
                'record_count': record_count,
                'sample_data': sample_df
            }
            
    except Exception as e:
        print(f"❌ Error analyzing {table_name}: {e}")
        return None

def compare_tables_structures(tables_data):
    """Compare the structures of all tables"""
    
    print(f"\n🔍 Comparing table structures...")
    print("=" * 60)
    
    # Get all unique columns across tables
    all_columns = set()
    for table_data in tables_data:
        if table_data:
            for col in table_data['columns']['column_name']:
                all_columns.add(col)
    
    print(f"📊 Found {len(all_columns)} unique columns across all tables:")
    print("-" * 60)
    
    # Show which tables have which columns
    for col in sorted(all_columns):
        tables_with_col = []
        for table_data in tables_data:
            if table_data and col in table_data['columns']['column_name'].values:
                tables_with_col.append(table_data['table_name'])
        
        print(f"   📋 {col:<30} → {', '.join(tables_with_col)}")
    
    # Show common columns (present in all tables)
    common_columns = set()
    if tables_data:
        common_columns = set(tables_data[0]['columns']['column_name'])
        for table_data in tables_data[1:]:
            if table_data:
                common_columns = common_columns.intersection(set(table_data['columns']['column_name']))
    
    print(f"\n📊 Common columns (present in all tables):")
    print("-" * 60)
    for col in sorted(common_columns):
        print(f"   ✅ {col}")
    
    # Show unique columns (present in only one table)
    unique_columns = {}
    for table_data in tables_data:
        if table_data:
            table_name = table_data['table_name']
            table_columns = set(table_data['columns']['column_name'])
            unique_cols = table_columns - common_columns
            for col in unique_cols:
                if col not in unique_columns:
                    unique_columns[col] = []
                unique_columns[col].append(table_name)
    
    print(f"\n📊 Unique columns (present in only one table):")
    print("-" * 60)
    for col, tables in unique_columns.items():
        if len(tables) == 1:
            print(f"   🔸 {col:<30} → {tables[0]}")

def show_data_types_summary(tables_data):
    """Show summary of data types across all tables"""
    
    print(f"\n📊 Data Types Summary:")
    print("=" * 60)
    
    data_types = {}
    for table_data in tables_data:
        if table_data:
            for _, row in table_data['columns'].iterrows():
                col_name = row['column_name']
                data_type = row['data_type']
                
                if col_name not in data_types:
                    data_types[col_name] = set()
                data_types[col_name].add(data_type)
    
    for col_name in sorted(data_types.keys()):
        types = ', '.join(sorted(data_types[col_name]))
        print(f"   📋 {col_name:<30} → {types}")

def main():
    """Main function"""
    print("🚀 Analyzing Supabase Tables Structure")
    print("=" * 60)
    print("📥 Source: Remote Supabase Database")
    print("🎯 Goal: Show all variable fields (columns) for all 6 tables")
    print("=" * 60)
    
    try:
        # Create Supabase connection
        engine = get_supabase_engine()
        
        # Tables to analyze
        tables = [
            "farm_prices", "supermarket_prices", "distribution_center_prices",
            "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
        ]
        
        tables_data = []
        
        # Analyze each table
        for table in tables:
            table_data = analyze_table_structure(engine, table)
            tables_data.append(table_data)
        
        # Compare structures
        compare_tables_structures(tables_data)
        
        # Show data types summary
        show_data_types_summary(tables_data)
        
        print(f"\n✅ Analysis completed successfully!")
        print(f"📊 Analyzed {len(tables)} tables")
        print(f"🎯 You now have a complete understanding of your Supabase table structures!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
