#!/usr/bin/env python3
"""
Script to fetch all products from Supabase tables with source information
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def fetch_supabase_products():
    """Fetch all products from all Supabase tables with source information"""
    
    print("Fetching all products from Supabase tables...")
    
    try:
        supabase_engine = get_db_engine('supabase')
        supabase_tables = settings.SUPABASE_TABLES
        
        all_products = []
        
        for table in supabase_tables:
            print(f"\nFetching from table: {table}")
            
            try:
                query = f"""
                SELECT 
                    id::text AS product_id,
                    product_name,
                    created_at,
                    '{table}' AS source_table,
                    'Supabase' AS source_type,
                    'postgres' AS database
                FROM {settings.SUPABASE_SCHEMA}.{table}
                ORDER BY product_name
                """
                
                with supabase_engine.connect() as conn:
                    df = pd.read_sql(query, conn)
                    
                    if not df.empty:
                        print(f"  Found {len(df)} products in {table}")
                        
                        # Show sample data
                        sample_df = df.head(3)
                        for _, row in sample_df.iterrows():
                            print(f"    - {row['product_name']} (ID: {row['product_id']})")
                        
                        if len(df) > 3:
                            print(f"    ... and {len(df) - 3} more products")
                        
                        all_products.append(df)
                    else:
                        print(f"  No products found in {table}")
                        
            except Exception as e:
                print(f"  Error fetching from {table}: {e}")
        
        if all_products:
            # Combine all Supabase products
            combined_df = pd.concat(all_products, ignore_index=True)
            
            print(f"\nTotal Supabase products: {len(combined_df)}")
            
            # Export to CSV
            csv_filename = "supabase_products.csv"
            combined_df.to_csv(csv_filename, index=False)
            print(f"Exported to {csv_filename}")
            
            # Show statistics
            print(f"\nSupabase Statistics:")
            print(f"  Total products: {len(combined_df)}")
            print(f"  Products with names: {len(combined_df[combined_df['product_name'].notna()])}")
            print(f"  Products with empty names: {len(combined_df[combined_df['product_name'].isna() | (combined_df['product_name'] == '')])}")
            print(f"  Products with name '0': {len(combined_df[combined_df['product_name'] == '0'])}")
            
            # Show breakdown by table
            print(f"\nBreakdown by table:")
            table_counts = combined_df['source_table'].value_counts()
            for table, count in table_counts.items():
                print(f"  {table}: {count} products")
            
            return combined_df
        else:
            print("No products found in any Supabase table")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error fetching Supabase products: {e}")
        return pd.DataFrame()

def fetch_individual_supabase_tables():
    """Fetch products from each Supabase table separately"""
    
    print("\n" + "="*60)
    print("INDIVIDUAL SUPABASE TABLE ANALYSIS")
    print("="*60)
    
    try:
        supabase_engine = get_db_engine('supabase')
        supabase_tables = settings.SUPABASE_TABLES
        
        for table in supabase_tables:
            print(f"\n--- Table: {table} ---")
            
            try:
                query = f"""
                SELECT 
                    id::text AS product_id,
                    product_name,
                    created_at,
                    COUNT(*) OVER() as total_count
                FROM {settings.SUPABASE_SCHEMA}.{table}
                ORDER BY product_name
                LIMIT 10
                """
                
                with supabase_engine.connect() as conn:
                    df = pd.read_sql(query, conn)
                    
                    if not df.empty:
                        total_count = df['total_count'].iloc[0]
                        print(f"Total products in {table}: {total_count}")
                        
                        # Show sample products
                        print("Sample products:")
                        for _, row in df.iterrows():
                            print(f"  - {row['product_name']} (ID: {row['product_id']})")
                        
                        # Check for '0' products
                        zero_query = f"""
                        SELECT COUNT(*) as zero_count
                        FROM {settings.SUPABASE_SCHEMA}.{table}
                        WHERE product_name = '0'
                        """
                        zero_df = pd.read_sql(zero_query, conn)
                        zero_count = zero_df['zero_count'].iloc[0]
                        
                        if zero_count > 0:
                            print(f"  Products with name '0': {zero_count}")
                            
                            # Get the actual '0' products
                            zero_products_query = f"""
                            SELECT id::text AS product_id, product_name, created_at
                            FROM {settings.SUPABASE_SCHEMA}.{table}
                            WHERE product_name = '0'
                            """
                            zero_products_df = pd.read_sql(zero_products_query, conn)
                            print("  '0' products:")
                            for _, row in zero_products_df.iterrows():
                                print(f"    - ID: {row['product_id']}, Created: {row['created_at']}")
                    else:
                        print(f"No products found in {table}")
                        
            except Exception as e:
                print(f"Error analyzing {table}: {e}")
                
    except Exception as e:
        print(f"Error in individual table analysis: {e}")

def main():
    """Main function"""
    print("Supabase Products Fetcher")
    print("=" * 50)
    
    # Fetch all Supabase products
    df = fetch_supabase_products()
    
    # Analyze individual tables
    fetch_individual_supabase_tables()
    
    if not df.empty:
        print(f"\nSuccessfully processed {len(df)} Supabase products")
    else:
        print("\nNo Supabase products found")

if __name__ == "__main__":
    main()
