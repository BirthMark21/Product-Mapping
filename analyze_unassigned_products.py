#!/usr/bin/env python3
"""
Script to analyze products that are not assigned parent names
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def analyze_unassigned_products():
    """Analyze products that don't have parent assignments"""
    
    print("Analyzing products without parent assignments...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Check if verification table exists
            table_check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'product_verification_complete'
            );
            """
            table_exists = pd.read_sql(table_check_query, conn).iloc[0, 0]
            
            if not table_exists:
                print("Verification table does not exist. Please run create_complete_verification_table.py first.")
                return
            
            # Get all products without parent assignments
            unassigned_query = """
            SELECT 
                all_products,
                parent_assign,
                checkbox,
                product_id,
                source_table,
                table_type,
                verification_status
            FROM public.product_verification_complete
            WHERE parent_assign = 'NO_PARENT_IN_CANONICAL'
            OR verification_status = 'NO_PARENT_IN_CANONICAL'
            ORDER BY all_products
            """
            
            unassigned_df = pd.read_sql(unassigned_query, conn)
            
            print(f"\nTotal products without parent assignments: {len(unassigned_df)}")
            
            if len(unassigned_df) > 0:
                # Count by source table
                source_counts = unassigned_df['table_type'].value_counts()
                print(f"\nBreakdown by source:")
                for source, count in source_counts.items():
                    print(f"   {source}: {count} products")
                
                # Count by verification status
                status_counts = unassigned_df['verification_status'].value_counts()
                print(f"\nBreakdown by verification status:")
                for status, count in status_counts.items():
                    print(f"   {status}: {count} products")
                
                # Show sample unassigned products
                print(f"\nSample unassigned products (first 20):")
                sample_df = unassigned_df.head(20)
                for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                    print(f"   {i:2d}. {row['all_products']} (from {row['table_type']})")
                
                if len(unassigned_df) > 20:
                    print(f"   ... and {len(unassigned_df) - 20} more products")
                
                # Export to CSV for detailed review
                csv_filename = "unassigned_products.csv"
                unassigned_df.to_csv(csv_filename, index=False)
                print(f"\nExported {len(unassigned_df)} unassigned products to {csv_filename}")
                
                # Show some statistics
                print(f"\nStatistics:")
                total_products = pd.read_sql('SELECT COUNT(*) as total FROM public.product_verification_complete', conn).iloc[0, 0]
                products_with_parents = pd.read_sql("SELECT COUNT(*) as total FROM public.product_verification_complete WHERE verification_status = 'HAS_PARENT_FROM_CANONICAL'", conn).iloc[0, 0]
                print(f"   Total products in verification table: {total_products}")
                print(f"   Products with parents: {products_with_parents}")
                print(f"   Products without parents: {len(unassigned_df)}")
                
            else:
                print("All products have parent assignments!")
                
    except Exception as e:
        print(f"Error analyzing unassigned products: {e}")

def show_verification_summary():
    """Show overall verification table summary"""
    
    print("\n" + "="*60)
    print("VERIFICATION TABLE SUMMARY")
    print("="*60)
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Overall counts
            total_query = "SELECT COUNT(*) as total FROM public.product_verification_complete"
            total_count = pd.read_sql(total_query, conn).iloc[0, 0]
            
            # Count by verification status
            status_query = """
            SELECT 
                verification_status,
                COUNT(*) as count
            FROM public.product_verification_complete
            GROUP BY verification_status
            ORDER BY count DESC
            """
            status_df = pd.read_sql(status_query, conn)
            
            # Count by table type
            table_query = """
            SELECT 
                table_type,
                COUNT(*) as count
            FROM public.product_verification_complete
            GROUP BY table_type
            ORDER BY count DESC
            """
            table_df = pd.read_sql(table_query, conn)
            
            # Count by checkbox status
            checkbox_query = """
            SELECT 
                checkbox,
                COUNT(*) as count
            FROM public.product_verification_complete
            GROUP BY checkbox
            ORDER BY count DESC
            """
            checkbox_df = pd.read_sql(checkbox_query, conn)
            
            print(f"Total products: {total_count}")
            
            print(f"\nBy verification status:")
            for _, row in status_df.iterrows():
                percentage = (row['count'] / total_count) * 100
                print(f"   {row['verification_status']}: {row['count']} ({percentage:.1f}%)")
            
            print(f"\nBy source table:")
            for _, row in table_df.iterrows():
                percentage = (row['count'] / total_count) * 100
                print(f"   {row['table_type']}: {row['count']} ({percentage:.1f}%)")
            
            print(f"\nBy checkbox status:")
            for _, row in checkbox_df.iterrows():
                percentage = (row['count'] / total_count) * 100
                print(f"   {row['checkbox']}: {row['count']} ({percentage:.1f}%)")
                
    except Exception as e:
        print(f"Error showing verification summary: {e}")

def main():
    """Main function"""
    print("Analyzing Products Without Parent Assignments")
    print("=" * 60)
    
    # First show overall summary
    show_verification_summary()
    
    # Then analyze unassigned products
    analyze_unassigned_products()
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()
