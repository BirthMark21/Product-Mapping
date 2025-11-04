#!/usr/bin/env python3
"""
Apply dynamic mappings to remote Supabase tables
"""

import sys
import os
import json
import pandas as pd
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

def load_mapping_config():
    """Load mapping configuration from JSON file"""
    
    config_path = os.path.join(os.path.dirname(__file__), 'mapping_config.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"SUCCESS: Loaded mapping configuration v{config.get('version', 'unknown')}")
        return config
    except Exception as e:
        print(f"ERROR: Failed to load mapping configuration: {e}")
        return None

def get_parent_mapping_from_config(table_name, parent_products):
    """Create parent mapping dictionary from configuration"""
    
    parent_mapping = {}
    
    for parent_name, parent_info in parent_products.items():
        parent_id = parent_info['parent_id']
        children = parent_info.get('children', [])
        
        # Add parent itself
        parent_mapping[parent_name.lower()] = parent_id
        
        # Add children
        for child in children:
            if child and child.strip():
                parent_mapping[child.lower().strip()] = parent_id
    
    return parent_mapping

def apply_mapping_to_remote_table(table_name, parent_mapping):
    """Apply parent mapping to remote Supabase table"""
    
    print(f"Applying dynamic mapping to remote {table_name}...")
    
    try:
        # Get Supabase engine
        supabase_engine = get_db_engine('supabase')
        
        with supabase_engine.connect() as conn:
            # Check if column exists, if not add it
            check_column_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = 'parent_product_id'
            """
            result = pd.read_sql(check_column_query, conn, params=[table_name])
            
            if result.empty:
                print(f"  Adding parent_product_id column to {table_name}...")
                add_column_query = f"ALTER TABLE {table_name} ADD COLUMN parent_product_id TEXT"
                conn.execute(add_column_query)
                conn.commit()
                print(f"  SUCCESS: Added parent_product_id column to {table_name}")
            else:
                print(f"  parent_product_id column already exists in {table_name}")
            
            # Get all products from the table
            get_products_query = f"SELECT id, product_name FROM {table_name}"
            products_df = pd.read_sql(get_products_query, conn)
            
            if products_df.empty:
                print(f"  WARNING: No products found in {table_name}")
                return
            
            print(f"  Updating {len(products_df)} products with parent IDs...")
            
            # Update each product with parent ID
            updated_count = 0
            for _, row in products_df.iterrows():
                product_name = row['product_name']
                product_id = row['id']
                
                # Find parent ID for this product
                parent_id = None
                for mapped_name, mapped_id in parent_mapping.items():
                    if product_name.lower().strip() == mapped_name:
                        parent_id = mapped_id
                        break
                
                if parent_id:
                    update_query = f"""
                    UPDATE {table_name} 
                    SET parent_product_id = %s 
                    WHERE id = %s
                    """
                    conn.execute(update_query, (parent_id, product_id))
                    updated_count += 1
                else:
                    print(f"    WARNING: No parent found for: {product_name}")
            
            conn.commit()
            print(f"  SUCCESS: Updated {updated_count} products with parent IDs")
            
    except Exception as e:
        print(f"  ERROR: Error applying mapping to {table_name}: {e}")

def verify_mapping_results(table_name):
    """Verify mapping results for a table"""
    
    print(f"Verifying mapping results for {table_name}...")
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        with supabase_engine.connect() as conn:
            # Get sample of products with parent assignments
            query = f"""
            SELECT product_name, parent_product_id 
            FROM {table_name} 
            WHERE parent_product_id IS NOT NULL 
            LIMIT 5
            """
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                print(f"  Sample products with parent assignments:")
                for _, row in df.iterrows():
                    print(f"    {row['product_name']} -> {row['parent_product_id']}")
            
            # Get summary statistics
            summary_query = f"""
            SELECT 
                COUNT(*) as total_products,
                COUNT(parent_product_id) as with_parent_id,
                COUNT(*) - COUNT(parent_product_id) as without_parent_id
            FROM {table_name}
            """
            summary_df = pd.read_sql(summary_query, conn)
            
            if not summary_df.empty:
                row = summary_df.iloc[0]
                print(f"  Summary: {row['with_parent_id']}/{row['total_products']} products have parent IDs")
                
    except Exception as e:
        print(f"  ERROR: Error verifying results for {table_name}: {e}")

def main():
    """Main function to apply dynamic mappings to all remote tables"""
    
    print("Dynamic Mapping Application to Remote Supabase Tables")
    print("=" * 60)
    
    # Load configuration
    config = load_mapping_config()
    if not config:
        return
    
    # Apply mappings to each table
    for table_name, table_config in config['tables'].items():
        print(f"\n--- Processing {table_name} ---")
        
        # Get parent mapping from configuration
        parent_products = table_config['parent_products']
        parent_mapping = get_parent_mapping_from_config(table_name, parent_products)
        
        if parent_mapping:
            # Apply mapping to remote table
            apply_mapping_to_remote_table(table_name, parent_mapping)
            
            # Verify results
            verify_mapping_results(table_name)
        else:
            print(f"  WARNING: No parent mapping found for {table_name}")
    
    print(f"\nSUCCESS: Dynamic mappings applied to all remote tables!")
    print(f"Configuration version: {config.get('version', 'unknown')}")

if __name__ == "__main__":
    main()
