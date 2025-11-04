#!/usr/bin/env python3
"""
Update master tables with dynamic mappings from JSON configuration
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

def update_master_table(table_name, parent_products):
    """Update a master table with dynamic mappings"""
    
    print(f"Updating {table_name} master table...")
    
    try:
        # Get database engine
        engine = get_db_engine('hub')
        
        with engine.connect() as conn:
            # Clear existing data
            delete_query = f"DELETE FROM public.{table_name}_master"
            conn.execute(delete_query)
            print(f"  Cleared existing data from {table_name}_master")
            
            # Prepare data for insertion
            master_data = []
            
            for parent_name, parent_info in parent_products.items():
                parent_id = parent_info['parent_id']
                children = parent_info.get('children', [])
                
                # Create parent record
                parent_record = {
                    'parent_product_id': parent_id,
                    'parent_product_name': parent_name,
                    'child_product_names': str(children) if children else None,
                    'created_at': pd.Timestamp.now(),
                    'updated_at': pd.Timestamp.now()
                }
                master_data.append(parent_record)
            
            # Insert new data
            if master_data:
                master_df = pd.DataFrame(master_data)
                master_df.to_sql(f'{table_name}_master', conn, if_exists='append', index=False, method='multi')
                print(f"  SUCCESS: Inserted {len(master_data)} parent products into {table_name}_master")
            else:
                print(f"  WARNING: No parent products found for {table_name}")
                
    except Exception as e:
        print(f"  ERROR: Failed to update {table_name}_master: {e}")

def validate_mapping_config(config):
    """Validate mapping configuration"""
    
    print("Validating mapping configuration...")
    
    required_fields = ['version', 'tables']
    for field in required_fields:
        if field not in config:
            print(f"ERROR: Missing required field: {field}")
            return False
    
    # Validate each table
    for table_name, table_config in config['tables'].items():
        if 'parent_products' not in table_config:
            print(f"ERROR: Missing parent_products for table: {table_name}")
            return False
        
        for parent_name, parent_info in table_config['parent_products'].items():
            if 'parent_id' not in parent_info:
                print(f"ERROR: Missing parent_id for {parent_name} in {table_name}")
                return False
            
            # Validate UUID format
            parent_id = parent_info['parent_id']
            if len(parent_id) != 36 or parent_id.count('-') != 4:
                print(f"WARNING: Invalid UUID format for {parent_name}: {parent_id}")
    
    print("SUCCESS: Mapping configuration is valid")
    return True

def main():
    """Main function to update all master tables with dynamic mappings"""
    
    print("Dynamic Mapping Update System")
    print("=" * 50)
    
    # Load configuration
    config = load_mapping_config()
    if not config:
        return
    
    # Validate configuration
    if not validate_mapping_config(config):
        print("ERROR: Invalid configuration. Please fix and try again.")
        return
    
    # Update each master table
    for table_name, table_config in config['tables'].items():
        print(f"\n--- Updating {table_name} ---")
        parent_products = table_config['parent_products']
        update_master_table(table_name, parent_products)
    
    print(f"\nSUCCESS: All master tables updated with dynamic mappings!")
    print(f"Configuration version: {config.get('version', 'unknown')}")
    print(f"Last updated: {config.get('last_updated', 'unknown')}")

if __name__ == "__main__":
    main()
