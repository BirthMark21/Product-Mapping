#!/usr/bin/env python3
"""
Add new products to dynamic mapping configuration
"""

import sys
import os
import json
import uuid
from datetime import datetime

def load_mapping_config():
    """Load mapping configuration from JSON file"""
    
    config_path = os.path.join(os.path.dirname(__file__), 'mapping_config.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"ERROR: Failed to load mapping configuration: {e}")
        return None

def save_mapping_config(config):
    """Save mapping configuration to JSON file"""
    
    config_path = os.path.join(os.path.dirname(__file__), 'mapping_config.json')
    
    try:
        # Update last_updated timestamp
        config['last_updated'] = datetime.now().strftime('%Y-%m-%d')
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        print("SUCCESS: Configuration saved successfully")
        return True
    except Exception as e:
        print(f"ERROR: Failed to save configuration: {e}")
        return False

def generate_parent_id(parent_name):
    """Generate a stable UUID for parent product"""
    
    # Use a namespace UUID for consistent generation
    namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    return str(uuid.uuid5(namespace, parent_name))

def add_product_to_table(config, table_name, parent_name, children=None):
    """Add a new product to a specific table"""
    
    if table_name not in config['tables']:
        print(f"ERROR: Table {table_name} not found in configuration")
        return False
    
    # Generate parent ID
    parent_id = generate_parent_id(parent_name)
    
    # Create parent product entry
    parent_product = {
        "parent_id": parent_id,
        "children": children or []
    }
    
    # Add to configuration
    config['tables'][table_name]['parent_products'][parent_name] = parent_product
    
    print(f"SUCCESS: Added {parent_name} to {table_name}")
    print(f"  Parent ID: {parent_id}")
    print(f"  Children: {children or 'None'}")
    
    return True

def add_product_to_all_tables(config, parent_name, children=None):
    """Add a new product to all tables"""
    
    success_count = 0
    total_tables = len(config['tables'])
    
    for table_name in config['tables'].keys():
        if add_product_to_table(config, table_name, parent_name, children):
            success_count += 1
    
    print(f"\nSUCCESS: Added {parent_name} to {success_count}/{total_tables} tables")
    return success_count == total_tables

def interactive_add_product():
    """Interactive function to add new products"""
    
    print("Add New Product to Dynamic Mapping")
    print("=" * 40)
    
    # Load configuration
    config = load_mapping_config()
    if not config:
        return
    
    # Get product details
    parent_name = input("Enter parent product name: ").strip()
    if not parent_name:
        print("ERROR: Parent name cannot be empty")
        return
    
    # Get children
    children_input = input("Enter children (comma-separated, or press Enter for none): ").strip()
    children = [child.strip() for child in children_input.split(',')] if children_input else []
    
    # Filter out empty children
    children = [child for child in children if child]
    
    # Choose tables
    print(f"\nAvailable tables: {', '.join(config['tables'].keys())}")
    table_choice = input("Enter table name (or 'all' for all tables): ").strip().lower()
    
    if table_choice == 'all':
        success = add_product_to_all_tables(config, parent_name, children)
    else:
        if table_choice in config['tables']:
            success = add_product_to_table(config, table_choice, parent_name, children)
        else:
            print(f"ERROR: Table {table_choice} not found")
            return
    
    if success:
        # Save configuration
        if save_mapping_config(config):
            print(f"\n✅ Product '{parent_name}' added successfully!")
            print("Run 'python update_mapping.py' to apply changes to master tables")
            print("Run 'python apply_dynamic_mapping.py' to update remote tables")
        else:
            print("❌ Failed to save configuration")
    else:
        print("❌ Failed to add product")

def main():
    """Main function"""
    
    interactive_add_product()

if __name__ == "__main__":
    main()
