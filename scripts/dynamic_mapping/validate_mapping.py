#!/usr/bin/env python3
"""
Validate dynamic mapping configuration
"""

import sys
import os
import json
import re
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

def validate_uuid(uuid_string):
    """Validate UUID format"""
    
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, uuid_string, re.IGNORECASE))

def validate_mapping_config(config):
    """Validate mapping configuration"""
    
    print("Validating Dynamic Mapping Configuration")
    print("=" * 50)
    
    errors = []
    warnings = []
    
    # Check required top-level fields
    required_fields = ['version', 'last_updated', 'tables']
    for field in required_fields:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    
    # Validate version format
    if 'version' in config:
        version = config['version']
        if not isinstance(version, str) or not re.match(r'^\d+\.\d+$', version):
            warnings.append(f"Invalid version format: {version}")
    
    # Validate tables
    if 'tables' in config:
        tables = config['tables']
        
        if not isinstance(tables, dict):
            errors.append("Tables must be a dictionary")
        else:
            for table_name, table_config in tables.items():
                # Check table structure
                if not isinstance(table_config, dict):
                    errors.append(f"Table {table_name} config must be a dictionary")
                    continue
                
                # Check required table fields
                if 'parent_products' not in table_config:
                    errors.append(f"Missing parent_products for table: {table_name}")
                    continue
                
                parent_products = table_config['parent_products']
                if not isinstance(parent_products, dict):
                    errors.append(f"parent_products for {table_name} must be a dictionary")
                    continue
                
                # Validate each parent product
                for parent_name, parent_info in parent_products.items():
                    if not isinstance(parent_info, dict):
                        errors.append(f"Parent info for {parent_name} in {table_name} must be a dictionary")
                        continue
                    
                    # Check required parent fields
                    if 'parent_id' not in parent_info:
                        errors.append(f"Missing parent_id for {parent_name} in {table_name}")
                    else:
                        parent_id = parent_info['parent_id']
                        if not validate_uuid(parent_id):
                            errors.append(f"Invalid UUID format for {parent_name}: {parent_id}")
                    
                    # Check children
                    if 'children' in parent_info:
                        children = parent_info['children']
                        if not isinstance(children, list):
                            errors.append(f"Children for {parent_name} in {table_name} must be a list")
                        else:
                            for child in children:
                                if not isinstance(child, str):
                                    errors.append(f"Child name must be string: {child}")
                                elif not child.strip():
                                    warnings.append(f"Empty child name for {parent_name} in {table_name}")
    
    # Print results
    if errors:
        print("❌ ERRORS FOUND:")
        for error in errors:
            print(f"  - {error}")
    
    if warnings:
        print("⚠️  WARNINGS:")
        for warning in warnings:
            print(f"  - {warning}")
    
    if not errors and not warnings:
        print("✅ Configuration is valid!")
        return True
    elif not errors:
        print("✅ Configuration is valid with warnings!")
        return True
    else:
        print("❌ Configuration has errors!")
        return False

def show_mapping_summary(config):
    """Show summary of mapping configuration"""
    
    print("\nMapping Configuration Summary")
    print("=" * 40)
    
    if 'version' in config:
        print(f"Version: {config['version']}")
    
    if 'last_updated' in config:
        print(f"Last Updated: {config['last_updated']}")
    
    if 'tables' in config:
        print(f"Tables: {len(config['tables'])}")
        
        for table_name, table_config in config['tables'].items():
            if 'parent_products' in table_config:
                parent_count = len(table_config['parent_products'])
                print(f"  - {table_name}: {parent_count} parent products")
                
                # Count total children
                total_children = 0
                for parent_info in table_config['parent_products'].values():
                    if 'children' in parent_info:
                        total_children += len(parent_info['children'])
                
                print(f"    Total children: {total_children}")

def main():
    """Main function to validate mapping configuration"""
    
    # Load configuration
    config = load_mapping_config()
    if not config:
        return
    
    # Validate configuration
    is_valid = validate_mapping_config(config)
    
    # Show summary
    show_mapping_summary(config)
    
    if is_valid:
        print("\n✅ Configuration is ready to use!")
    else:
        print("\n❌ Please fix errors before using the configuration!")

if __name__ == "__main__":
    main()
