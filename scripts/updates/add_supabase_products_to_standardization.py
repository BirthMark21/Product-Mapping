#!/usr/bin/env python3
"""
Script to fetch Supabase product names from all price tables
and add them to the standardization mapping.
"""

import pandas as pd
import os
import re
import sys
import traceback
import importlib.util
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.db_connector import get_db_engine
import config.setting as settings

# Load environment variables
load_dotenv()

def fetch_supabase_products():
    """Fetch all unique product names from Supabase price tables"""
    print("\n📥 Fetching product names from Supabase...")
    
    try:
        supabase_engine = get_db_engine('supabase')
        supabase_tables = settings.SUPABASE_TABLES
        
        all_products = []
        
        for table in supabase_tables:
            try:
                query = f"""
                SELECT DISTINCT
                    product_name
                FROM {settings.SUPABASE_SCHEMA}.{table}
                WHERE product_name IS NOT NULL 
                  AND product_name != ''
                  AND product_name != '0'
                ORDER BY product_name
                """
                
                with supabase_engine.connect() as conn:
                    df = pd.read_sql(text(query), conn)
                    
                    if not df.empty:
                        print(f"  ✅ {table}: {len(df)} unique products")
                        all_products.append(df)
                    else:
                        print(f"  ⚠️  {table}: 0 products")
                        
            except Exception as e:
                print(f"  ❌ Error fetching from {table}: {e}")
        
        if all_products:
            combined_df = pd.concat(all_products, ignore_index=True)
            # Get unique product names
            unique_df = combined_df.drop_duplicates(subset=['product_name'])
            print(f"\n✅ Total unique products from Supabase: {len(unique_df)}")
            return unique_df
        else:
            print("❌ No products found in Supabase tables")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error fetching Supabase products: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def normalize_name(name):
    """Normalize product name for comparison"""
    if not name or pd.isna(name):
        return ""
    name = str(name).strip()
    # Remove extra spaces
    name = re.sub(r'\s+', ' ', name)
    return name

def group_similar_products(products_df):
    """Group similar product names together"""
    print("\n🔍 Grouping similar products...")
    
    # Normalize all names
    products_df['normalized'] = products_df['product_name'].apply(normalize_name)
    
    # Group by base name (remove common suffixes/prefixes)
    def get_base_name(name):
        name_lower = name.lower()
        # Remove common suffixes
        suffixes = [' fresh', ' fres', ' local', ' loc', ' imported', ' impo', ' imp', 
                   ' grade a', ' grade b', ' grade c', ' grade d',
                   ' a', ' b', ' c', ' d',
                   ' large', ' small', ' medium', 
                   ' (large)', ' (small)', ' (medium)',
                   ' fre', ' fresi', ' loci', ' loca', ' impoi']
        for suffix in sorted(suffixes, key=len, reverse=True):  # Try longest first
            if name_lower.endswith(suffix):
                return name_lower[:-len(suffix)].strip()
        return name_lower
    
    products_df['base_name'] = products_df['normalized'].apply(get_base_name)
    
    # Group by base name
    grouped = products_df.groupby('base_name')['normalized'].apply(lambda x: sorted(list(set(x)))).to_dict()
    
    # Filter groups with multiple variations
    multi_variant = {k: v for k, v in grouped.items() if len(v) > 1}
    
    print(f"✅ Found {len(multi_variant)} products with multiple variations")
    
    return multi_variant, grouped

def load_existing_mapping():
    """Load existing PARENT_CHILD_MAPPING from standardization.py"""
    standardization_path = os.path.join('Mapping', 'pipeline', 'standardization.py')
    
    spec = importlib.util.spec_from_file_location("standardization", standardization_path)
    standardization_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(standardization_module)
    
    return standardization_module.PARENT_CHILD_MAPPING

def update_standardization_mapping(new_mappings):
    """Update the standardization.py file with new mappings"""
    print("\n📝 Updating standardization.py file...")
    
    standardization_file = 'Mapping/pipeline/standardization.py'
    
    # Load existing mapping
    try:
        existing_mapping = load_existing_mapping()
        print(f"✅ Loaded existing mapping with {len(existing_mapping)} parent products")
    except Exception as e:
        print(f"⚠️  Could not load existing mapping: {e}")
        existing_mapping = {}
    
    # Merge new mappings with existing
    updated_mapping = existing_mapping.copy()
    
    for parent, children in new_mappings.items():
        # Check if parent already exists
        if parent in updated_mapping:
            # Merge children lists
            existing_children = set(updated_mapping[parent])
            new_children = set(children)
            updated_mapping[parent] = sorted(list(existing_children | new_children))
            print(f"  Updated: {parent} (added {len(new_children - existing_children)} new variations)")
        else:
            # Add new parent
            updated_mapping[parent] = sorted(list(set(children)))
            print(f"  Added: {parent} ({len(children)} variations)")
    
    # Read the file
    with open(standardization_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the PARENT_CHILD_MAPPING section and replace it
    start_marker = 'PARENT_CHILD_MAPPING = {'
    end_marker = '}'
    
    start_idx = content.find(start_marker)
    if start_idx == -1:
        print("❌ Could not find PARENT_CHILD_MAPPING in file")
        return False
    
    # Find the matching closing brace (need to track nested braces)
    brace_count = 0
    in_string = False
    string_char = None
    escape_next = False
    
    mapping_start = start_idx + len(start_marker)
    mapping_end = mapping_start
    
    for i in range(mapping_start, len(content)):
        char = content[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\':
            escape_next = True
            continue
        
        if not in_string:
            if char in ['"', "'"]:
                in_string = True
                string_char = char
            elif char == '{':
                brace_count += 1
            elif char == '}':
                if brace_count == 0:
                    mapping_end = i
                    break
                brace_count -= 1
        else:
            if char == string_char:
                in_string = False
                string_char = None
    
    if mapping_end == mapping_start:
        print("❌ Could not find end of PARENT_CHILD_MAPPING")
        return False
    
    # Generate new mapping string
    new_mapping_lines = ['PARENT_CHILD_MAPPING = {']
    for parent, children in sorted(updated_mapping.items()):
        children_str = ", ".join([f'"{child}"' for child in children])
        new_mapping_lines.append(f'  "{parent}": [{children_str}],')
    new_mapping_lines.append('}')
    new_mapping_str = '\n'.join(new_mapping_lines)
    
    # Replace the mapping section
    new_content = content[:start_idx] + new_mapping_str + content[mapping_end+1:]
    
    # Create backup
    backup_file = standardization_file + '.backup'
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✅ Created backup: {backup_file}")
    
    # Write updated file
    with open(standardization_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"✅ Updated {standardization_file}")
    print(f"   Total parent products: {len(updated_mapping)}")
    
    return True

def main():
    """Main function"""
    print("=" * 70)
    print("🚀 ADD SUPABASE PRODUCTS TO STANDARDIZATION MAPPING")
    print("=" * 70)
    
    # Fetch products from Supabase
    products_df = fetch_supabase_products()
    
    if products_df.empty:
        print("\n❌ No products found. Exiting.")
        return
    
    print(f"\n📊 Total unique product names: {len(products_df)}")
    
    # Group similar products
    multi_variant, all_groups = group_similar_products(products_df)
    
    if not multi_variant:
        print("\n⚠️  No products with multiple variations found.")
        print("   All products are unique or already mapped.")
        return
    
    # Create mapping format - use first variation as parent, or cleaned name
    new_mappings = {}
    
    for base_name, variations in multi_variant.items():
        # Use a cleaned version of the first variation as parent
        parent = variations[0]
        # Capitalize first letter
        if len(parent) > 0:
            parent = parent[0].upper() + parent[1:] if len(parent) > 1 else parent.upper()
        new_mappings[parent] = variations
    
    print(f"\n📋 Created {len(new_mappings)} new mappings")
    print("\n📝 Sample mappings (first 10):")
    for i, (parent, children) in enumerate(list(new_mappings.items())[:10]):
        preview = f"{children[:3]}..." if len(children) > 3 else children
        print(f"  {i+1}. {parent}: {preview}")
    
    # Ask user if they want to proceed
    print(f"\n⚠️  This will update {len(new_mappings)} product mappings in standardization.py")
    print("   A backup will be created automatically.")
    
    # Update standardization file
    if update_standardization_mapping(new_mappings):
        print("\n✅ Successfully updated standardization mapping!")
        print("   Please review the changes before using.")
    else:
        print("\n⚠️  Could not automatically update file.")
        print("\n📋 Suggested mappings to add manually:")
        for parent, children in list(new_mappings.items())[:20]:
            children_str = ", ".join([f'"{child}"' for child in children[:5]])
            if len(children) > 5:
                children_str += f", ... ({len(children)-5} more)"
            print(f'  "{parent}": [{children_str}],')

if __name__ == "__main__":
    main()

