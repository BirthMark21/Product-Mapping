#!/usr/bin/env python3
"""
Script to fetch Supabase product names and variations
and add them to the standardization mapping.
"""

import pandas as pd
import os
import re
import sys
import traceback
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'pipeline'))

from utils.db_connector import get_db_engine
import config.setting as settings
try:
    from standardization import PARENT_CHILD_MAPPING
except ImportError:
    # Fallback if standardization.py is not in the path yet
    PARENT_CHILD_MAPPING = {}

# Load environment variables
load_dotenv()

def fetch_supabase_products():
    """Fetch all product names from Supabase tables"""
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
            unique_df = unique_df.rename(columns={'product_name': 'product_name'})
            print(f"✅ Total unique products from Supabase: {len(unique_df)}")
            return unique_df
        else:
            print("❌ No products found in Supabase tables")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error fetching Supabase products: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def fetch_supabase_variations():
    """Fetch product variations with variation_ni from ClickHouse"""
    print("\n📥 Fetching product variations from ClickHouse...")
    
    try:
        sql_query = '''
        SELECT 
            id,
            product_id,
            variation_ni,
            toString(created_at) AS created_at
        FROM "chipchip"."product_variations"
        WHERE variation_ni IS NOT NULL AND variation_ni != ''
          AND (_peerdb_is_deleted = 0 OR _peerdb_is_deleted IS NULL)
        ORDER BY variation_ni
        '''
        
        payload = {
            "client_id": f"ch_var_{uuid.uuid4().hex[:6]}",
            "database_id": 1,
            "json": True,
            "runAsync": False,
            "schema": "chipchip",
            "sql": sql_query,
            "expand_data": True
        }
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}',
            'User-Agent': 'chipchip/bot'
        }
        
        response = requests.post(
            f"{SUPERSET_URL}/api/v1/sqllab/execute/",
            json=payload,
            headers=headers,
            verify=False,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'result' in result and 'data' in result['result']:
                data = result['result']['data']
                df = pd.DataFrame(data)
                print(f"✅ Fetched {len(df)} product variations from ClickHouse")
                return df
            else:
                print("⚠️  No variation data in response (table might not exist or be empty)")
                return pd.DataFrame()
        else:
            print(f"⚠️  Error fetching variations: {response.status_code} (table might not exist)")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"⚠️  Error fetching ClickHouse variations: {e}")
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
        for suffix in [' fresh', ' fres', ' local', ' loc', ' imported', ' impo', ' grade a', ' grade b', 
                       ' grade c', ' a', ' b', ' c', ' large', ' small', ' medium', ' (large)', ' (small)']:
            if name_lower.endswith(suffix):
                return name_lower[:-len(suffix)].strip()
        return name_lower
    
    products_df['base_name'] = products_df['normalized'].apply(get_base_name)
    
    # Group by base name
    grouped = products_df.groupby('base_name')['normalized'].apply(list).to_dict()
    
    # Filter groups with multiple variations
    multi_variant = {k: sorted(list(set(v))) for k, v in grouped.items() if len(set(v)) > 1}
    
    print(f"✅ Found {len(multi_variant)} products with multiple variations")
    
    return multi_variant, grouped

def update_standardization_mapping(new_mappings):
    """Update the standardization.py file with new mappings"""
    print("\n📝 Updating standardization.py file...")
    
    standardization_file = 'Mapping/pipeline/standardization.py'
    
    # Read current file
    with open(standardization_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the PARENT_CHILD_MAPPING section
    start_marker = 'PARENT_CHILD_MAPPING = {'
    end_marker = '}'
    
    start_idx = content.find(start_marker)
    if start_idx == -1:
        print("❌ Could not find PARENT_CHILD_MAPPING in file")
        return False
    
    # Find the matching closing brace
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
    
    # Extract existing mapping
    existing_mapping_str = content[mapping_start:mapping_end].strip()
    
    # Merge new mappings with existing
    updated_mapping = PARENT_CHILD_MAPPING.copy()
    
    for parent, children in new_mappings.items():
        if parent in updated_mapping:
            # Merge children lists
            existing_children = set(updated_mapping[parent])
            new_children = set(children)
            updated_mapping[parent] = sorted(list(existing_children | new_children))
        else:
            # Add new parent
            updated_mapping[parent] = sorted(list(set(children)))
    
    # Generate new mapping string
    new_mapping_str = "PARENT_CHILD_MAPPING = {\n"
    for parent, children in sorted(updated_mapping.items()):
        children_str = ", ".join([f'"{child}"' for child in children])
        new_mapping_str += f'  "{parent}": [{children_str}],\n'
    new_mapping_str = new_mapping_str.rstrip(',\n') + '\n}'
    
    # Replace the mapping section
    new_content = content[:start_idx] + new_mapping_str + content[mapping_end+1:]
    
    # Write back
    backup_file = standardization_file + '.backup'
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✅ Created backup: {backup_file}")
    
    with open(standardization_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"✅ Updated {standardization_file}")
    print(f"   Added/updated {len(new_mappings)} product mappings")
    
    return True

def main():
    """Main function"""
    print("=" * 70)
    print("🚀 ADD CLICKHOUSE PRODUCTS TO STANDARDIZATION MAPPING")
    print("=" * 70)
    
    # Fetch products
    products_df = fetch_clickhouse_products()
    variations_df = fetch_clickhouse_variations()
    
    if products_df.empty and variations_df.empty:
        print("\n❌ No products or variations found. Exiting.")
        return
    
    # Combine product names and variations
    all_names = []
    
    if not products_df.empty:
        all_names.extend(products_df['product_name'].dropna().tolist())
    
    if not variations_df.empty and 'variation_ni' in variations_df.columns:
        all_names.extend(variations_df['variation_ni'].dropna().tolist())
    
    if not all_names:
        print("\n❌ No product names found. Exiting.")
        return
    
    # Create DataFrame from all names
    all_products_df = pd.DataFrame({
        'product_name': list(set(all_names))
    })
    
    print(f"\n📊 Total unique product names: {len(all_products_df)}")
    
    # Group similar products
    multi_variant, all_groups = group_similar_products(all_products_df)
    
    # Create mapping format
    new_mappings = {}
    
    for base_name, variations in multi_variant.items():
        # Use the first variation as parent, or a cleaned version
        parent = variations[0]
        # Capitalize first letter
        parent = parent[0].upper() + parent[1:] if len(parent) > 1 else parent.upper()
        new_mappings[parent] = variations
    
    print(f"\n📋 Created {len(new_mappings)} new mappings")
    print("\n📝 Sample mappings:")
    for i, (parent, children) in enumerate(list(new_mappings.items())[:10]):
        print(f"  {i+1}. {parent}: {children[:3]}..." if len(children) > 3 else f"  {i+1}. {parent}: {children}")
    
    # Update standardization file
    if update_standardization_mapping(new_mappings):
        print("\n✅ Successfully updated standardization mapping!")
        print("   Please review the changes before using.")
    else:
        print("\n⚠️  Could not automatically update file. Please review and update manually.")
        print("\n📋 Suggested mappings to add:")
        for parent, children in new_mappings.items():
            children_str = ", ".join([f'"{child}"' for child in children])
            print(f'  "{parent}": [{children_str}],')

if __name__ == "__main__":
    main()

