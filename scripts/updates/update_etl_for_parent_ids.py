#!/usr/bin/env python3
"""
Script to update the ETL pipeline to include parent_id information
This will modify the data loader to fetch parent_id along with product data
"""

import os

def update_data_loader():
    """Update data_loader.py to include parent_id in queries"""
    
    print("🔄 Updating data_loader.py to include parent_id...")
    
    data_loader_file = "pipeline/data_loader.py"
    
    if not os.path.exists(data_loader_file):
        print(f"❌ {data_loader_file} not found!")
        return False
    
    # Read current content
    with open(data_loader_file, 'r') as f:
        content = f.read()
    
    # Update the Supabase query to include parent_id and parent_name
    old_query = 'f"SELECT id::text AS raw_product_id, product_name AS raw_product_name FROM {settings.SUPABASE_SCHEMA}.{table}"'
    new_query = 'f"SELECT id::text AS raw_product_id, product_name AS raw_product_name, parent_id::text AS parent_id, parent_name AS parent_name FROM {settings.SUPABASE_SCHEMA}.{table}"'
    
    updated_content = content.replace(old_query, new_query)
    
    # Update the DataFrame columns
    old_columns = "columns=['raw_product_id', 'raw_product_name']"
    new_columns = "columns=['raw_product_id', 'raw_product_name', 'parent_id', 'parent_name']"
    
    updated_content = updated_content.replace(old_columns, new_columns)
    
    # Update empty DataFrame creation
    old_empty = "pd.DataFrame(columns=['raw_product_id', 'raw_product_name'])"
    new_empty = "pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'parent_id', 'parent_name'])"
    
    updated_content = updated_content.replace(old_empty, new_empty)
    
    # Write updated content
    with open(data_loader_file, 'w') as f:
        f.write(updated_content)
    
    print("✅ Updated data_loader.py to include parent_id and parent_name")
    return True

def update_standardization():
    """Update standardization.py to handle parent_id information"""
    
    print("🔄 Updating standardization.py to handle parent_id...")
    
    standardization_file = "pipeline/standardization.py"
    
    if not os.path.exists(standardization_file):
        print(f"❌ {standardization_file} not found!")
        return False
    
    # Read current content
    with open(standardization_file, 'r') as f:
        content = f.read()
    
    # Add parent_id and parent_name to the aggregation rules
    old_agg_rules = """agg_rules = {
        'raw_product_id': lambda ids: sorted(list(ids.astype(str).unique())),
        'raw_product_name': lambda names: sorted(list(names.unique()))
    }"""
    
    new_agg_rules = """agg_rules = {
        'raw_product_id': lambda ids: sorted(list(ids.astype(str).unique())),
        'raw_product_name': lambda names: sorted(list(names.unique())),
        'parent_id': lambda parent_ids: parent_ids.dropna().iloc[0] if not parent_ids.dropna().empty else None,
        'parent_name': lambda parent_names: parent_names.dropna().iloc[0] if not parent_names.dropna().empty else None
    }"""
    
    updated_content = content.replace(old_agg_rules, new_agg_rules)
    
    # Add parent_id and parent_name to the final columns order
    old_final_cols = """final_cols_order = [
        'parent_product_id',
        'parent_name',
        'child_product_ids',
        'child_product_names',
        'all_possible_child_names'
    ]"""
    
    new_final_cols = """final_cols_order = [
        'parent_product_id',
        'parent_name',
        'parent_id',
        'child_product_ids',
        'child_product_names',
        'all_possible_child_names'
    ]"""
    
    updated_content = updated_content.replace(old_final_cols, new_final_cols)
    
    # Write updated content
    with open(standardization_file, 'w') as f:
        f.write(updated_content)
    
    print("✅ Updated standardization.py to handle parent_id and parent_name")
    return True

def create_parent_id_test_script():
    """Create a test script to verify parent_id functionality"""
    
    print("🔄 Creating parent_id test script...")
    
    test_script = """#!/usr/bin/env python3
'''
Test script to verify parent_id functionality in the ETL pipeline
'''

import pandas as pd
import config.setting as settings
from utils.db_connector import get_db_engine
from pipeline.data_loader import load_all_product_data_from_supabase
from pipeline.standardization import create_parent_child_master_table

def test_parent_id_pipeline():
    '''Test the ETL pipeline with parent_id information'''
    
    print("🧪 Testing ETL Pipeline with Parent IDs")
    print("=" * 50)
    
    try:
        # Connect to Supabase
        supabase_engine = get_db_engine('supabase')
        
        # Load data with parent_id information
        print("📥 Loading data from Supabase with parent_id...")
        df = load_all_product_data_from_supabase(supabase_engine)
        
        print(f"📊 Loaded {len(df)} records")
        print(f"📋 Columns: {list(df.columns)}")
        
        if 'parent_id' in df.columns and 'parent_name' in df.columns:
            # Show parent_id statistics
            parent_stats = df.groupby('parent_name').size().sort_values(ascending=False)
            print(f"\\n📈 Parent categories found:")
            print(parent_stats.head(10))
            
            # Show sample records with parent_id
            print(f"\\n📋 Sample records with parent_id:")
            sample_df = df[['raw_product_name', 'parent_name', 'parent_id']].dropna().head()
            print(sample_df)
            
            # Test standardization with parent_id
            print(f"\\n🔄 Testing standardization with parent_id...")
            master_df = create_parent_child_master_table(df)
            print(f"📊 Created master table with {len(master_df)} parent groups")
            
            if 'parent_id' in master_df.columns:
                print(f"✅ Parent ID information preserved in master table")
                print(f"📋 Master table columns: {list(master_df.columns)}")
            else:
                print(f"⚠️  Parent ID information not found in master table")
        else:
            print(f"❌ Parent ID columns not found in data")
            print(f"📋 Available columns: {list(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error testing pipeline: {e}")

if __name__ == "__main__":
    test_parent_id_pipeline()
"""
    
    with open("test_parent_id_pipeline.py", 'w') as f:
        f.write(test_script)
    
    print("✅ Created test_parent_id_pipeline.py")
    return True

def main():
    """Main function"""
    print("🚀 Updating ETL Pipeline for Parent IDs")
    print("=" * 60)
    print("📝 Goal: Modify ETL pipeline to use parent_id information")
    print("=" * 60)
    
    try:
        # Update data loader
        if update_data_loader():
            print("✅ Data loader updated successfully")
        else:
            print("❌ Failed to update data loader")
            return
        
        # Update standardization
        if update_standardization():
            print("✅ Standardization updated successfully")
        else:
            print("❌ Failed to update standardization")
            return
        
        # Create test script
        if create_parent_id_test_script():
            print("✅ Test script created successfully")
        else:
            print("❌ Failed to create test script")
            return
        
        print("\\n🎉 ETL pipeline updated for parent_id support!")
        print("📋 Next steps:")
        print("   1. Run: python add_parent_ids.py (to add parent_id to tables)")
        print("   2. Run: python test_parent_id_pipeline.py (to test the pipeline)")
        print("   3. Run: python main.py (to run the full ETL pipeline)")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
