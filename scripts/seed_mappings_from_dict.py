#!/usr/bin/env python3
"""
Seed Mapping Rules from Dictionary
Migrates hardcoded dictionary rules into the database-driven system
"""

import sys
import os
import uuid
import pandas as pd
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["FORCE_STATIC_MAPPING"] = "true"
from utils.db_connector import get_db_engine
from pipeline.standardization import PARENT_CHILD_MAPPING, _generate_stable_uuid
from sqlalchemy import text

def seed_mappings():
    """Read PARENT_CHILD_MAPPING and insert into product_mapping_rules table"""
    print("=" * 80)
    print("🌱 SEEDING MAPPING RULES FROM DICTIONARY")
    print("=" * 80)
    
    engine = get_db_engine('hub')
    
    rules_to_insert = []
    parent_records = []
    
    print(f"Found {len(PARENT_CHILD_MAPPING)} parent products in dictionary.")
    
    for parent_name, children in PARENT_CHILD_MAPPING.items():
        # Generate stable UUID for parent
        parent_id = _generate_stable_uuid(parent_name)
        
        # 1. Prepare Parent Record (for canonical_products_master)
        parent_records.append({
            'parent_product_id': parent_id,
            'parent_name': parent_name,
            'source': 'seed_script',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })
        
        # 2. Add Rule: Parent maps to itself (Exact Match)
        rules_to_insert.append({
            'id': uuid.uuid4(),
            'input_pattern': parent_name,
            'parent_product_id': parent_id,
            'match_type': 'EXACT',
            'priority': 100,
            'is_active': True
        })
        
        # 3. Add Rules: Children map to Parent
        for child in children:
            if child != parent_name: # Avoid duplicate self-rule
                rules_to_insert.append({
                    'id': uuid.uuid4(),
                    'input_pattern': child,
                    'parent_product_id': parent_id,
                    'match_type': 'EXACT', # Assume exact for dictionary entries
                    'priority': 90,
                    'is_active': True
                })
    
    print(f"Prepared {len(parent_records)} parent records.")
    print(f"Prepared {len(rules_to_insert)} mapping rules.")
    
    # Execute Insertion
    try:
        with engine.begin() as conn:
            # A. Insert Parents (Handle duplicates via UPSERT)
            print(f"   → Inserting/Updating {len(parent_records)} Parent Records...")
            
            # Use raw SQL for upsert to handle conflicts gracefully
            # Target table: canonical_product_master (singular)
            # Columns: parent_product_id, parent_product_name, created_at
            # We skip 'source' and 'updated_at' if they don't exist, or use defaults if they do
            
            # First, check if 'source' exists to be safe? No, let's stick to core schema
            # based on user feedback.
            
            for record in parent_records:
                conn.execute(text("""
                    INSERT INTO hub_standard_products (parent_product_id, parent_product_name, created_at)
                    VALUES (:pid, :pname, :cat)
                    ON CONFLICT (parent_product_id) DO UPDATE SET
                        parent_product_name = EXCLUDED.parent_product_name
                """), {
                    'pid': str(record['parent_product_id']), # Ensure string for TEXT column
                    'pname': record['parent_name'],         # Map 'parent_name' -> 'parent_product_name'
                    'cat': record['created_at']
                })
            
            print(f"   ✅ Parent records synced.")

            # B. Insert Rules
            print("   → Inserting Mapping Rules...")
            # Clear old rules first? Maybe safer to just append or check.
            # For this seed script, let's assume we want to populate fresh or append.
            conn.execute(text("TRUNCATE TABLE hub_mapping_rules")) # OPTIONAL: Clear table for clean seed
            
            # Convert UUIDs to strings for TEXT column compatibility if needed, 
            # or rely on driver. Given parent_product_id is TEXT in rules table now:
            for rule in rules_to_insert:
                rule['parent_product_id'] = str(rule['parent_product_id'])
            
            rules_df = pd.DataFrame(rules_to_insert)
            rules_df.to_sql('hub_mapping_rules', conn, if_exists='append', index=False)
            
        print("\n✅ SEEDING COMPLETE!")
        print(f"   Now the database contains all {len(rules_to_insert)} rules from your Python dictionary.")
        return True
        
    except Exception as e:
        print(f"\n❌ SEEDING FAILED: {e}")
        return False

if __name__ == "__main__":
    seed_mappings()
