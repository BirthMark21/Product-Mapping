#!/usr/bin/env python3
"""
Create database-driven mapping tables
Implements the 'Powerful Solution' for dynamic, scalable product standardization
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_db_engine
from sqlalchemy import text

def create_mapping_tables(engine, db_name: str):
    """Create hub_standard_products, hub_mapping_rules and hub_unmapped_queue tables"""
    print(f"\n🔹 Creating mapping infrastructure in {db_name}...")
    
    try:
        with engine.begin() as conn:
            # 0. Create hub_standard_products table first (Required for Foreign Keys)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS hub_standard_products (
                    parent_product_id TEXT PRIMARY KEY,
                    parent_product_name TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """))

            # 1. Create hub_mapping_rules table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS hub_mapping_rules (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    input_pattern TEXT NOT NULL,
                    parent_product_id TEXT NOT NULL,  -- Changed to TEXT to match hub_standard_products
                    match_type VARCHAR(20) DEFAULT 'EXACT', -- EXACT, FUZZY, REGEX
                    priority INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    
                    CONSTRAINT fk_mapping_parent 
                        FOREIGN KEY (parent_product_id) 
                        REFERENCES hub_standard_products(parent_product_id)
                        ON DELETE CASCADE
                )
            """))
            
            # Create indexes for mapping rules
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_mapping_pattern 
                    ON hub_mapping_rules(input_pattern)
            """))
            
            # 2. Create hub_unmapped_queue table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS hub_unmapped_queue (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    raw_product_name TEXT NOT NULL,
                    source_system VARCHAR(50),
                    occurrence_count INTEGER DEFAULT 1,
                    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, IGNORED, MAPPED
                    resolution_note TEXT,
                    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """))
            
            # Create indexes for unmapped queue
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_unmapped_status 
                    ON hub_unmapped_queue(status)
            """))
            
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_unmapped_name_source 
                    ON hub_unmapped_queue(raw_product_name, source_system)
            """))

        print(f"   ✅ Success: Mapping tables created in {db_name}")
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def main():
    """Create mapping tables in Hub database"""
    print("=" * 80)
    print("🚀 CREATING DATABASE-DRIVEN MAPPING INFRASTRUCTURE")
    print("=" * 80)
    
    # We only need these tables in the HUB (Central) database
    # The source databases (Supabase, Prod) don't need to know about mapping rules
    db_name = 'Local Hub PostgreSQL'
    
    try:
        engine = get_db_engine('hub')
        success = create_mapping_tables(engine, db_name)
        
        if success:
            print("\n" + "=" * 80)
            print("🎉 Infrastructure ready! Next steps:")
            print("   1. Run script to seed tables from legacy Standardization.py")
            print("   2. Update pipeline to read from DB instead of hardcoded dict")
            print("=" * 80)
        
        return success
        
    except Exception as e:
        print(f"\n❌ Failed to connect to {db_name}: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
