#!/usr/bin/env python3
"""
Create canonical_products_master table in all databases
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_db_engine
from sqlalchemy import text

def create_canonical_master_table(engine, db_name: str):
    """Create canonical_products_master table"""
    print(f"\n🔹 Creating canonical_products_master in {db_name}...")
    
    try:
        with engine.begin() as conn:
            # Create table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canonical_products_master (
                    parent_product_id UUID PRIMARY KEY,
                    parent_name VARCHAR(255) NOT NULL UNIQUE,
                    source VARCHAR(50),
                    created_at TIMESTAMP WITH TIME ZONE,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """))
            
            # Create indexes
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_canonical_parent_name 
                    ON canonical_products_master(parent_name)
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_canonical_created_at 
                    ON canonical_products_master(created_at)
            """))
            
            # Create update trigger function
            conn.execute(text("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """))
            
            # Create trigger
            conn.execute(text("""
                DROP TRIGGER IF EXISTS update_canonical_products_updated_at 
                    ON canonical_products_master
            """))
            
            conn.execute(text("""
                CREATE TRIGGER update_canonical_products_updated_at
                    BEFORE UPDATE ON canonical_products_master
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column()
            """))
        
        print(f"   ✅ Success: canonical_products_master created in {db_name}")
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def main():
    """Create canonical_products_master in all databases"""
    print("=" * 80)
    print("🚀 CREATING CANONICAL_PRODUCTS_MASTER TABLE")
    print("=" * 80)
    
    databases = {
        'supabase': 'Supabase PostgreSQL',
        'prod_postgres': 'Production PostgreSQL (chipchip)',
        'hub': 'Local Hub PostgreSQL'
    }
    
    success_count = 0
    
    for db_type, db_name in databases.items():
        try:
            engine = get_db_engine(db_type)
            if create_canonical_master_table(engine, db_name):
                success_count += 1
        except Exception as e:
            print(f"   ❌ Failed to connect to {db_name}: {e}")
    
    print("\n" + "=" * 80)
    print(f"✅ Created table in {success_count}/{len(databases)} databases")
    print("=" * 80)
    
    return success_count == len(databases)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
