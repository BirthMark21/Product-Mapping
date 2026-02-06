#!/usr/bin/env python3
"""
Add parent_product_id and parent_product_name columns to products table
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_db_engine
from sqlalchemy import text

def add_parent_columns_to_products(engine, db_name: str):
    """Add parent columns to products table"""
    print(f"\n🔹 Adding parent columns to products table in {db_name}...")
    
    try:
        with engine.begin() as conn:
            # Add parent_product_id column
            conn.execute(text("""
                ALTER TABLE products 
                ADD COLUMN IF NOT EXISTS parent_product_id UUID
            """))
            
            # Add parent_product_name column
            conn.execute(text("""
                ALTER TABLE products 
                ADD COLUMN IF NOT EXISTS parent_product_name VARCHAR(255)
            """))
            
            # Check if canonical_products_master exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'canonical_products_master'
                )
            """))
            table_exists = result.scalar()
            
            if table_exists:
                # Drop existing constraint if it exists
                conn.execute(text("""
                    ALTER TABLE products 
                    DROP CONSTRAINT IF EXISTS fk_products_parent
                """))
                
                # Add foreign key constraint
                conn.execute(text("""
                    ALTER TABLE products
                    ADD CONSTRAINT fk_products_parent
                        FOREIGN KEY (parent_product_id) 
                        REFERENCES canonical_products_master(parent_product_id)
                        ON DELETE SET NULL
                        ON UPDATE CASCADE
                """))
            
            # Create indexes
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_products_parent_id 
                    ON products(parent_product_id)
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_products_parent_name 
                    ON products(parent_product_name)
            """))
        
        print(f"   ✅ Success: Added parent columns to products in {db_name}")
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def main():
    """Add parent columns to products table in Supabase and Production PostgreSQL"""
    print("=" * 80)
    print("🚀 ADDING PARENT COLUMNS TO PRODUCTS TABLE")
    print("=" * 80)
    
    databases = {
        'supabase': 'Supabase PostgreSQL',
        'prod_postgres': 'Production PostgreSQL (chipchip)'
    }
    
    success_count = 0
    
    for db_type, db_name in databases.items():
        try:
            engine = get_db_engine(db_type)
            if add_parent_columns_to_products(engine, db_name):
                success_count += 1
        except Exception as e:
            print(f"   ❌ Failed to connect to {db_name}: {e}")
    
    print("\n" + "=" * 80)
    print(f"✅ Updated {success_count}/{len(databases)} databases")
    print("=" * 80)
    
    return success_count == len(databases)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
