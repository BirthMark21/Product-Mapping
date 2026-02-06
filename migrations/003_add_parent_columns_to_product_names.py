#!/usr/bin/env python3
"""
Add parent_product_id and parent_product_name columns to product_names table
(Production PostgreSQL only)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_db_engine
from sqlalchemy import text

def add_parent_columns_to_product_names(engine, db_name: str):
    """Add parent columns to product_names table"""
    print(f"\n🔹 Adding parent columns to product_names table in {db_name}...")
    
    try:
        with engine.begin() as conn:
            # Add parent_product_id column
            conn.execute(text("""
                ALTER TABLE product_names 
                ADD COLUMN IF NOT EXISTS parent_product_id UUID
            """))
            
            # Add parent_product_name column
            conn.execute(text("""
                ALTER TABLE product_names 
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
                    ALTER TABLE product_names 
                    DROP CONSTRAINT IF EXISTS fk_product_names_parent
                """))
                
                # Add foreign key constraint
                conn.execute(text("""
                    ALTER TABLE product_names
                    ADD CONSTRAINT fk_product_names_parent
                        FOREIGN KEY (parent_product_id) 
                        REFERENCES canonical_products_master(parent_product_id)
                        ON DELETE SET NULL
                        ON UPDATE CASCADE
                """))
            
            # Create indexes
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_product_names_parent_id 
                    ON product_names(parent_product_id)
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_product_names_parent_name 
                    ON product_names(parent_product_name)
            """))
        
        print(f"   ✅ Success: Added parent columns to product_names in {db_name}")
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def main():
    """Add parent columns to product_names table in Production PostgreSQL only"""
    print("=" * 80)
    print("🚀 ADDING PARENT COLUMNS TO PRODUCT_NAMES TABLE")
    print("=" * 80)
    
    db_name = 'Production PostgreSQL (chipchip)'
    
    try:
        engine = get_db_engine('prod_postgres')
        success = add_parent_columns_to_product_names(engine, db_name)
        
        print("\n" + "=" * 80)
        if success:
            print("✅ Successfully updated product_names table")
        else:
            print("❌ Failed to update product_names table")
        print("=" * 80)
        
        return success
        
    except Exception as e:
        print(f"\n❌ Failed to connect to {db_name}: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
