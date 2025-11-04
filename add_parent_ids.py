#!/usr/bin/env python3
"""
Script to add parent_id columns to all Supabase tables and populate them
This will create parent-child relationships based on product name standardization
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import uuid

# Load environment variables
load_dotenv()

def get_db_engine():
    """Create database engine for Supabase"""
    supabase_url = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
    return create_engine(supabase_url)

def create_parent_child_mapping():
    """Create parent-child mapping based on product name standardization"""
    
    print("🔍 Creating parent-child mapping...")
    
    # Product name standardization mappings
    standardization_mappings = {
        'tomato': ['tomato', 'tomatoes', 'cherry tomato', 'roma tomato', 'beef tomato'],
        'onion': ['onion', 'onions', 'red onion', 'white onion', 'yellow onion'],
        'potato': ['potato', 'potatoes', 'sweet potato', 'russet potato', 'red potato'],
        'carrot': ['carrot', 'carrots', 'baby carrot'],
        'cabbage': ['cabbage', 'white cabbage', 'red cabbage', 'napa cabbage'],
        'lettuce': ['lettuce', 'iceberg lettuce', 'romaine lettuce', 'leaf lettuce'],
        'spinach': ['spinach', 'baby spinach', 'fresh spinach'],
        'cucumber': ['cucumber', 'cucumbers', 'english cucumber'],
        'pepper': ['pepper', 'bell pepper', 'red pepper', 'green pepper', 'yellow pepper'],
        'garlic': ['garlic', 'garlic cloves', 'fresh garlic'],
        'ginger': ['ginger', 'fresh ginger', 'ginger root'],
        'banana': ['banana', 'bananas', 'plantain'],
        'apple': ['apple', 'apples', 'red apple', 'green apple', 'gala apple'],
        'orange': ['orange', 'oranges', 'navel orange', 'blood orange'],
        'lemon': ['lemon', 'lemons', 'lime', 'limes'],
        'mango': ['mango', 'mangoes', 'ripe mango'],
        'rice': ['rice', 'white rice', 'brown rice', 'jasmine rice', 'basmati rice'],
        'wheat': ['wheat', 'wheat flour', 'whole wheat'],
        'corn': ['corn', 'sweet corn', 'corn kernels'],
        'beans': ['beans', 'black beans', 'kidney beans', 'green beans'],
        'lentils': ['lentils', 'red lentils', 'green lentils'],
        'chicken': ['chicken', 'chicken breast', 'chicken thigh', 'whole chicken'],
        'beef': ['beef', 'ground beef', 'beef steak', 'beef roast'],
        'fish': ['fish', 'salmon', 'tilapia', 'cod', 'tuna'],
        'milk': ['milk', 'whole milk', 'skim milk', '2% milk'],
        'cheese': ['cheese', 'cheddar cheese', 'mozzarella', 'parmesan'],
        'bread': ['bread', 'white bread', 'whole wheat bread', 'sourdough bread'],
        'oil': ['oil', 'vegetable oil', 'olive oil', 'coconut oil'],
        'salt': ['salt', 'table salt', 'sea salt', 'kosher salt'],
        'sugar': ['sugar', 'white sugar', 'brown sugar', 'cane sugar'],
        'flour': ['flour', 'all-purpose flour', 'wheat flour', 'bread flour']
    }
    
    # Create parent-child mapping
    parent_child_map = {}
    parent_names = {}
    
    for parent_name, child_names in standardization_mappings.items():
        parent_id = str(uuid.uuid4())
        parent_names[parent_name] = parent_id
        
        for child_name in child_names:
            parent_child_map[child_name.lower()] = {
                'parent_id': parent_id,
                'parent_name': parent_name
            }
    
    print(f"✅ Created mapping for {len(parent_child_map)} product variations")
    return parent_child_map, parent_names

def add_parent_id_columns(engine):
    """Add parent_id columns to all tables"""
    
    print("🏗️  Adding parent_id columns to all tables...")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    for table in tables:
        try:
            with engine.connect() as conn:
                # Add parent_id column if it doesn't exist
                alter_query = f"""
                ALTER TABLE public.{table} 
                ADD COLUMN IF NOT EXISTS parent_id UUID,
                ADD COLUMN IF NOT EXISTS parent_name TEXT;
                """
                conn.execute(text(alter_query))
                conn.commit()
                print(f"✅ Added parent_id and parent_name columns to {table}")
                
        except Exception as e:
            print(f"❌ Error adding columns to {table}: {e}")

def populate_parent_ids(engine, parent_child_map):
    """Populate parent_id values based on product name matching"""
    
    print("📝 Populating parent_id values...")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    for table in tables:
        try:
            print(f"🔄 Processing {table}...")
            
            with engine.connect() as conn:
                # Get all records to update
                select_query = f"SELECT id, product_name FROM public.{table}"
                df = pd.read_sql(select_query, conn)
                
                if df.empty:
                    print(f"   ⚠️  No records in {table}")
                    continue
                
                updated_count = 0
                
                for _, row in df.iterrows():
                    product_name = str(row['product_name']).lower().strip()
                    record_id = row['id']
                    
                    # Find matching parent
                    parent_info = None
                    for child_name, parent_data in parent_child_map.items():
                        if child_name in product_name or product_name in child_name:
                            parent_info = parent_data
                            break
                    
                    if parent_info:
                        # Update the record with parent_id and parent_name
                        update_query = f"""
                        UPDATE public.{table} 
                        SET parent_id = '{parent_info['parent_id']}',
                            parent_name = '{parent_info['parent_name']}'
                        WHERE id = '{record_id}';
                        """
                        conn.execute(text(update_query))
                        updated_count += 1
                
                conn.commit()
                print(f"   ✅ Updated {updated_count:,} records in {table}")
                
        except Exception as e:
            print(f"❌ Error populating {table}: {e}")

def create_parent_products_table(engine, parent_names):
    """Create a parent_products table with all parent categories"""
    
    print("🏗️  Creating parent_products table...")
    
    try:
        with engine.connect() as conn:
            # Create parent_products table
            create_query = """
            CREATE TABLE IF NOT EXISTS public.parent_products (
                id UUID PRIMARY KEY,
                parent_name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            
            # Insert parent products
            for parent_name, parent_id in parent_names.items():
                insert_query = f"""
                INSERT INTO public.parent_products (id, parent_name, description)
                VALUES ('{parent_id}', '{parent_name}', 'Parent category for {parent_name} products')
                ON CONFLICT (id) DO NOTHING;
                """
                conn.execute(text(insert_query))
            
            conn.commit()
            print(f"✅ Created parent_products table with {len(parent_names)} parent categories")
            
    except Exception as e:
        print(f"❌ Error creating parent_products table: {e}")

def verify_parent_ids(engine):
    """Verify that parent_id population worked correctly"""
    
    print("🔍 Verifying parent_id population...")
    
    tables = [
        "farm_prices", "supermarket_prices", "distribution_center_prices",
        "local_shop_prices", "ecommerce_prices", "sunday_market_prices"
    ]
    
    total_with_parent = 0
    total_records = 0
    
    for table in tables:
        try:
            with engine.connect() as conn:
                # Count records with parent_id
                count_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(parent_id) as records_with_parent
                FROM public.{table}
                """
                result = conn.execute(text(count_query))
                row = result.fetchone()
                
                if row:
                    table_total = row[0]
                    table_with_parent = row[1]
                    
                    print(f"📊 {table}: {table_with_parent:,}/{table_total:,} records have parent_id ({table_with_parent/table_total*100:.1f}%)")
                    
                    total_records += table_total
                    total_with_parent += table_with_parent
                    
        except Exception as e:
            print(f"❌ Error verifying {table}: {e}")
    
    print(f"\n📈 Overall: {total_with_parent:,}/{total_records:,} records have parent_id ({total_with_parent/total_records*100:.1f}%)")

def main():
    """Main function"""
    print("🚀 Adding Parent IDs to Supabase Tables")
    print("=" * 60)
    print("📥 Source: Remote Supabase Database")
    print("🎯 Goal: Add parent_id columns and populate them")
    print("=" * 60)
    
    try:
        # Create database connection
        engine = get_db_engine()
        
        # Step 1: Create parent-child mapping
        parent_child_map, parent_names = create_parent_child_mapping()
        
        # Step 2: Add parent_id columns to all tables
        add_parent_id_columns(engine)
        
        # Step 3: Populate parent_id values
        populate_parent_ids(engine, parent_child_map)
        
        # Step 4: Create parent_products table
        create_parent_products_table(engine, parent_names)
        
        # Step 5: Verify the results
        verify_parent_ids(engine)
        
        print("\n✅ Parent ID addition completed successfully!")
        print("🎯 Your tables now have parent-child relationships!")
        print("📊 You can now run your ETL pipeline with parent_id information!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
