#!/usr/bin/env python3
"""
Script to add parent_product_id column to local farm_prices table
Uses parent IDs from canonical_products_master table to ensure consistency
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys
import uuid
import re

# Add the parent directory to the path so we can import config and utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

# Use the same namespace UUID as canonical master
NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

# Parent-child mapping for farm prices (same as canonical master)
FARM_PRICES_PARENT_CHILD_MAPPING = {
  "Red Onion A": ["Red Onion A", "Red Onion Grade A", "Red Onion Grade A Restaurant q", "Red Onion Grade A Restaurant quality", "Red Onion", "Red onion ( ሃበሻ )"],
  "Red Onion B": ["Red Onion B", "Red Onion Grade B"],
  "Red Onion C": ["Red Onion C", "Red Onion Grade C" , "Red Onion D"],
  "Red Onion Elfora": ["Red Onion Elfora" , "Redonion Elfora"],
  "Carrot": ["Carrot", "Carrot B"],
  "Tomato A": ["Tomato A", "Tomatoes Grade A", "Tomato", "Tomato Restaurant Quality ", "Tomatoes A", "Tomato Grade A"],
  "Tomato B": ["Tomato B", "Tomatoes B", "Tomatoes Grade B"],
  "Potato": ["Potato", "Potatoes", "Potatoes Chips", "Potato for Chips",  "Potatoes Restaurant quality", "Potatoes Restaurant Quality", "Potato Chips"],
  "Chili Green": ["Chili Green", "Chilly Green", "Chilly Green (Starta)", "Green Chili", "Green Chilli", "Green Chili (ስታርታ)","Chilly Green (Starter)", "Chilly Green Elfora", "Chilly Green (Elfora)", "Green chili starta"],
  "Beetroot": ["Beetroot", "beetroot", "Beet root"],
  "White Cabbage": ["White Cabbage", "White Cabbage B", "White Cabbage (Large)", "White Cabbage (Small)","White Cabbage (medium)","Habesha Cabbage", "whitecabbage", "White cabbage"],
  "Avocado": ["Avocado", "Avocado A", "Avocado OG",  "Avocado Shekaraw", "Avocado B"],
  "Strawberry": ["Strawberry"],
  "Papaya": ["Papaya", "Papaya B" , "Papaya Oversize"],
  "Cucumber": ["Cucumber"],
  "Zucchini": ["Zucchini", "Zucchini", "Courgetti", "Courgette"],
  "Garlic": ["Garlic", "Garlic B" , "garlic" , "Garlic (የተፈለፈለ)", "Hatched Garlic"],
  "Ginger": ["Ginger"],
  "Pineapple": ["Pineapple", "pineapple", "Pineapple B"],
  "Mango Apple": ["Mango Apple", "Apple Mango"],
  "Lemon": ["Lemon", "lemon", "Lemon", "Lomen"],
  "Orange": ["Orange", "Valencia Orange", "orange Valencia", "Valencia Orange", "Orange Yerer", "Yerer Orange" , "Orange Yarer ", "Orange Pineapple "],
  "Apple": ["Apple" , "Appel"],
  "Corn": ["Corn"],
  "Mango": ["Mango", "Ye Habeshaa Mango","Habesha Mango", "Ye Habesha Mango" ],
  "Green Beans": ["Fossolia", "Green Beans", "Grean Beans" , "Green bean"],
  "Salad": ["Salad"],
  "Broccoli": ["Broccoli", "Broccolis"],
  "Sweet Potato": ["Sweet Potatoes", "sweet potato", "Sweet Potato", "sweet potato"],
  "Banana": ["Banana" , "Banana/ Raw "],
  "Eggplant": ["Egg plant", "eggplant", "Egg plant"],
  "AL-Hinan Flour": ["AL-Hinan Flour", "AL-Hinan flour"],
  "Aluu Sunflower Oil": ["Aluu Pure Sunflower Oil", "Aluu Sunflower Oil"],
  "CK Powdered Soap": ["CK Powdered Soap", "CK Powdered Soap ", "Ck Powder Soap", "Ck powdered soap"],
  "Cross-body Bag": ["Cross-body Bag", "Crossbody Bag"],
  "Dachi Ketchup": ["Dachi Ketchup", "Dachi Ketchup ", "Dachi Kethup"],
  "Dawedo Pepper": ["Dawed pepper ", "Dawedo Pepper "],
  "Difo Package": ["Difo Package", "Difo package ", "ድፎ ጥቅል", "ዳቦ ጥቅል"],
  "Girum Cinnamon Tea Bag": ["Girum Cinamon Tea Bag", "Girum Cinnamon Tea Bag "],
  "Happy Toilet Tissue": ["Happy Toilet Tissue", "Happy Toilet Paper"],
  "Iceberg Salad": ["Iceberg", "Iceberg Salad", "iceberg salad"],
  "Indomie Vegetable Noodles": ["Indomie Vegetable Noodles ", "Indomie Vegie Noodles "],
  "MIA Pasta": ["MIA Pasta", "Mia Pasta"],
  "Mawi Coffee": ["Mawi Coffee ", "Mawi coffee "],
  "Moon Vanilla Biscuit": ["Moon Vanilla Biscuit ", "Moon vanilla biscut "],
  "Omar Sunflower Oil": ["Omaar sunflower oil", "Omar Sunflower Oil"],
  "Qulet Package": ["Qulet Package", "Qulet package ", "ቁሌት ጥቅል "],
  "Roll Detergent powder": ["Rol Bio Laundry powder ", "Roll Detergent powder"],
  "Tulip Toilet Paper": ["Tulip Toilet Paper", "Tulip soft "],
  "YALI Bleach 5lit": ["YALI Bleach 5lit ", "Yali Bleach 5L"],
  "YALI Multi Purpose 2lit": ["YALI Multi Purpose 2lit ", "Yali Multi purpose 2lit"],
  "YALI Multi Purpose 5lit": ["YALI Multi Purpose 5lit", "Yali Multi purpose 5lit"],
  "Yeah laundry bar soap": ["YEAH Bar Soap", "Yeah laundry bar soap"],
  "Zagol Yoghurt": ["Zagol Yoghurt", "Zagol yoghurt"],
  "Victory Water": ["Victory Natural Water/Pack", "Victory Water ", "victory purified natural water"],
  "ABC Diaper": ["ABC Diaper", "ABC Daiper"],
  "Bonga Honey": ["Bonga Honey", "Bonga Mar"],
  "Cheese": ["Cheese", "አይብ", "Ethiopian Cheese"],
  "Cheese Package": ["Cheese Package", "አይብ ጥቅል"],
  "Goat Meat": ["Goat Meat", "የፍየል ስጋ", "Special Goat Package", "Regular Goat Package", "ሙክት የፍየል ስጋ"],
  "Lamb": ["Lamb", "Lamp", "የበግ ስጋ", "ጠቦት የበግ ስጋ", "Regular Mutton Package", "Sheep package"],
  "Bravo Table Tissue": ["Bravo Table Tissue", "Bravo Toilet Paper"],
  "Habesha Mitin Shiro": ["Habesha Mitin Shiro", "Habsha Mitin Shiro"],
  "Chicken Package": ["Chicken Groceries", "Regular Chicken Package", "Special Chicken Package", "BGS Foreign Chicken", "12 Piece Chicken", "Chicken Package", "Habesha Chicken", "Habesha Chicken Package"],
  "Tesfaye Peanut Butter": ["Tesfaye Peanut Butter", "Tesfaye Peanut"]
}

def _create_child_to_parent_map(mapping):
    """Create a reverse mapping from child names to parent names"""
    child_to_parent = {}
    for parent, children in mapping.items():
        for child in children:
            child_to_parent[child.lower().strip()] = parent
    return child_to_parent

CHILD_TO_PARENT_MAP = _create_child_to_parent_map(FARM_PRICES_PARENT_CHILD_MAPPING)

def _generate_stable_uuid(name):
    """Generate stable UUID for parent product (matching canonical master logic)"""
    return str(uuid.uuid5(NAMESPACE_UUID, name))

def get_farm_prices_master_data():
    """Get parent-child relationships from farm_prices_master table"""
    
    print("Fetching parent-child relationships from farm_prices_master...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            query = """
            SELECT 
                parent_product_id,
                parent_product_name,
                child_product_names
            FROM public.farm_prices_master
            ORDER BY parent_product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("ERROR: No data found in farm_prices_master table!")
                return {}
        
            print(f"SUCCESS: Found {len(df)} parent products in farm_prices_master")
            
            # Create mapping dictionary
            parent_mapping = {}
            for _, row in df.iterrows():
                parent_name = row['parent_product_name']
                parent_id = row['parent_product_id']
                
                # Add parent itself
                parent_mapping[parent_name.lower().strip()] = {
                    'parent_name': parent_name,
                    'parent_id': parent_id
                }
                
                # Add all child names
                if row['child_product_names']:
                    try:
                        import ast
                        child_names = ast.literal_eval(row['child_product_names'])
                        for child_name in child_names:
                            parent_mapping[child_name.lower().strip()] = {
                                'parent_name': parent_name,
                                'parent_id': parent_id
                            }
                    except:
                        pass
            
            return parent_mapping
            
    except Exception as e:
        print(f"ERROR: Error fetching farm_prices_master data: {e}")
        return {}

def add_parent_product_id_to_table(engine, table_name, parent_mapping):
    """Add parent_product_id column to the specified table"""
    
    print(f"Adding parent_product_id column to {table_name}...")
    
    try:
        with engine.connect() as conn:
            # Check if column already exists
            check_column_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}' 
            AND column_name = 'parent_product_id'
            """
            result = pd.read_sql(check_column_query, conn)
            
            if not result.empty:
                print(f"  WARNING: parent_product_id column already exists in {table_name}")
                # Drop existing column first
                drop_column_query = f"ALTER TABLE public.{table_name} DROP COLUMN IF EXISTS parent_product_id"
                conn.execute(text(drop_column_query))
                conn.commit()
                print(f"  Dropped existing parent_product_id column")
            
            # Add parent_product_id column
            add_column_query = f"ALTER TABLE public.{table_name} ADD COLUMN parent_product_id TEXT"
            conn.execute(text(add_column_query))
            conn.commit()
            print(f"  SUCCESS: Added parent_product_id column to {table_name}")
            
            # Get all products from the table
            get_products_query = f"SELECT id, product_name FROM public.{table_name}"
            products_df = pd.read_sql(get_products_query, conn)
            
            if products_df.empty:
                print(f"  ⚠️  No products found in {table_name}")
                return True
            
            print(f"  📝 Updating {len(products_df)} products with parent IDs...")
            
            # Update each product with parent_product_id
            updated_count = 0
            for _, row in products_df.iterrows():
                product_name = row['product_name']
                product_id = row['id']
                
                # Clean product name for matching
                cleaned_name = re.sub(r'\s+', ' ', str(product_name)).strip().lower()
                
                # Find parent mapping
                if cleaned_name in parent_mapping:
                    parent_info = parent_mapping[cleaned_name]
                    parent_id = parent_info['parent_id']
                    
                    # Update the record
                    update_query = text(f"""
                        UPDATE public.{table_name} 
                        SET parent_product_id = :parent_id 
                        WHERE id = :product_id
                    """)
                    
                    conn.execute(update_query, {
                        'parent_id': parent_id,
                        'product_id': product_id
                    })
                    conn.commit()
                    updated_count += 1
                else:
                    print(f"    WARNING: No parent found for: {product_name}")
            
                print(f"  SUCCESS: Updated {updated_count} products with parent IDs")
            return True
            
    except Exception as e:
        print(f"  ERROR: Error adding parent_product_id to {table_name}: {e}")
        return False

def verify_parent_product_ids(engine, table_name):
    """Verify parent_product_id assignments"""
    
    print(f"🔍 Verifying parent_product_id assignments in {table_name}...")
    
    try:
        with engine.connect() as conn:
            # Get products with their parent IDs
            verify_query = f"""
            SELECT 
                product_name,
                parent_product_id,
                CASE 
                    WHEN parent_product_id IS NOT NULL THEN 'YES' 
                    ELSE 'NO' 
                END as has_parent
            FROM public.{table_name}
            ORDER BY product_name
            LIMIT 10
            """
            verify_df = pd.read_sql(verify_query, conn)
            
            if not verify_df.empty:
                print(f"  Sample of {table_name} products with parent assignments:")
                for _, row in verify_df.iterrows():
                    print(f"    {row['has_parent']} {row['product_name']} -> {row['parent_product_id']}")
            
            # Count products with and without parent IDs
            count_query = f"""
            SELECT 
                COUNT(*) as total_products,
                COUNT(parent_product_id) as products_with_parent,
                COUNT(*) - COUNT(parent_product_id) as products_without_parent
            FROM public.{table_name}
            """
            count_df = pd.read_sql(count_query, conn)
            
            if not count_df.empty:
                row = count_df.iloc[0]
                print(f"  Summary:")
                print(f"    Total products: {row['total_products']}")
                print(f"    With parent ID: {row['products_with_parent']}")
                print(f"    Without parent ID: {row['products_without_parent']}")
            
    except Exception as e:
        print(f"  ERROR: Error verifying parent_product_id assignments: {e}")

def main():
    """Main function"""
    print("Adding parent_product_id to LOCAL farm_prices table")
    print("USING PARENT IDs FROM FARM_PRICES_MASTER TABLE")
    print("=" * 60)
    
    try:
        # Step 1: Get parent-child relationships from farm_prices_master
        parent_mapping = get_farm_prices_master_data()
        if not parent_mapping:
            print("No parent mapping found. Cannot proceed.")
            return
        
        # Step 2: Get local database engine
        local_engine = get_db_engine('hub')
        
        # Step 3: Add parent_product_id to farm_prices table
        if add_parent_product_id_to_table(local_engine, 'farm_prices', parent_mapping):
            print("\nVerification Results:")
            print("=" * 40)
            verify_parent_product_ids(local_engine, 'farm_prices')
            print("\nSUCCESS: Parent product ID assignment completed for farm_prices!")
        else:
            print("\nERROR: Failed to add parent_product_id to farm_prices table.")
        
    except Exception as e:
        print(f"\nERROR: Error in main process: {e}")

if __name__ == "__main__":
    main()
