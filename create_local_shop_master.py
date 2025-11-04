#!/usr/bin/env python3
"""
Script to create local master table for local_shop_prices using standardization logic
Applies the same parent-child mapping logic as canonical_products_master
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import config.setting as settings
from utils.db_connector import get_db_engine
import uuid
import re

# Load environment variables
load_dotenv()

# Use the same namespace UUID as canonical master
NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

# Parent-child mapping for local shop products (based on standardization.py)
LOCAL_SHOP_PARENT_CHILD_MAPPING = {
  "Red Onion A": ["Red Onion A", "Red Onion Grade A", "Red Onion Grade A Restaurant q", "Red Onion Grade A Restaurant quality", "Red Onion"],
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
  "Red Onion (ሃበሻ)": ["Red Onion (ሃበሻ)", "Red onion ( ሃበሻ ) "],
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
    """Create reverse mapping from child to parent"""
    child_to_parent = {}
    for parent, children in mapping.items():
        for child in children:
            child_to_parent[child.lower().strip()] = parent
    return child_to_parent

CHILD_TO_PARENT_MAP = _create_child_to_parent_map(LOCAL_SHOP_PARENT_CHILD_MAPPING)

def _generate_stable_uuid(name):
    """Generate stable UUID for parent product (matching canonical master logic)"""
    return str(uuid.uuid5(NAMESPACE_UUID, name))

def get_local_shop_products():
    """Get all products from remote Supabase local_shop_prices table"""
    
    print("Fetching all products from remote Supabase local_shop_prices table...")
    print("=" * 70)
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        with supabase_engine.connect() as conn:
            query = """
            SELECT 
                id::text AS raw_product_id,
                product_name AS raw_product_name,
                created_at
            FROM public.local_shop_prices
            WHERE product_name IS NOT NULL 
            AND product_name != ''
            ORDER BY product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("❌ No products found in remote Supabase local_shop_prices table!")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} products in remote Supabase local_shop_prices table")
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase local_shop_prices: {e}")
        return pd.DataFrame()

def create_local_shop_master_table(products_df):
    """Create local shop master table with parent-child structure"""
    
    print("\n--- Generating Local Shop Master Table with Parent-Child Structure ---")
    
    if products_df.empty:
        print(" -> Input data is empty. No master table to generate.")
        return pd.DataFrame()
    
    df = products_df.copy().dropna(subset=['raw_product_name'])
    
    # Filter out products with name '0'
    initial_count = len(df)
    df = df[df['raw_product_name'].astype(str).str.strip() != '0']
    final_count = len(df)
    if initial_count > final_count:
        print(f" -> Filtered out {initial_count - final_count} records where product name was '0'.")
    
    print("Step 1: Mapping products using the explicit LOCAL_SHOP_PARENT_CHILD_MAPPING...")
    df['cleaned_name'] = df['raw_product_name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower())
    df['parent_name_from_map'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
    
    print("Step 2: Identifying a canonical parent name for unmapped products...")
    grouping_key = df['cleaned_name']
    df['parent_name_for_unmapped'] = df.groupby(grouping_key)['raw_product_name'].transform('first')
    df['parent_name'] = df['parent_name_from_map'].fillna(df['parent_name_for_unmapped'])
    
    print("Step 3: Converting created_at to consistent format...")
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    
    print("Step 4: Aggregating child data under each unique parent...")
    agg_rules = {
        'raw_product_id': lambda ids: sorted(list(ids.astype(str).unique())),
        'raw_product_name': lambda names: sorted(list(names.unique())),
        'created_at': lambda dates: dates.dropna().min() if not dates.dropna().empty else None
    }
    
    parent_groups = df.groupby('parent_name').agg(agg_rules).reset_index()
    
    parent_groups.rename(columns={
        'raw_product_id': 'child_product_ids',
        'raw_product_name': 'child_product_names'
    }, inplace=True)
    
    print("Step 5: Generating parent product IDs...")
    parent_groups['parent_product_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
    
    print("Step 6: Assigning the complete list of all possible child names...")
    parent_groups['all_possible_child_names'] = parent_groups['parent_name'].map(LOCAL_SHOP_PARENT_CHILD_MAPPING)
    parent_groups['all_possible_child_names'].fillna(parent_groups['parent_name'].apply(lambda x: [x]), inplace=True)
    
    print("Step 7: Finalizing columns...")
    final_master_df = parent_groups.copy()
    
    final_cols_order = [
        'parent_product_id',
        'parent_name',
        'child_product_ids',
        'child_product_names',
        'all_possible_child_names',
        'created_at'
    ]
    
    final_master_df = final_master_df[final_cols_order].sort_values('parent_name').reset_index(drop=True)
    final_master_df.rename(columns={'parent_name': 'parent_product_name'}, inplace=True)
    
    print(f" -> Created a final local shop master table with {len(final_master_df)} unique parent products.")
    return final_master_df

def create_local_shop_master_table_in_db(master_df):
    """Create local_shop_master table in local database"""
    
    print("\nCreating local_shop_master table in local database...")
    
    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.local_shop_master CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing local_shop_master table if it existed")
            
            # Create new local_shop_master table
            create_query = """
            CREATE TABLE public.local_shop_master (
                parent_product_id TEXT PRIMARY KEY,
                parent_product_name TEXT NOT NULL,
                child_product_ids TEXT[],
                child_product_names TEXT[],
                all_possible_child_names TEXT[],
                created_at TIMESTAMP WITH TIME ZONE
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created local_shop_master table")
            
            # Insert data
            master_df.to_sql(
                name='local_shop_master',
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"  ✅ Inserted {len(master_df)} parent products into local_shop_master table")
            
            # Verify insertion
            verify_query = "SELECT COUNT(*) as count FROM public.local_shop_master"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(master_df):
                print(f"  ✅ Verification: {inserted_count} parent products inserted successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(master_df)}, got {inserted_count}")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Error creating local_shop_master table: {e}")
        return False

def show_master_table_summary(master_df):
    """Show summary of the local shop master table"""
    
    print(f"\n📋 LOCAL SHOP MASTER TABLE SUMMARY")
    print("=" * 50)
    
    if master_df.empty:
        print("No data in master table!")
        return
    
    print(f"Total parent products: {len(master_df)}")
    
    # Show sample parent products
    print(f"\nSample parent products:")
    sample_df = master_df.head(10)
    for i, (_, row) in enumerate(sample_df.iterrows(), 1):
        child_count = len(row['child_product_ids']) if isinstance(row['child_product_ids'], list) else 0
        print(f"  {i}. {row['parent_product_name']} ({child_count} children)")
    
    if len(master_df) > 10:
        print(f"  ... and {len(master_df) - 10} more parent products")
    
    # Show child count statistics
    child_counts = []
    for _, row in master_df.iterrows():
        if isinstance(row['child_product_ids'], list):
            child_counts.append(len(row['child_product_ids']))
        else:
            child_counts.append(0)
    
    if child_counts:
        print(f"\nChild count statistics:")
        print(f"  Average children per parent: {sum(child_counts) / len(child_counts):.1f}")
        print(f"  Maximum children: {max(child_counts)}")
        print(f"  Minimum children: {min(child_counts)}")

def create_verification_table(products_df, master_df):
    """Create verification table with unique shop products and their parents"""
    
    print("\nCreating verification table with unique product names...")
    
    try:
        # Create verification data
        verification_data = []
        
        # Get unique product names to avoid duplicates
        unique_products = products_df.drop_duplicates(subset=['raw_product_name'])
        print(f"  📊 Found {len(unique_products)} unique product names (from {len(products_df)} total records)")
        
        for _, product_row in unique_products.iterrows():
            product_name = product_row['raw_product_name']
            product_id = product_row['raw_product_id']
            
            # Find parent for this product
            parent_name = "NO_PARENT_ASSIGNED"
            for _, master_row in master_df.iterrows():
                if isinstance(master_row['child_product_names'], list):
                    if product_name in master_row['child_product_names']:
                        parent_name = master_row['parent_product_name']
                        break
                elif isinstance(master_row['child_product_names'], str):
                    # Handle string representation of list
                    try:
                        import ast
                        child_names = ast.literal_eval(master_row['child_product_names'])
                        if product_name in child_names:
                            parent_name = master_row['parent_product_name']
                            break
                    except:
                        pass
            
            verification_data.append({
                'all_shop_products': product_name,
                'parent_name': parent_name,
                'product_id': product_id
            })
        
        verification_df = pd.DataFrame(verification_data)
        
        # Ensure uniqueness in the final verification table
        verification_df = verification_df.drop_duplicates(subset=['all_shop_products'])
        print(f"  ✅ Created verification table with {len(verification_df)} unique product names")
        
        # Create verification table in database
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = "DROP TABLE IF EXISTS public.local_shop_verification CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing local_shop_verification table if it existed")
            
            # Create new verification table
            create_query = """
            CREATE TABLE public.local_shop_verification (
                id SERIAL PRIMARY KEY,
                all_shop_products TEXT NOT NULL,
                parent_name TEXT NOT NULL,
                product_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created local_shop_verification table")
            
            # Insert data
            verification_df.to_sql(
                name='local_shop_verification',
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"  ✅ Inserted {len(verification_df)} products into verification table")
            
            # Verify insertion
            verify_query = "SELECT COUNT(*) as count FROM public.local_shop_verification"
            verify_df = pd.read_sql(verify_query, conn)
            inserted_count = verify_df['count'].iloc[0]
            
            if inserted_count == len(verification_df):
                print(f"  ✅ Verification: {inserted_count} products inserted successfully")
            else:
                print(f"  ❌ Verification failed: Expected {len(verification_df)}, got {inserted_count}")
                return False
            
            return verification_df
            
    except Exception as e:
        print(f"  ❌ Error creating verification table: {e}")
        return pd.DataFrame()

def export_to_csv(master_df):
    """Export master table to CSV"""
    
    print("\n📄 Exporting to CSV...")
    
    try:
        csv_filename = "local_shop_master.csv"
        master_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(master_df)} parent products to {csv_filename}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def export_verification_to_csv(verification_df):
    """Export verification table to CSV"""
    
    print("\n📄 Exporting verification table to CSV...")
    
    try:
        csv_filename = "local_shop_verification.csv"
        verification_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported {len(verification_df)} products to {csv_filename}")
        
    except Exception as e:
        print(f"❌ Error exporting verification to CSV: {e}")

def main():
    """Main function"""
    print("Creating Local Shop Master Table with Standardization Logic")
    print("=" * 70)
    
    try:
        # Step 1: Get all products from remote Supabase local_shop_prices
        products_df = get_local_shop_products()
        if products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Create master table with standardization logic
        master_df = create_local_shop_master_table(products_df)
        if master_df.empty:
            print("No master table generated. Cannot proceed.")
            return
        
        # Step 3: Show summary
        show_master_table_summary(master_df)
        
        # Step 4: Create local database table
        create_local_shop_master_table_in_db(master_df)
        
        # Step 5: Create verification table
        verification_df = create_verification_table(products_df, master_df)
        
        # Step 6: Export to CSV
        export_to_csv(master_df)
        export_verification_to_csv(verification_df)
        
        print(f"\n🎯 LOCAL SHOP MASTER TABLE CREATION COMPLETED!")
        print(f"📁 Local shop master table is now available in:")
        print(f"  - Local database: public.local_shop_master table")
        print(f"  - Local database: public.local_shop_verification table")
        print(f"  - CSV file: local_shop_master.csv")
        print(f"  - CSV file: local_shop_verification.csv")
        print(f"  - Total parent products: {len(master_df)}")
        print(f"  - Total shop products: {len(verification_df)}")
        print(f"  - Source: Remote Supabase local_shop_prices table")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
