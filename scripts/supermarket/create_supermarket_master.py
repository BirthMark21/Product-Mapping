#!/usr/bin/env python3
"""
Script to create local master table for supermarket_prices using standardization logic
Applies the same parent-child mapping logic as canonical_products_master
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys
import uuid
import re
import ast

# Add the parent directory to the path so we can import config and utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import config.setting as settings
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

# Use the same namespace UUID as canonical master
NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

# Parent-child mapping for supermarket products (based on standardization.py)
SUPERMARKET_PARENT_CHILD_MAPPING = {
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

CHILD_TO_PARENT_MAP = _create_child_to_parent_map(SUPERMARKET_PARENT_CHILD_MAPPING)

def _generate_stable_uuid(name):
    """Generate stable UUID for parent product (matching canonical master logic)"""
    return str(uuid.uuid5(NAMESPACE_UUID, name))

def get_supermarket_products():
    """Get all products from remote Supabase supermarket_prices table"""
    
    print("📥 Fetching products from remote Supabase supermarket_prices table...")
    print("=" * 70)
    
    try:
        supabase_engine = get_db_engine('supabase')
        
        if not supabase_engine:
            print("❌ Failed to get Supabase database engine.")
            return pd.DataFrame()
        
        with supabase_engine.connect() as conn:
            query = """
            SELECT 
                id::text AS raw_product_id,
                product_name AS raw_product_name,
                created_at
            FROM public.supermarket_prices
            WHERE product_name IS NOT NULL 
            AND product_name != ''
            ORDER BY product_name
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("❌ No products found in remote Supabase supermarket_prices table!")
                return pd.DataFrame()
            
            print(f"✅ Found {len(df)} products in remote Supabase supermarket_prices table")
            
            return df
            
    except Exception as e:
        print(f"❌ Error fetching from remote Supabase supermarket_prices: {e}")
        print("Trying to reconnect...")
        try:
            # Retry with a fresh connection
            supabase_engine = get_db_engine('supabase')
            with supabase_engine.connect() as conn:
                df = pd.read_sql(query, conn)
                if not df.empty:
                    print(f"✅ Retry successful: Found {len(df)} products")
                    return df
        except Exception as retry_e:
            print(f"❌ Retry also failed: {retry_e}")
        return pd.DataFrame()

def create_supermarket_master_table(products_df):
    """Create master table for supermarket products using standardization logic"""
    
    print("\n--- Generating Supermarket Master Table with Parent-Child Structure ---")

    if products_df.empty:
        print(" -> Input data is empty. No master table to generate.")
        return pd.DataFrame()

    df = products_df.copy().dropna(subset=['raw_product_name'])
    
    initial_count = len(df)
    df = df[df['raw_product_name'].astype(str).str.strip() != '0']
    final_count = len(df)
    if initial_count > final_count:
        print(f" -> Filtered out {initial_count - final_count} records where product name was '0'.")

    print("Step 1: Mapping products using the explicit PARENT_CHILD_MAPPING...")
    df['cleaned_name'] = df['raw_product_name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower())
    df['parent_name_from_map'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)

    print("Step 2: Identifying a canonical parent name for unmapped products...")
    grouping_key = df['cleaned_name']
    df['parent_name_for_unmapped'] = df.groupby(grouping_key)['raw_product_name'].transform('first')
    df['parent_name'] = df['parent_name_from_map'].fillna(df['parent_name_for_unmapped'])

    print("Step 3: Converting created_at to consistent format...")
    # Convert all created_at values to datetime and normalize to UTC for consistent comparison
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
    parent_groups['all_possible_child_names'] = parent_groups['parent_name'].map(SUPERMARKET_PARENT_CHILD_MAPPING)
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
    
    print(f" -> Created a final master table with {len(final_master_df)} unique parent products.")
    return final_master_df

def write_master_table_to_db(master_df):
    """Write the master table to local database"""
    
    print("\n--- Writing Supermarket Master Table to Local Database ---")
    local_engine = get_db_engine('hub')
    table_name = 'supermarket_master'
    schema = 'public'

    try:
        with local_engine.connect() as conn:
            # Drop existing table if it exists
            drop_query = text(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE")
            conn.execute(drop_query)
            conn.commit()
            print(f"  🗑️ Removed existing {schema}.{table_name} table if it existed.")

            # Convert list columns to string representation for TEXT storage
            for col in ['child_product_ids', 'child_product_names', 'all_possible_child_names']:
                if col in master_df.columns:
                    master_df[col] = master_df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

            master_df.to_sql(
                name=table_name,
                con=local_engine,
                schema=schema,
                if_exists='replace',
                index=False
            )
            print(f"✅ Successfully wrote {len(master_df)} records to {schema}.{table_name}")
            
            # Add primary key constraint
            with local_engine.connect() as conn_pk:
                pk_query = text(f"ALTER TABLE {schema}.{table_name} ADD PRIMARY KEY (parent_product_id)")
                conn_pk.execute(pk_query)
                conn_pk.commit()
                print(f"✅ Added primary key to {schema}.{table_name} on parent_product_id.")

    except Exception as e:
        print(f"❌ Error writing master table to database: {e}")
        raise

def create_verification_table(products_df, master_df):
    """Create verification table for supermarket products"""
    
    print("\n--- Creating Supermarket Verification Table ---")
    
    if products_df.empty or master_df.empty:
        print(" -> Input data is empty. No verification table to generate.")
        return pd.DataFrame()

    # Ensure product_name is cleaned for matching
    products_df['cleaned_name'] = products_df['raw_product_name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower())
    master_df['parent_product_name_lower'] = master_df['parent_product_name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower())

    # Create a mapping from cleaned product name to parent name and parent_product_id
    # This handles cases where a child name might be a parent name itself
    name_to_parent_map = {}
    name_to_parent_id_map = {}

    for _, row in master_df.iterrows():
        parent_name = row['parent_product_name']
        parent_id = row['parent_product_id']
        
        # Add the parent itself
        cleaned_parent_name = re.sub(r'\s+', ' ', str(parent_name)).strip().lower()
        name_to_parent_map[cleaned_parent_name] = parent_name
        name_to_parent_id_map[cleaned_parent_name] = parent_id

        # Add all possible child names
        all_children = ast.literal_eval(row['all_possible_child_names']) if isinstance(row['all_possible_child_names'], str) else row['all_possible_child_names']
        for child_name in all_children:
            cleaned_child_name = re.sub(r'\s+', ' ', str(child_name)).strip().lower()
            name_to_parent_map[cleaned_child_name] = parent_name
            name_to_parent_id_map[cleaned_child_name] = parent_id

    # Apply the mapping to the original products_df
    verification_df = products_df.copy()
    verification_df['parent_name'] = verification_df['cleaned_name'].map(name_to_parent_map)
    verification_df['parent_product_id'] = verification_df['cleaned_name'].map(name_to_parent_id_map)

    # Handle products that are parents but not explicitly mapped as children
    # If a product_name is a parent_product_name in master_df, assign itself as parent
    for _, row in master_df.iterrows():
        parent_name = row['parent_product_name']
        parent_id = row['parent_product_id']
        cleaned_parent_name = re.sub(r'\s+', ' ', str(parent_name)).strip().lower()
        
        # For products whose cleaned_name matches a parent_product_name, and they don't have a parent yet
        mask = (verification_df['cleaned_name'] == cleaned_parent_name) & (verification_df['parent_name'].isna())
        verification_df.loc[mask, 'parent_name'] = parent_name
        verification_df.loc[mask, 'parent_product_id'] = parent_id

    # Select and rename columns for the verification table
    verification_df = verification_df[[
        'raw_product_id', 
        'raw_product_name', 
        'parent_product_id',
        'parent_name', 
        'created_at'
    ]].copy()
    verification_df.rename(columns={
        'raw_product_id': 'product_id',
        'raw_product_name': 'all_supermarket_products'
    }, inplace=True)

    # Ensure unique product names in the verification table
    verification_df = verification_df.drop_duplicates(subset=['all_supermarket_products'])

    try:
        local_engine = get_db_engine('hub')
        
        with local_engine.connect() as conn:
            drop_query = "DROP TABLE IF EXISTS public.supermarket_verification CASCADE"
            conn.execute(text(drop_query))
            conn.commit()
            print("  🗑️ Removed existing verification table if it existed")

            # Create verification table
            create_query = """
            CREATE TABLE public.supermarket_verification (
                id SERIAL PRIMARY KEY,
                all_supermarket_products TEXT NOT NULL,
                parent_name TEXT,
                parent_product_id TEXT,
                product_id TEXT,
                created_at TIMESTAMP WITH TIME ZONE
            );
            """
            conn.execute(text(create_query))
            conn.commit()
            print("  ✅ Created supermarket_verification table")
            
            # Insert data into verification table
            verification_df.to_sql(
                name='supermarket_verification',
                con=local_engine,
                schema='public',
                if_exists='append',
                index=False
            )
            
            print(f"  ✅ Populated verification table with {len(verification_df)} unique products")
            
            return verification_df
            
    except Exception as e:
        print(f"  ❌ Error creating verification table: {e}")
        return pd.DataFrame()

def export_verification_to_csv(verification_df):
    """Export verification table to CSV"""
    
    print("\n📄 Exporting verification table to CSV...")
    
    try:
        csv_filename = "supermarket_verification.csv"
        verification_df.to_csv(csv_filename, index=False)
        print(f"✅ Exported verification table to {csv_filename}")
        
    except Exception as e:
        print(f"❌ Error exporting verification table: {e}")

def main():
    """Main function"""
    print("Supermarket Master Table Creation")
    print("=" * 50)
    print("📥 Source: Remote Supabase supermarket_prices table")
    print("🎯 Goal: Create local master table with standardization")
    print("=" * 50)
    
    try:
        # Step 1: Get products from remote Supabase
        products_df = get_supermarket_products()
        if products_df.empty:
            print("No products found. Cannot proceed.")
            return
        
        # Step 2: Create master table
        master_df = create_supermarket_master_table(products_df)
        if master_df.empty:
            print("Failed to create master table")
            return
        
        # Step 3: Write master table to database
        write_master_table_to_db(master_df)
        
        # Step 4: Create verification table
        verification_df = create_verification_table(products_df, master_df)
        
        # Step 5: Export verification to CSV
        export_verification_to_csv(verification_df)
        
        print(f"\n🎯 SUPERMARKET MASTER TABLE CREATED!")
        print(f"📁 Local database: public.supermarket_master table created")
        print(f"📁 Local database: public.supermarket_verification table created")
        print(f"📄 CSV file created: supermarket_verification.csv")
        print(f"📊 Total products processed: {len(products_df)}")
        print(f"📊 Unique products: {len(products_df.drop_duplicates(subset=['raw_product_name']))}")
        print(f"📊 Parent products: {len(master_df)}")
        print(f"🔗 Source: Remote Supabase supermarket_prices table")
        print(f"💾 Destination: Local hub database")
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")

if __name__ == "__main__":
    main()
