#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import uuid
import re

# Load environment variables
load_dotenv()

# ==============================================================================
#  1. USER PROVIDED MAPPING & LOGIC
# ==============================================================================

NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

PARENT_CHILD_MAPPING = {
    # --- VEGETABLES (Strictly Separated) ---
    "Red Onion A": ["Red Onion A", "Red Onion Grade A", "Red Onion Grade A Restaurant q", "Red Onion Grade A Restaurant quality", "Red Onion", "Red Onion Qelafo", "Pilled Red onion"],
    "Red Onion B": ["Red Onion B", "Red Onion Grade B"],
    "Red Onion C": ["Red Onion C", "Red Onion Grade C", "Red Onion D", "Small Red Onion"],
    "Red Onion Elfora": ["Red Onion Elfora", "Redonion Elfora"],
    "Red Onion (ሃበሻ)": ["Red Onion (ሃበሻ)", "Red onion ( ሃበሻ ) "],
    "Carrot": ["Carrot", "Carrot B", "Small Size Carrot", "Small Carrot", "Small size Carrot"],
    "Tomato A": ["Tomato A", "Tomatoes Grade A", "Tomato", "Tomato Restaurant Quality ", "Tomatoes A", "Tomato Grade A", "Tomato Ripe", "Tomato/ Ripe/ Small size /"],
    "Tomato B": ["Tomato B", "Tomatoes B", "Tomatoes Grade B"],
    "Tomato Roma": ["Tomato Roma", "Tomato Beef"],
    "Potato": ["Potato", "Potatoes", "Potatoes Chips", "Potato for Chips", "Potatoes Restaurant quality", "Potatoes Restaurant Quality", "Potato Chips", "Potato Grade C", "Potato C", "Small Size Potato", "Small size Potato", "Small Size Potato "],
    "Chili Green": ["Chili Green", "Chilly Green", "Chilly Green (Starta)", "Green Chili", "Green Chilli", "Green Chili (ስታርታ)", "Chilly Green (Starter)", "Chilly Green Elfora", "Chilly Green (Elfora)", "Green Pepper", "Indian chilly", "Chilly short"],
    "Beetroot": ["Beetroot", "beetroot", "Beet root", "Beetroot Small Size", "Small & Big Size Beetroot"],
    "White Cabbage": ["White Cabbage", "White Cabbage B", "White Cabbage (Large)", "White Cabbage (Small)", "White Cabbage (medium)", "whitecabbage", "White cabbage", "Cabbage"],
    "Habesha Cabbage": ["Habesha Cabbage", "Habesha cabbage", "Gurage cabbage"],
    "Avocado": ["Avocado", "Avocado A", "Avocado OG", "Avocado Shekaraw", "Avocado B", "Avocado Ripe", "Ripe Avocado", "Local Avocado"],
    "Strawberry": ["Strawberry"],
    "Papaya": ["Papaya", "Papaya B", "Papaya Oversize"],
    "Cucumber": ["Cucumber"],
    "Garlic": ["Garlic", "Garlic B", "garlic", "Garlic (የተፈለፈለ)", "Hatched Garlic", "Garlic China", "Garlic Local", "Garlic Small Size"],
    "Ginger": ["Ginger", "Ginger Local", "Ginger Thai"],
    "Pineapple": ["Pineapple", "pineapple", "Pineapple B"],
    "Mango Apple": ["Mango Apple", "Apple Mango"],
    "Lemon": ["Lemon", "lemon", "Lomen", "Lemon 250g", "Lime"],
    "Orange": ["Orange", "Valencia Orange", "orange Valencia", "Valencia Orange", "Orange Yerer", "Yerer Orange", "Orange Yarer ", "Orange Pineapple "],
    "Apple": ["Apple", "Appel"],
    "Corn": ["Corn"],
    "Mango": ["Mango", "Ye Habeshaa Mango", "Habesha Mango", "Ye Habesha Mango"],
    "Green Beans": ["Fossolia", "Green Beans", "Grean Beans", "Green bean", "Fossolia (250g)"],
    "Lettuce": ["Lettuce", "Chinese lettuce", "Iceberg Salad", "Iceberg", "Lolo rosso"],
    "Spinach": ["spinach", "Spinach", "Swiss chard", "Kale"],
    "Celery": ["Celery", "Parsley", "Leek", "Coriander"],
    "Salad": ["Salad"],
    "Broccoli": ["Broccoli", "Broccolis"],
    "Cauliflower": ["Cauliflower"],
    "Sweet Potato": ["Sweet Potatoes", "sweet potato", "Sweet Potato"],
    "Banana": ["Banana", "Banana/ Raw "],
    "Eggplant": ["Eggplant", "Egg plant", "eggplant"],
    "Zucchini": ["Zucchini", "Courgette", "Courgetti"],
    "Pumpkin": ["Pumpkin"],
    "Squash": ["squash"],
    "Watermelon": ["Watermelon"],

    # --- DACHI PRODUCTS ---
    "Dachi Vimto": ["dachi vimto", "Dachi Vimto"],
    "Dachi Vinegar": ["dachi vinegar"],
    "Dachi Ketchup": ["dachi ketchiup", "Dachi Ketchup", "Dachi Ketchup ", "Dachi Kethup"],
    "Dawedo Pepper": ["Dawed pepper ", "Dawedo Pepper "],

    # --- TEFF ---
    "Shega Red Teff": ["Shega Red Teff"],
    "Shega White Teff": ["Shega White Teff", "Shega Teff"],
    "Ture Red Teff": ["Ture Red Teff"],
    "Ture White Teff": ["Ture White Teff"],

    # --- GRAINS, PULSES & FLOUR ---
    "Ater Kik": ["Ater Kik"],
    "Chickpea": ["Chickpea", "Shembera"],
    "Lentils": ["lentils", "difen misir", "Difen miser (Imported)", "Sambusa Miser"],
    "Aja Kinche": ["Aja kinche", "Aja Kinche"],
    "Barley Kinche": ["Yegebes Kinche", "kinche yegebis", "Kinche Yegebs"],
    "AL-Hinan Flour": ["AL-Hinan Flour", "AL-Hinan flour", "AL-Hinan Food Complex"],
    "Bethel Flour": ["bethel flour", "Bethel Flour"],
    "Wakene Flour": ["wakene flour", "Wakene flour", "Wakene 1st Level Flour"],
    "DK Geda Flour": ["DK GEDA FLOUR", "Dh geda flour"],

    # --- OILS ---
    "Tena Cooking Oil": ["Tena Cooking oil", "Tena cooking oil 1L", "Tena cooking oil 5L"],
    "Aluu Sunflower Oil": ["Aluu Pure Sunflower Oil", "Aluu Sunflower Oil"],
    "Omar Sunflower Oil": ["Omaar sunflower oil", "Omar Sunflower Oil", "omaar ghee(500g)", "Omaar Pure Vegetable Ghee"],
    "Dania Oil": ["Dania oils"],
    "Inci Oil": ["inci oil", "Inci sunflower oil"],
    "Bahja Sunflower Oil": ["Bahja Sunflower Oil"],
    "Bizce Sunflower Oil": ["Bizce Sunflower Oil"],
    "Hilwa Sunflower Oil": ["Hilwa Sunflower Oil"],

    # --- WATER ---
    "Victory Water": ["Victory water 1L", "Victory water 2L", "Victory Natural Water/Pack", "Victory Water ", "victory purified natural water"],
    "Yes Water": ["Yes Water", "Yes water 0.33lt", "Yes Water 1L", "Yes water 2L", "Yes water 500ml", "Yes Water (2L)", "Yes Water (Pack)"],
    "Gold Water": ["Gold Water"],
    "Top Bottled Water": ["Top Bottled Water"],

    # --- TEA & COFFEE ---
    "Addis Tea": ["Addis tea", "Addis Tea", "Addis Tea Bag"],
    "Black Lion Tea": ["Black lion tea", "Black Lion (40g)", "Black Lion Tea (80g)"],
    "Wush Wush Tea": ["wush wush tea", "Wush Wush Tea"],
    "Simba Tea": ["Simba tea bag", "Simaba tea bag tyhme", "Simba Tea Bag"],
    "Girum Cinnamon Tea Bag": ["Girum Cinamon Tea Bag", "Girum Cinnamon Tea Bag "],
    "AMG Coffee": ["AMG coffee", "Amg Coffee 50g", "AMG Coffee (Grinded)", "AMG Coffee (Roasted)"],
    "Mawi Coffee": ["Mawi Coffee ", "Mawi coffee "],
    "Akkoo Coffee": ["Akkoo Coffee"],

    # --- DAIRY & HONEY ---
    "Cheese": ["Cheese", "አይብ", "Ethiopian Cheese"],
    "Cheese Package": ["Cheese Package", "አይብ ጥቅል"],
    "Butter": ["Butter", "Table Butter", "Shola Table butter", "Tycoon Table Butter", "sheno lega ghee", "Sheno Lega Ghee"],
    "Bonga Honey": ["Bonga Honey", "Bonga Mar"],
    "Ami White Honey": ["ami white honey", "AMI White Honey"],
    "Tesfaye Peanut Butter": ["Tesfaye Peanut Butter", "Tesfaye Peanut"],
    "Zagol Yoghurt": ["Zagol Yoghurt", "Zagol yoghurt"],
    "Habesha Mitin Shiro": ["Habesha Mitin Shiro", "Habsha Mitin Shiro"],

    # --- CLEANING & HYGIENE ---
    "CK Powdered Soap": ["CK laundary soap", "CK Powdered Soap", "Ck Powder Soap", "Ck powdered soap", "ck powderd soap 150g", "ck powderd soap 1kg", "Ck powderd soap 40g", "ck powderd soap 500g", "ck powderd soap 5kg"],
    "Cloud Bleach": ["cloud bleach 5L", "Cloud Bleach", "Cloud Bleach / 1L", "Cloud Bleach / 5L"],
    "Sunny Bleach": ["Sunny Bleach", "Sunny Bleach / 5L", "Sunny Bleach 5 Liter"],
    "YALI Bleach": ["YALI Bleach 5lit ", "Yali Bleach 5L", "YALI Bleach 1 Lit", "YALI Bleach 350ml"],
    "YALI Multi Purpose 2lit": ["YALI Multi Purpose 2lit ", "Yali Multi purpose 2lit"],
    "YALI Multi Purpose 5lit": ["YALI Multi Purpose 5lit", "Yali Multi purpose 5lit"],
    "Ajax Soap": ["ajax soap", "Ajax (Large)"],
    "Duru Soap": ["DURU soap", "Duru Soap", "Duru Soap (180g)"],
    "Lifebuoy Soap": ["life bouy 70g", "Life Buoy ( Big)", "Lifebuoy 70g", "Lifebuoy antibacterial bar soap", "lifebuoy(70g)"],
    "Yeah laundry bar soap": ["YEAH Bar Soap", "Yeah laundry bar soap"],
    "Roll Detergent powder": ["Rol Bio Laundry powder ", "Roll Detergent powder"],
    "Happy Tissue": ["Happy soft", "happy toilet paper", "Happy Toilet Tissue", "Happy Toilet Paper"],
    "Bravo Tissue": ["bravo table tissue", "bravo toilet paper", "Bravo Table Tissue", "Bravo Toilet Paper"],
    "Tulip Toilet Paper": ["Tulip Toilet Paper", "Tulip soft "],

    # --- MEAT & POULTRY ---
    "Lamb": ["Lamb", "Lamp", "የበግ ስጋ", "ጠቦት የበግ ስጋ", "Regular Mutton Package", "Sheep package", "Mutton"],
    "Goat Meat": ["Goat Meat", "የፍየል ስጋ", "Special Goat Package", "Regular Goat Package", "ሙክት የፍየል ስጋ"],
    "Chicken Package": ["Chicken", "Chicken Groceries", "Regular Chicken Package", "Special Chicken Package", "BGS Foreign Chicken", "12 Piece Chicken", "Chicken Package", "Habesha Chicken", "Habesha Chicken Package"],

    # --- PACKAGES ---
    "Difo Package": ["Difo Package", "Difo package ", "ድፎ ጥቅል", "ዳቦ ጥቅል", "Defo Package", "Special Defo package"],
    "Qulet Package": ["Qulet Package", "Qulet package ", "ቁሌት ጥቅል ", "ቁሌት ጥቅል", "Special qulet package"],
    "Kukulu Package": ["Kukulu Package", "Kukulu package", "Special Kululu Package"],

    # --- PASTA & NOODLES ---
    "Indomie Vegetable Noodles": ["Indomie Vegetable Noodles ", "Indomie Vegie Noodles "],
    "MIA Pasta": ["MIA Pasta", "Mia Pasta"],

    # --- BISCUITS ---
    "Moon Vanilla Biscuit": ["Moon Vanilla Biscuit ", "Moon vanilla biscut "],

    # --- STATIONERY (Separated by Brand/Item) ---
    "Afro EIIDE Exercise Book": ["Afro Eiide Exercise book", "Afro EIIDE Exercise Book", "EIIDE Exercise Book (12 pieces)"],
    "Radical Exercise Book": ["Radical Exercise Book/ Pack", "radical exercise book", "Radical Exercise Book/50p"],
    "Buna Pen": ["buna pen"],
    "Buna Pencil": ["Buna pencil"],
    "Pencil": ["Pencil"],
    "Tooth Brush": ["Tooth brush"],

    # --- CLOTHING (Separated by Type) ---
    "Casual T-Shirt": ["Casual T-Shirt"],
    "Cotton T-Shirt": ["Cotton T-Shirt", "Cotton T-shirt"],
    "Printed T-Shirt": ["Printed T-shirt", "printing T-shirt"],
    "T-Shirt": ["T-shirt"],

    # --- BAGS ---
    "Cross-body Bag": ["Cross-body Bag", "Crossbody Bag"],

    # --- BABY PRODUCTS ---
    "ABC Diaper": ["ABC Diaper", "ABC Daiper"],

    # --- KITS (Separated by Team) ---
    "Arsenal Kit": ["Arsenal 2024/2025 Kit"],
    "Liverpool Kit": ["Liverpool 2024/2025 Kit"],
    "Manchester City Kit": ["Manchester City 2024/2025 Kit"],
    "Manchester United Kit": ["Manchester United 2024/2025 Kit"]
}


def _create_child_to_parent_map(mapping):
    """Creates a reverse mapping from a cleaned child name to its parent name."""
    child_map = {}
    for parent, children in mapping.items():
        for child in children:
            cleaned_child = re.sub(r'\s+', ' ', child).strip().lower()
            child_map[cleaned_child] = parent
    return child_map

CHILD_TO_PARENT_MAP = _create_child_to_parent_map(PARENT_CHILD_MAPPING)


def _generate_stable_uuid(name):
    """Generates a consistent UUID5 based on a given name."""
    return str(uuid.uuid5(NAMESPACE_UUID, name))


def create_parent_child_master_table(all_product_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the canonical_product_master table with OPTION 3 structure:
    - parent_product_id: UUID for the parent
    - parent_name: canonical parent product name
    - created_at: earliest timestamp for this parent
    
    ONE ROW PER PARENT (minimal, clean structure)
    Child information is stored in source tables via parent_product_id foreign key.
    """
    print("\n--- Generating canonical_product_master Table (Option 3) ---")

    if all_product_data_df.empty:
        print(" -> Input data is empty. No master table to generate.")
        return pd.DataFrame()

    df = all_product_data_df.copy().dropna(subset=['raw_product_name'])
    
    # Filter out invalid entries
    initial_count = len(df)
    df = df[df['raw_product_name'].astype(str).str.strip() != '0']
    final_count = len(df)
    if initial_count > final_count:
        print(f" -> Filtered out {initial_count - final_count} records where product name was '0'.")

    print("Step 1: Mapping products to parents using PARENT_CHILD_MAPPING...")
    df['cleaned_name'] = df['raw_product_name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower())
    df['parent_name'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
    
    # For unmapped products, use the original name as parent
    df['parent_name'] = df['parent_name'].fillna(df['raw_product_name'])

    print("Step 2: Converting created_at to consistent format...")
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    
    print("Step 3: Aggregating by parent to get earliest created_at and source...")
    # Group by parent and get the earliest created_at and first source
    parent_groups = df.groupby('parent_name').agg({
        'created_at': lambda x: x.dropna().min() if not x.dropna().empty else None,
        'source_db': lambda x: x.iloc[0] if len(x) > 0 else None
    }).reset_index()
    
    print("Step 4: Generating parent_product_id...")
    parent_groups['parent_product_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
    
    print("Step 5: Finalizing canonical master table...")
    # Select final columns in the correct order
    canonical_df = parent_groups[[
        'parent_product_id',
        'parent_name',
        'source_db',
        'created_at'
    ]].copy()
    
    # Rename source_db to source
    canonical_df = canonical_df.rename(columns={'source_db': 'source'})
    
    # Sort by parent_name
    canonical_df = canonical_df.sort_values('parent_name').reset_index(drop=True)
    
    print(f" -> Created canonical_product_master with {len(canonical_df)} parent products.")
    print(f" -> Child products are linked via parent_product_id in source tables.")
    return canonical_df

# ==============================================================================
#  2. SUPABASE FETCH & EXECUTION LOGIC
# ==============================================================================


import sys
# Add parent directory to path to reach utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_connector import get_db_engine
import requests
import json
import uuid

def main():
    try:
        supabase_engine = get_db_engine('supabase')
        hub_engine = get_db_engine('hub')
        
        print("\n🚀 CONNECTING TO REMOTE SOURCES...")

        # 1. Fetch Supabase Data (Remote - ONLY products)
        print("🌐 Fetching from Remote Supabase (products)...")
        with supabase_engine.connect() as conn:
            df_sup_p = pd.read_sql("SELECT id as raw_product_id, name as raw_product_name, created_at FROM products", conn)
        print(f"✅ Supabase: Fetched {len(df_sup_p)} products.")

        # 2. Fetch ClickHouse Data (Remote via API - product_names & products)
        print("🌐 Fetching from Remote ClickHouse (API)...")
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}

        def fetch_ch(table):
            payload = {
                "client_id": f"p_{uuid.uuid4().hex[:6]}", "database_id": 1, "json": True, "runAsync": False,
                "schema": "chipchip", "sql": f'SELECT id as raw_product_id, name as raw_product_name, toString(created_at) as created_at FROM "chipchip"."{table}"',
                "expand_data": True
            }
            res = requests.post(f"{superset_url}/api/v1/sqllab/execute/", headers=headers, json=payload, verify=False)
            if res.status_code == 200:
                data = res.json().get('data', [])
                return pd.DataFrame(data)
            return pd.DataFrame(columns=['raw_product_id', 'raw_product_name', 'created_at'])

        df_ch_names = fetch_ch("product_names")
        df_ch_products = fetch_ch("products")
        print(f"✅ ClickHouse: Fetched {len(df_ch_names)} product_names and {len(df_ch_products)} products.")

        # 3. Combine ALL Data
        all_product_data = pd.concat([df_sup_p, df_ch_names, df_ch_products], ignore_index=True)
        
        # 4. Generate the Canonical Master Table
        master_df = create_parent_child_master_table(all_product_data)

        if master_df.empty:
            print("❌ Standardization failed. No data to save.")
            return

        # 5. Create a Lookup for Parent IDs (raw_name -> parent_id)
        # We use a case-insensitive, whitespace-trimmed approach for safety
        print("\n📝 Creating mapping lookup...")
        
        # Helper to get parent_id for many names
        def get_parent_id(name):
            if not name: return None
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned)
            if not parent_name:
                # Fallback to identify unmapped canonical name (same logic as standardization)
                parent_name = name # For unmapped, the name is its own parent
            
            # Stable UUID from the parent name
            return str(_generate_stable_uuid(parent_name))

        # Helper to get parent name and status for the verification table
        def get_parent_status(name):
            if not name: return pd.Series([None, "❌"])
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned)
            if parent_name:
                return pd.Series([parent_name, "✅"])
            else:
                return pd.Series([name, "❌"]) # If not mapped, it's its own parent, status is '❌'

        # 6. Update Local PostgreSQL Tables in pgAdmin
        print("\n💾 SAVING RESULTS TO LOCAL POSTGRES (HUB)...")
        
        with hub_engine.begin() as conn:
            # A. Update supabase_products local copy
            print(" -> [1/4] Updating local 'supabase_products'...")
            local_sup_p = pd.read_sql("SELECT * FROM supabase_products", conn)
            # Only add the ID column
            local_sup_p['parent_product_id'] = local_sup_p['name'].apply(get_parent_id)
            local_sup_p.to_sql('supabase_products', conn, if_exists='replace', index=False, method='multi', chunksize=500)

            # B. Update clickhouse_product_names local copy
            print(" -> [2/4] Updating local 'clickhouse_product_names'...")
            local_ch_p = pd.read_sql("SELECT * FROM clickhouse_product_names", conn)
            # Only add the ID column
            local_ch_p['parent_product_id'] = local_ch_p['name'].apply(get_parent_id)
            local_ch_p.to_sql('clickhouse_product_names', conn, if_exists='replace', index=False, method='multi', chunksize=500)

            # C. Save the final Canonical Master Table
            print(" -> [3/4] Creating 'canonical_product_master' table...")
            master_df.to_sql('canonical_product_master', conn, if_exists='replace', index=False, method='multi', chunksize=500)

            # 7. Create the requested "all_product_master_Table" (The Verification Table)
            print(" -> [4/4] Creating 'all_product_master_Table'...")
            df_view_sup = df_sup_p[['raw_product_name']].copy()
            df_view_ch = df_ch_names[['raw_product_name']].copy()
            consolidated_view = pd.concat([df_view_sup, df_view_ch], ignore_index=True)
            consolidated_view.rename(columns={'raw_product_name': 'source_product_name'}, inplace=True)
            consolidated_view.drop_duplicates(subset=['source_product_name'], inplace=True)
            
            # This is the ONLY table that gets the 'Has parent?' column
            consolidated_view[['assign_parent_name', 'Has parent?']] = consolidated_view['source_product_name'].apply(get_parent_status)
            consolidated_view.to_sql('all_product_master_Table', conn, if_exists='replace', index=False)

        print("\n" + "="*80)
        print("✅ SUCCESS: ALL TABLES UPDATED IN LOCAL HUB!")
        print("   - Table: supabase_products (parent_product_id)")
        print("   - Table: clickhouse_product_names (parent_product_id)")
        print("   - Table: canonical_product_master (Master parent list)")
        print("   - Table: all_product_master_Table (Mapping Status Report + Has parent?)")
        print("="*80)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
