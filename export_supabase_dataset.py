#!/usr/bin/env python3
"""
Fetches data for 15 specific TOP products from three price tables,
retrieves their corresponding Product IDs from the 'products' table,
cleans names, calculates prices, and exports a single, unified CSV file
containing the date, product_id, name, price, and location details.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import re

# Load environment variables from .env file
load_dotenv()

# ==============================================================================
# 1. TARGET PRODUCTS AND NAME NORMALIZATION
# ==============================================================================

# The user-defined list of the 15 most important products.
TOP_15_PRODUCTS = [
    "Potato", "Tomato A", "Red Onion A", "Avocado", "Carrot", "White Cabbage",
    "Red Onion (ሃበሻ)", "Red Onion B", "Beetroot", "Garlic", "Papaya", "Red Onion",
    "Chili Green", "Green Beans", "Red Onion Elfora"
]

# Complete normalization map to identify all variations
NAME_NORMALIZATION_MAP = {
  "Red Onion A": ["Red Onion A", "Red Onion Grade A", "Red Onion Grade A Restaurant q", "Red Onion Grade A Restaurant quality", "Red Onion"],
  "Red Onion B": ["Red Onion B", "Red Onion Grade B"],
  "Red Onion C": ["Red Onion C", "Red Onion Grade C" , "Red Onion D"],
  "Red Onion Elfora": ["Red Onion Elfora" , "Redonion Elfora"],
  "Carrot": ["Carrot", "Carrot B"],
  "Tomato A": ["Tomato A", "Tomatoes Grade A", "Tomato", "Tomato Restaurant Quality ", "Tomatoes A", "Tomato Grade A"],
  "Tomato B": ["Tomato B", "Tomatoes B", "Tomatoes Grade B"],
  "Potato": ["Potato", "Potatoes", "Potatoes Chips", "Potato for Chips",  "Potatoes Restaurant quality", "Potatoes Restaurant Quality", "Potato Chips"],
  "Chili Green": ["Chili Green", "Chilly Green", "Chilly Green (Starta)", "Green Chili", "Green Chilli", "Green Chili (ስታርታ)","Chilly Green (Starter)", "Chilly Green Elfora", "Chilly Green (Elfora)"],
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


# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================

def get_supabase_engine():
    """Establishes and returns a connection engine to the PostgreSQL database."""
    try:
        conn_str = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
        return create_engine(conn_str)
    except Exception as e:
        print(f"❌ Error creating database engine: {e}")
        return None

def create_reverse_normalization_map(norm_map):
    """Creates a reverse map from a variation to its standard product name."""
    reverse_map = {}
    for standard_name, variations in norm_map.items():
        for variation in variations:
            reverse_map[str(variation).lower().strip()] = standard_name
    return reverse_map

def standardize_product_names(series, reverse_map):
    """Standardizes product names in a pandas Series using the reverse map."""
    cleaned_series = series.astype(str).str.lower().str.strip()
    return cleaned_series.map(reverse_map).fillna(series) # Keep original if no map found

def fetch_product_id_map(engine, reverse_map):
    """
    Fetches the 'products' table and creates a map from standardized
    product name to product ID.
    """
    print("\n📦 Fetching product data to create ID map...")
    query = text("SELECT id, name FROM public.products")
    try:
        with engine.connect() as conn:
            products_df = pd.read_sql(query, conn)
        
        products_df['standard_name'] = standardize_product_names(products_df['name'], reverse_map)
        products_df.drop_duplicates(subset=['standard_name'], keep='first', inplace=True)
        
        name_to_id_map = pd.Series(products_df.id.values, index=products_df.standard_name).to_dict()
        print(f"✅ Created map for {len(name_to_id_map)} unique products.")
        return name_to_id_map
        
    except Exception as e:
        print(f"❌ Error fetching products table: {e}")
        return {}

def fetch_filtered_table_data(engine, table_name, start_date, end_date):
    """Fetches data from a given price table within a specific date range."""
    print(f"\n🔍 Fetching data from {table_name} ({start_date.date()} → {end_date.date()})...")
    query = text("SELECT * FROM public.{} WHERE date BETWEEN :start_date AND :end_date".format(table_name))
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})
        print(f"✅ {len(df):,} records fetched from {table_name}")
        return df
    except Exception as e:
        print(f"❌ Error fetching {table_name}: {e}")
        return pd.DataFrame()

# ==============================================================================
# 3. MAIN DATA PROCESSING AND EXPORT
# ==============================================================================

def create_unified_dataset():
    """Main function to orchestrate the data fetching, processing, and exporting."""
    print("🚀 Starting TOP 15 PRODUCTS Unified Dataset Creation")
    print("=" * 70)
    
    start_date = pd.to_datetime("2025-07-01")
    end_date = pd.to_datetime("2025-08-31")
    
    engine = get_supabase_engine()
    if engine is None: return

    reverse_map = create_reverse_normalization_map(NAME_NORMALIZATION_MAP)
    product_id_map = fetch_product_id_map(engine, reverse_map)
    if not product_id_map:
        print("❌ Aborting: Could not create product ID map.")
        return

    tables_to_unify = ["farm_prices", "supermarket_prices", "distribution_center_prices"]
    all_dfs = []

    # --- MAIN PROCESSING LOOP for price tables ---
    for table_name in tables_to_unify:
        df = fetch_filtered_table_data(engine, table_name, start_date, end_date)
        if df.empty:
            continue

        df['product_name'] = standardize_product_names(df['product_name'], reverse_map)
        df = df[df['product_name'].isin(TOP_15_PRODUCTS)].copy()
        
        print(f"    - Found {len(df):,} records for TOP 15 products in {table_name}")
        if df.empty:
            continue

        df['source_market_type'] = table_name.replace('_prices', '').replace('_', ' ').title()

        if table_name == 'farm_prices':
            cost_components = ['farm_price_per_kg', 'sack_crate_cost_per_kg', 'loading_cost_per_kg', 'transportation_cost_per_kg', 'border_cost_per_kg', 'agent_cost_per_kg', 'miscellaneous_costs', 'vat']
            for col in cost_components:
                if col not in df.columns: df[col] = 0
            df['price'] = df[cost_components].fillna(0).sum(axis=1)
            df['location_detail'] = df.get('region', '') + ' - ' + df.get('origin', '')
        else:
            df['location_detail'] = df['location']
        
        # *** KEY CHANGE: Define columns to keep, excluding the record's primary key ***
        cols_to_keep = [
            'product_name', 'price', 'date', 'source_market_type',
            'location_detail', 'product_remark', 'created_at'
        ]
        
        existing_cols = [col for col in cols_to_keep if col in df.columns]
        all_dfs.append(df[existing_cols])

    if not all_dfs:
        print("\n❌ No data found for the TOP 15 products. Aborting.")
        return

    # --- Combine, Add Product ID, and Export ---
    print("\n🔗 Combining price tables...")
    final_df = pd.concat(all_dfs, ignore_index=True)

    print("🔑 Adding 'product_id' using the map...")
    final_df['product_id'] = final_df['product_name'].map(product_id_map)

    # --- Reorder columns for final output ---
    final_column_order = [
        'date',
        'product_id',
        'product_name',
        'price',
        'source_market_type',
        'location_detail',
        'product_remark',
        'created_at'
    ]
    final_column_order = [col for col in final_column_order if col in final_df.columns]
    final_df = final_df[final_column_order]

    export_folder = "chipoai_unified_export"
    os.makedirs(export_folder, exist_ok=True)
    export_path = os.path.join(export_folder, "unified_price_data_top_15.csv")
    
    final_df.to_csv(export_path, index=False, date_format='%Y-m-d %H:%M:%S')
    
    print("\n" + "="*50)
    print("📊 FINAL EXPORT SUMMARY")
    print("="*50)
    print(f"✅ Exported a clean, unified dataset with 'product_id'.")
    print(f"   - Filename: {export_path}")
    print(f"   - Total Rows: {len(final_df):,}")
    print(f"   - Total Columns: {len(final_df.columns)}")
    print("\nFinal Columns in the Dataset:")
    for col in final_df.columns:
        print(f"  - {col}")
    print("="*50)
    print("\n🎉 Unified dataset creation completed successfully!")

if __name__ == "__main__":
    create_unified_dataset()