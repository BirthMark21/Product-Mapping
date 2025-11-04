import pandas as pd
import uuid
import re

NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

# --- MAPPING UPDATED with your new entries ---
PARENT_CHILD_MAPPING = {
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
  "Goat Meat": ["Goat Meat", "የፍየል ስጋ", "Special Goat Package", "Regular Goat Package", "ሙክት የፍየል ስጋ"], # Updated
  "Lamb": ["Lamb", "Lamp", "የበግ ስጋ", "ጠቦት የበግ ስጋ", "Regular Mutton Package", "Sheep package"],
  "Bravo Table Tissue": ["Bravo Table Tissue", "Bravo Toilet Paper"],
  "Habesha Mitin Shiro": ["Habesha Mitin Shiro", "Habsha Mitin Shiro"],
  "Chicken Package": ["Chicken Groceries", "Regular Chicken Package", "Special Chicken Package", "BGS Foreign Chicken", "12 Piece Chicken", "Chicken Package", "Habesha Chicken", "Habesha Chicken Package"], # Updated
  "Tesfaye Peanut Butter": ["Tesfaye Peanut Butter", "Tesfaye Peanut"] # Updated
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
    Takes a DataFrame of raw product data and creates a standardized master table
    with parent-child relationships.
    """
    print("\n--- Generating Master Table with Parent-Child Structure ---")

    if all_product_data_df.empty:
        print(" -> Input data is empty. No master table to generate.")
        return pd.DataFrame()

    df = all_product_data_df.copy().dropna(subset=['raw_product_name'])
    
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
    parent_groups['all_possible_child_names'] = parent_groups['parent_name'].map(PARENT_CHILD_MAPPING)
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