#!/usr/bin/env python3
"""
Script to populate the 6 Supabase tables with sample data
Run this after creating the tables
"""

import pandas as pd
import uuid
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import random

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'master-db',
    'user': 'postgres',
    'password': 'birthmark21'
}

def get_db_engine():
    """Create database engine"""
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(conn_str)

def generate_sample_data():
    """Generate sample data for all tables"""
    
    # Sample products
    products = [
        "Apple", "Banana", "Orange", "Tomato", "Potato", "Onion", "Carrot", "Cabbage",
        "Lettuce", "Cucumber", "Bell Pepper", "Chili", "Garlic", "Ginger", "Lemon",
        "Avocado", "Mango", "Pineapple", "Strawberry", "Grape", "Rice", "Wheat",
        "Corn", "Beans", "Lentils", "Chicken", "Beef", "Fish", "Eggs", "Milk"
    ]
    
    # Sample locations
    regions = ["Addis Ababa", "Oromia", "Amhara", "Tigray", "SNNPR", "Sidama", "Harari"]
    origins = ["Ethiopian Farm", "Local Producer", "Cooperative", "Private Farm", "Government Farm"]
    suppliers = ["ABC Farm", "Green Valley", "Mountain View", "River Side", "Sunshine Farm"]
    supermarkets = ["Shimelis Supermarket", "Ethio Mart", "City Mall", "Central Market", "Fresh Foods"]
    distribution_centers = ["Central Distribution", "North Hub", "South Hub", "East Logistics", "West Supply"]
    local_shops = ["Corner Store", "Neighborhood Shop", "Local Market", "Community Store", "Family Shop"]
    ecommerce_sites = ["Jumia", "Souq", "Amazon", "eBay", "Local Online"]
    markets = ["Mercato", "Shiro Meda", "Addis Mercato", "Bole Market", "Piazza Market"]
    
    sample_data = {}
    
    # 1. FARM_PRICES - Most detailed
    farm_data = []
    for i in range(50):
        product = random.choice(products)
        base_price = round(random.uniform(10, 100), 2)
        
        farm_data.append({
            'product_name': product,
            'region': random.choice(regions),
            'origin': random.choice(origins),
            'source_type': 'direct from farm',
            'supplier_name': random.choice(suppliers),
            'farm_price_per_kg': base_price,
            'sack_crate_cost_per_kg': round(base_price * 0.05, 2),
            'loading_cost_per_kg': round(base_price * 0.03, 2),
            'transportation_cost_per_kg': round(base_price * 0.15, 2),
            'border_cost_per_kg': round(base_price * 0.02, 2),
            'agent_cost_per_kg': round(base_price * 0.08, 2),
            'miscellaneous_costs': round(base_price * 0.05, 2),
            'vat': round(base_price * 0.15, 2),
            'product_remark': random.choice(['Fresh', 'Organic', 'Premium Quality', 'Grade A', 'Local']),
            'date': (date.today() - timedelta(days=random.randint(0, 30))).isoformat(),
            'price': round(base_price * 1.5, 2)
        })
    
    sample_data['farm_prices'] = farm_data
    
    # 2. SUPERMARKET_PRICES
    supermarket_data = []
    for i in range(100):
        product = random.choice(products)
        supermarket_data.append({
            'product_name': product,
            'price': round(random.uniform(15, 150), 2),
            'location': random.choice(supermarkets),
            'product_remark': random.choice(['On Sale', 'Fresh', 'Organic', 'Premium', 'Regular']),
            'date': (date.today() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    
    sample_data['supermarket_prices'] = supermarket_data
    
    # 3. DISTRIBUTION_CENTER_PRICES
    distribution_data = []
    for i in range(80):
        product = random.choice(products)
        distribution_data.append({
            'product_name': product,
            'price': round(random.uniform(12, 120), 2),
            'location': random.choice(distribution_centers),
            'product_remark': random.choice(['Wholesale', 'Bulk', 'Fresh', 'Quality Assured']),
            'date': (date.today() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    
    sample_data['distribution_center_prices'] = distribution_data
    
    # 4. LOCAL_SHOP_PRICES
    local_shop_data = []
    for i in range(120):
        product = random.choice(products)
        local_shop_data.append({
            'product_name': product,
            'price': round(random.uniform(20, 200), 2),
            'location': random.choice(local_shops),
            'product_remark': random.choice(['Local', 'Fresh', 'Daily', 'Community']),
            'date': (date.today() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    
    sample_data['local_shop_prices'] = local_shop_data
    
    # 5. ECOMMERCE_PRICES
    ecommerce_data = []
    for i in range(90):
        product = random.choice(products)
        ecommerce_data.append({
            'product_name': product,
            'price': round(random.uniform(25, 250), 2),
            'location': random.choice(ecommerce_sites),
            'product_remark': random.choice(['Free Shipping', 'Express Delivery', 'Online Only', 'Digital']),
            'date': (date.today() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    
    sample_data['ecommerce_prices'] = ecommerce_data
    
    # 6. SUNDAY_MARKET_PRICES
    sunday_market_data = []
    for i in range(110):
        product = random.choice(products)
        sunday_market_data.append({
            'product_name': product,
            'price': round(random.uniform(10, 180), 2),
            'location': random.choice(markets),
            'product_remark': random.choice(['Weekend Special', 'Fresh', 'Local', 'Traditional']),
            'date': (date.today() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    
    sample_data['sunday_market_prices'] = sunday_market_data
    
    return sample_data

def populate_tables():
    """Populate all tables with sample data"""
    engine = get_db_engine()
    
    print("Generating sample data...")
    sample_data = generate_sample_data()
    
    print("Populating tables with sample data...")
    
    for table_name, data in sample_data.items():
        try:
            df = pd.DataFrame(data)
            df.to_sql(
                name=table_name,
                con=engine,
                schema='public',
                if_exists='append',
                index=False
            )
            print(f"✅ Populated {table_name}: {len(data)} records")
        except Exception as e:
            print(f"❌ Error populating {table_name}: {e}")
    
    print("\n🎉 All tables populated successfully!")
    
    # Show summary
    with engine.connect() as conn:
        for table_name in sample_data.keys():
            result = conn.execute(text(f"SELECT COUNT(*) FROM public.{table_name}"))
            count = result.scalar()
            print(f"📊 {table_name}: {count} records")

if __name__ == "__main__":
    try:
        populate_tables()
        print("\n✅ Database population completed!")
        print("You can now run your ETL pipeline to process this data.")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Please check your database connection settings.")
