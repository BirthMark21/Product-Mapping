#!/usr/bin/env python3
"""
Master script to set up the complete database with tables and sample data
This script will:
1. Create all 6 Supabase tables in your local master-db
2. Populate them with sample data
3. Verify the setup
"""

import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from create_tables import create_tables
from populate_tables import populate_tables

def main():
    """Main function to set up the database"""
    print("🚀 Setting up Supabase tables in local master-db...")
    print("=" * 60)
    
    try:
        # Step 1: Create tables
        print("\n📋 Step 1: Creating tables...")
        create_tables()
        
        # Step 2: Populate with sample data
        print("\n📊 Step 2: Populating with sample data...")
        populate_tables()
        
        print("\n" + "=" * 60)
        print("🎉 DATABASE SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\n📋 What was created:")
        print("✅ farm_prices - Detailed farm cost breakdowns")
        print("✅ supermarket_prices - Retail supermarket prices")
        print("✅ distribution_center_prices - Wholesale distribution prices")
        print("✅ local_shop_prices - Local shop retail prices")
        print("✅ ecommerce_prices - Online e-commerce prices")
        print("✅ sunday_market_prices - Sunday market prices")
        print("\n🚀 You can now run your ETL pipeline:")
        print("   python main.py")
        
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        print("Please check your database connection settings.")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Setup completed successfully!")
    else:
        print("\n❌ Setup failed. Please check the errors above.")
        sys.exit(1)
