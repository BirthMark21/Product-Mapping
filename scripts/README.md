# Product Standardization Scripts

This directory contains modularized scripts for product data management and standardization.

## 📁 Directory Structure

```
scripts/
├── product_listing/           # Scripts for listing products from various sources
│   ├── list_distribution_center_products.py
│   └── list_supabase_table_products.py
├── master_creation/           # Scripts for creating master tables
├── parent_assignment/         # Scripts for assigning parent IDs
└── verification/             # Scripts for verification and analysis
```

## 🚀 Quick Start

### List Products from Supabase Tables

#### Specific Table (Distribution Center)
```bash
cd Mapping
python scripts/product_listing/list_distribution_center_products.py
```

#### Any Supabase Table
```bash
cd Mapping
python scripts/product_listing/list_supabase_table_products.py <table_name>
```

**Available tables:**
- `farm_prices`
- `supermarket_prices`
- `distribution_center_prices`
- `local_shop_prices`
- `ecommerce_prices`
- `sunday_market_prices`

### Examples

```bash
# List products from farm_prices
python scripts/product_listing/list_supabase_table_products.py farm_prices

# List products from supermarket_prices
python scripts/product_listing/list_supabase_table_products.py supermarket_prices

# List products from distribution_center_prices
python scripts/product_listing/list_supabase_table_products.py distribution_center_prices
```

## 📊 Output Files

Each script generates:
- `{table_name}_products.csv` - All products with details
- `{table_name}_unique_products.csv` - Unique product names only

## 🔧 Features

- **Remote Data Fetching** - Gets data from remote Supabase tables
- **Unique Product Detection** - Identifies and counts unique products
- **CSV Export** - Exports data for analysis
- **Product Analysis** - Shows statistics and samples
- **Error Handling** - Robust error handling and reporting

## 📋 Script Categories

### Product Listing (`product_listing/`)
- List products from remote Supabase tables
- Export to CSV for analysis
- Show product statistics and samples

### Master Creation (`master_creation/`)
- Create standardized master tables
- Apply parent-child relationships
- Generate consistent UUIDs

### Parent Assignment (`parent_assignment/`)
- Add parent_product_id columns
- Map products to parent categories
- Update local and remote tables

### Verification (`verification/`)
- Verify parent-child relationships
- Analyze data consistency
- Generate verification reports

## 🎯 Usage Guidelines

1. **Always check database connections** before running scripts
2. **Review output files** to verify data quality
3. **Use specific scripts** for targeted operations
4. **Check logs** for any errors or warnings
5. **Backup data** before making changes

## 🔗 Dependencies

- `pandas` - Data manipulation
- `sqlalchemy` - Database connections
- `python-dotenv` - Environment variables
- `config.setting` - Configuration settings
- `utils.db_connector` - Database connection utilities
