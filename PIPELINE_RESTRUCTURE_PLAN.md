# Pipeline Restructure Plan & Recommendations

## 📋 Current State Analysis

### **6 Data Sources Identified:**
1. `distribution_center_prices`
2. `ecommerce_prices`
3. `farm_prices`
4. `local_shop_prices`
5. `sunday_market_prices`
6. `supermarket_prices`

### **Duplicate Scripts Identified:**

#### **Pattern 1: Per-Source Scripts (6 folders × 5 scripts = 30 files)**
Each source has:
- `list_*_products.py` - Fetch from Supabase → Local Hub
- `create_*_master.py` - Create master table with standardization
- `add_parent_id_to_*.py` - Add parent_id to LOCAL table
- `add_parent_id_to_remote_*.py` - Add parent_id to REMOTE Supabase table
- `export_parent_products.py` - Export parent products to CSV

**Total: 30 scripts** (highly duplicated logic)

#### **Pattern 2: Updates Folder (11 scripts)**
- `add_parent_id_to_all_supabase_tables.py` - Does what individual scripts do
- `add_parent_ids_local.py` - Duplicate functionality
- `add_parent_product_id_local.py` - Duplicate functionality
- `add_parent_product_id_to_local.py` - Duplicate functionality
- `add_supabase_products_to_standardization.py` - Overlaps with create_*_master
- `write_parent_products_to_local.py` - Overlaps with export
- `sync_local_shop_supabase_to_local.py` - Overlaps with list_* scripts
- Others...

**Total: 11 scripts** (mostly duplicates)

---

## 🎯 Recommended Structure

### **New Unified Pipeline Architecture:**

```
Mapping/
├── pipeline/
│   ├── production_etl.py (EXISTING - keep)
│   ├── unified_source_pipeline.py (NEW - unified pipeline)
│   ├── data_loader.py (EXISTING - enhance)
│   ├── standardization.py (EXISTING - keep)
│   └── db_writer.py (EXISTING - keep)
│
├── pipeline/
│   └── sources/ (NEW - modular source handlers)
│       ├── __init__.py
│       ├── base_source.py (base class)
│       ├── distribution_center.py
│       ├── ecommerce.py
│       ├── farm_prices.py
│       ├── local_shop.py
│       ├── sunday_market.py
│       └── supermarket.py
│
├── scripts/
│   ├── legacy/ (MOVE old scripts here)
│   │   ├── distribution_center/
│   │   ├── ecommerce/
│   │   ├── farm_prices/
│   │   ├── local_shop/
│   │   ├── sunday_market/
│   │   ├── supermarket/
│   │   └── updates/
│   │
│   └── utilities/ (Keep utility scripts)
│       ├── listing/
│       ├── dynamic_mapping/
│       └── ... (other utilities)
│
└── config/
    └── setting.py (ENHANCE - add source config)
```

---

## 🔧 Implementation Plan

### **Phase 1: Create Unified Pipeline**

**File: `pipeline/unified_source_pipeline.py`**

**Features:**
- ✅ Command-line flags: `--distribution-center`, `--ecommerce`, `--farm-prices`, `--local-shop`, `--sunday-market`, `--supermarket`
- ✅ Or use `--all` to process all sources
- ✅ Fetches from Supabase tables
- ✅ Applies standardization (reuses existing logic)
- ✅ Migrates to local PostgreSQL Hub
- ✅ Adds `parent_product_id` to LOCAL tables
- ✅ **DISABLED**: Remote Supabase parent_id updates (flag: `--enable-remote-updates`)

**Usage Examples:**
```bash
# Process single source
python -m pipeline.unified_source_pipeline --supermarket

# Process multiple sources
python -m pipeline.unified_source_pipeline --supermarket --farm-prices --ecommerce

# Process all sources
python -m pipeline.unified_source_pipeline --all

# Process all + enable remote updates (disabled by default)
python -m pipeline.unified_source_pipeline --all --enable-remote-updates
```

### **Phase 2: Create Modular Source Handlers**

**File: `pipeline/sources/base_source.py`**
- Base class with common methods:
  - `extract_from_supabase()`
  - `create_master_table()`
  - `add_parent_id_to_local()`
  - `add_parent_id_to_remote()` (disabled by default)

**File: `pipeline/sources/supermarket.py`** (example)
- Inherits from `base_source.py`
- Source-specific configuration:
  - Table name: `supermarket_prices`
  - Master table: `supermarket_master`
  - Local table: `supermarket_prices`

**Repeat for:** distribution_center, ecommerce, farm_prices, local_shop, sunday_market

### **Phase 3: Enhance Config**

**File: `config/setting.py`** (additions)

```python
# Source table configuration
SOURCE_TABLES = {
    'distribution_center': {
        'supabase_table': 'distribution_center_prices',
        'local_table': 'distribution_center_prices',
        'master_table': 'distribution_center_master',
        'enabled': True
    },
    'ecommerce': {
        'supabase_table': 'ecommerce_prices',
        'local_table': 'ecommerce_prices',
        'master_table': 'ecommerce_master',
        'enabled': True
    },
    'farm_prices': {
        'supabase_table': 'farm_prices',
        'local_table': 'farm_prices',
        'master_table': 'farm_prices_master',
        'enabled': True
    },
    'local_shop': {
        'supabase_table': 'local_shop_prices',
        'local_table': 'local_shop_prices',
        'master_table': 'local_shop_master',
        'enabled': True
    },
    'sunday_market': {
        'supabase_table': 'sunday_market_prices',
        'local_table': 'sunday_market_prices',
        'master_table': 'sunday_market_master',
        'enabled': True
    },
    'supermarket': {
        'supabase_table': 'supermarket_prices',
        'local_table': 'supermarket_prices',
        'master_table': 'supermarket_master',
        'enabled': True
    }
}
```

---

## 📊 Workflow

### **Unified Pipeline Flow:**

```
1. Parse command-line flags → Determine which sources to process
2. For each enabled source:
   a. Extract from Supabase table
   b. Validate data (using existing ProductDataValidator)
   c. Apply standardization (using existing standardization.py logic)
   d. Create master table in Local Hub
   e. Migrate raw data to Local Hub table
   f. Add parent_product_id to LOCAL table
   g. [DISABLED] Add parent_product_id to REMOTE Supabase table
3. Generate summary report
```

---

## 🗑️ Scripts to Archive

### **Move to `scripts/legacy/`:**

**All per-source scripts:**
- `scripts/distribution_center/*`
- `scripts/ecommerce/*`
- `scripts/farm_prices/*`
- `scripts/local_shop/*`
- `scripts/sunday_market/*`
- `scripts/supermarket/*`

**Duplicate scripts in `scripts/updates/`:**
- `add_parent_id_to_all_supabase_tables.py` (replaced by unified pipeline)
- `add_parent_ids_local.py` (replaced)
- `add_parent_product_id_local.py` (replaced)
- `add_parent_product_id_to_local.py` (replaced)
- `write_parent_products_to_local.py` (replaced)
- `sync_local_shop_supabase_to_local.py` (replaced)

**Keep:**
- `scripts/listing/*` (utility scripts)
- `scripts/dynamic_mapping/*` (separate feature)
- `scripts/seed_mappings_from_dict.py` (utility)

---

## ✅ Benefits

1. **Single Entry Point**: One pipeline handles all sources
2. **No Duplication**: Reuse standardization logic
3. **Flexible**: Enable/disable sources via flags
4. **Safe**: Remote updates disabled by default
5. **Maintainable**: Changes in one place affect all sources
6. **Testable**: Each source handler can be tested independently

---

## 🚀 Next Steps

1. **Review & Approve** this structure
2. **Create** `pipeline/sources/` module
3. **Implement** `unified_source_pipeline.py`
4. **Test** with one source (e.g., supermarket)
5. **Migrate** remaining sources
6. **Archive** old scripts to `scripts/legacy/`
7. **Update** documentation

---

## 📝 Notes

- **Remote Updates**: Disabled by default. Enable with `--enable-remote-updates` flag when ready.
- **Backward Compatibility**: Old scripts remain in `scripts/legacy/` for reference
- **Standardization**: Reuses existing `pipeline/standardization.py` logic (no changes needed)
- **Master Tables**: Each source gets its own master table (e.g., `supermarket_master`, `farm_prices_master`)
- **Local Tables**: Raw data stored in local Hub with `parent_product_id` column added

---

**Ready to proceed?** Once approved, I'll implement:
1. Base source handler class
2. Unified pipeline with flags
3. All 6 source handlers
4. Enhanced config
5. Move old scripts to legacy folder
