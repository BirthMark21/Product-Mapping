# Dynamic Mapping System

This folder contains a dynamic mapping system that allows updating product standardization mappings without changing code.

## Structure:
- `mapping_config.json` - Main configuration file with all mappings
- `update_mapping.py` - Script to update mappings dynamically
- `apply_dynamic_mapping.py` - Script to apply mappings to tables
- `validate_mapping.py` - Script to validate mapping configuration

## How it works:
1. Owner can update `mapping_config.json` with new products/mappings
2. Run `update_mapping.py` to apply changes to master tables
3. Run `apply_dynamic_mapping.py` to update remote tables
4. All mappings are stored in JSON format for easy editing

## Benefits:
- No code changes needed for new products
- Easy to add/remove/modify mappings
- Version control for mapping changes
- Validation before applying changes
