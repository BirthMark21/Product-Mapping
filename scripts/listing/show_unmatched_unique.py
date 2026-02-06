#!/usr/bin/env python3
"""Show unique unmatched products (no duplicates) in table format"""

import pandas as pd

df = pd.read_csv('fmcg_exports/fmcg_clickhouse_comparison.csv', encoding='utf-8-sig')
unmatched = df[df['status'] == 'NO MATCH']

# Get unique product names only (remove duplicates)
unique_products = sorted(unmatched['fmcg_product'].unique())

print('='*70)
print('PRODUCTS IN FMCG BUT NOT IN CLICKHOUSE (UNIQUE PRODUCTS ONLY)')
print('='*70)
print(f'\nTotal unique unmatched products: {len(unique_products)}\n')

# Print table header
print('| #  | Product Name')
print('|----|' + '-'*64)

# Print each unique product
for i, product_name in enumerate(unique_products, 1):
    # Truncate if too long for table
    display_name = product_name
    if len(display_name) > 60:
        display_name = display_name[:57] + '...'
    print(f'| {i:3d} | {display_name}')

print('\n' + '='*70)
print(f'Total: {len(unique_products)} unique products not found in ClickHouse')
print('='*70)

