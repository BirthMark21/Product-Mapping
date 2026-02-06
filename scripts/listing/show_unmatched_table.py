#!/usr/bin/env python3
"""Show unmatched products in table format"""

import pandas as pd

df = pd.read_csv('fmcg_exports/fmcg_clickhouse_comparison.csv', encoding='utf-8-sig')
unmatched = df[df['status'] == 'NO MATCH'].sort_values('fmcg_product')

print('='*70)
print('PRODUCTS IN FMCG BUT NOT IN CLICKHOUSE')
print('='*70)
print(f'\nTotal: {len(unmatched)} products\n')

# Print table header
print('| #  | Product Name')
print('|----|' + '-'*64)

# Print each product
for i, (idx, row) in enumerate(unmatched.iterrows(), 1):
    product_name = str(row['fmcg_product']).strip()
    # Truncate if too long for table
    if len(product_name) > 60:
        product_name = product_name[:57] + '...'
    print(f'| {i:3d} | {product_name}')

print('\n' + '='*70)
print(f'Total: {len(unmatched)} products not found in ClickHouse')
print('='*70)

