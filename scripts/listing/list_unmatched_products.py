#!/usr/bin/env python3
"""List unmatched products from FMCG that did not match with ClickHouse"""

import pandas as pd

# Load comparison data
comparison_df = pd.read_csv('fmcg_exports/fmcg_clickhouse_comparison.csv', encoding='utf-8-sig')
products_df = pd.read_csv('fmcg_exports/fmcg_products_organized.csv', encoding='utf-8-sig')

# Get unmatched products
unmatched_df = comparison_df[comparison_df['status'] == 'NO MATCH']
unmatched_products = unmatched_df['fmcg_product'].tolist()

# Get unmatched records from organized CSV
unmatched_records = products_df[
    products_df['product_name'].isin(unmatched_products)
]

print('='*70)
print('UNMATCHED PRODUCTS LIST (NO MATCH with ClickHouse)')
print('='*70)
print(f'\nTotal unmatched products: {len(unmatched_products)}')
print(f'Total unmatched price records: {len(unmatched_records)}\n')

print('Unmatched Product Names:')
print('-'*70)

# Sort and display
sorted_products = sorted(unmatched_products)
for i, product in enumerate(sorted_products, 1):
    try:
        print(f'{i:3d}. {product}')
    except UnicodeEncodeError:
        # Handle special characters
        safe_name = product.encode('ascii', 'replace').decode('ascii')
        print(f'{i:3d}. {safe_name}')

print('\n' + '='*70)
print('Summary:')
print(f'  - Unique unmatched products: {len(unmatched_products)}')
print(f'  - Total price records for unmatched: {len(unmatched_records)}')
print('='*70)

