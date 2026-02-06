#!/usr/bin/env python3
"""List unique matched products from the MATCHED_ONLY CSV"""

import pandas as pd

df = pd.read_csv('fmcg_exports/fmcg_products_MATCHED_ONLY.csv', encoding='utf-8-sig')
products = sorted(df['product_name'].unique())

print('MATCHED PRODUCTS LIST (ClickHouse Matched)')
print('='*70)
print(f'Total unique matched products: {len(products)}\n')

for i, p in enumerate(products, 1):
    print(f'{i:3d}. {p}')

print(f'\nTotal: {len(products)} unique products')
print(f'Total price records: {len(df)}')

