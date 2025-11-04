#!/usr/bin/env python3
"""
Product Segmentation Data Fetcher
Fetches comprehensive product and user data from ClickHouse via Superset API
"""

import pandas as pd
import requests
import json
import uuid
import sys
import os
from datetime import datetime, timedelta

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import config.setting as settings

class ProductDataFetcher:
    def __init__(self):
        self.superset_url = "http://64.227.129.135:8088"
        self.access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"
        self.database_id = 1
        self.schema = "chipchip"
    
    def _make_superset_request(self, sql_query, client_id_prefix="prod"):
        """Make request to Superset API"""
        try:
            unique_client_id = f"{client_id_prefix}_{uuid.uuid4().hex[:6]}"
            
            payload = {
                "client_id": unique_client_id,
                "database_id": self.database_id,
                "json": True,
                "runAsync": False,
                "schema": self.schema,
                "sql": sql_query,
                "tab": "",
                "expand_data": True
            }
            
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.access_token}',
                'User-Agent': 'chipchip/product-segmentation'
            }
            
            response = requests.post(
                f"{self.superset_url}/api/v1/sqllab/execute/",
                headers=headers,
                json=payload,
                verify=False
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'data' in result and result['data']:
                    return result['data']
                else:
                    print(f"WARNING: Query returned no data")
                    return []
            else:
                print(f"ERROR: Superset API request failed with status {response.status_code}: {response.text}")
                return []
                
        except Exception as e:
            print(f"ERROR: Failed to execute Superset query. Error: {e}")
            return []
    
    def fetch_product_variations(self):
        """Fetch product variations data"""
        print("Fetching product variations data...")
        
        sql_query = '''
        SELECT 
            id,
            product_id,
            sku,
            image_url,
            stock,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            status,
            weight,
            stock_alert,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."product_variations"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "pv")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} product variations")
            return df
        else:
            print("ERROR: No product variations data fetched")
            return pd.DataFrame()
    
    def fetch_product_variation_prices(self):
        """Fetch product variation prices"""
        print("Fetching product variation prices...")
        
        sql_query = '''
        SELECT 
            id,
            product_variation_id,
            price,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."product_variation_prices"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "pvp")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} product variation prices")
            return df
        else:
            print("ERROR: No product variation prices data fetched")
            return pd.DataFrame()
    
    def fetch_categories(self):
        """Fetch categories data"""
        print("Fetching categories data...")
        
        sql_query = '''
        SELECT 
            id,
            name,
            short_description,
            long_description,
            status,
            primary_image,
            detail_images,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            created_by,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."categories"
        WHERE _peerdb_is_deleted = 0
        ORDER BY name
        '''
        
        data = self._make_superset_request(sql_query, "cat")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} categories")
            return df
        else:
            print("ERROR: No categories data fetched")
            return pd.DataFrame()
    
    def fetch_orders(self):
        """Fetch orders data for user-level analysis"""
        print("Fetching orders data...")
        
        sql_query = '''
        SELECT 
            id,
            personal_cart_id,
            groups_carts_id,
            status,
            total_amount,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            location_id,
            response,
            payment_method,
            discount,
            discount_type,
            discount_rule_id,
            meta,
            bill_id,
            secondary_order_location_id,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."orders"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "ord")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} orders")
            return df
        else:
            print("ERROR: No orders data fetched")
            return pd.DataFrame()
    
    def fetch_personal_cart_items(self):
        """Fetch personal cart items for user behavior analysis"""
        print("Fetching personal cart items...")
        
        sql_query = '''
        SELECT 
            id,
            cart_id,
            product_id,
            quantity,
            status,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            product_variation_id,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."personal_cart_items"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "pci")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} personal cart items")
            return df
        else:
            print("ERROR: No personal cart items data fetched")
            return pd.DataFrame()
    
    def fetch_products(self):
        """Fetch products data"""
        print("Fetching products data...")
        
        sql_query = '''
        SELECT 
            id,
            vendor_id,
            short_description,
            long_description,
            primary_image,
            detail_images,
            status,
            tags,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            name_id,
            sorting_level,
            approved,
            stock_alert,
            weight,
            require_variation,
            explainer_video,
            commission,
            stock_tracking_time,
            auto_accept,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."products"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "prod")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} products")
            return df
        else:
            print("ERROR: No products data fetched")
            return pd.DataFrame()
    
    def fetch_product_names(self):
        """Fetch product names data"""
        print("Fetching product names data...")
        
        sql_query = '''
        SELECT 
            id,
            name,
            category_id,
            created_by,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            measuring_unit,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."product_names"
        WHERE _peerdb_is_deleted = 0
        ORDER BY name
        '''
        
        data = self._make_superset_request(sql_query, "pn")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} product names")
            return df
        else:
            print("ERROR: No product names data fetched")
            return pd.DataFrame()
    
    def fetch_groups_carts(self):
        """Fetch groups carts data for group behavior analysis"""
        print("Fetching groups carts data...")
        
        sql_query = '''
        SELECT 
            id,
            group_id,
            user_id,
            quantity,
            status,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            created_by,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."groups_carts"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "gc")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} groups carts")
            return df
        else:
            print("ERROR: No groups carts data fetched")
            return pd.DataFrame()
    
    def fetch_group_cart_variations(self):
        """Fetch group cart variations data"""
        print("Fetching group cart variations data...")
        
        sql_query = '''
        SELECT 
            id,
            group_cart_id,
            product_variation_id,
            quantity,
            toString(created_at) AS created_at,
            toString(updated_at) AS updated_at,
            deleted_at,
            toString(_peerdb_synced_at) AS _peerdb_synced_at,
            _peerdb_is_deleted,
            _peerdb_version
        FROM "chipchip"."group_cart_variations"
        WHERE _peerdb_is_deleted = 0
        ORDER BY created_at DESC
        '''
        
        data = self._make_superset_request(sql_query, "gcv")
        if data:
            df = pd.DataFrame(data)
            print(f"SUCCESS: Fetched {len(df)} group cart variations")
            return df
        else:
            print("ERROR: No group cart variations data fetched")
            return pd.DataFrame()
    
    def fetch_all_data(self):
        """Fetch all data for comprehensive analysis"""
        print("="*60)
        print("FETCHING ALL PRODUCT SEGMENTATION DATA")
        print("="*60)
        
        data = {}
        
        # Core product data
        data['product_variations'] = self.fetch_product_variations()
        data['product_variation_prices'] = self.fetch_product_variation_prices()
        data['products'] = self.fetch_products()
        data['product_names'] = self.fetch_product_names()
        data['categories'] = self.fetch_categories()
        
        # User behavior data
        data['orders'] = self.fetch_orders()
        data['personal_cart_items'] = self.fetch_personal_cart_items()
        data['groups_carts'] = self.fetch_groups_carts()
        data['group_cart_variations'] = self.fetch_group_cart_variations()
        
        print("="*60)
        print("DATA FETCHING COMPLETED")
        print("="*60)
        
        return data

def main():
    """Main function to test data fetching"""
    fetcher = ProductDataFetcher()
    data = fetcher.fetch_all_data()
    
    # Print summary
    print("\nDATA SUMMARY:")
    for table_name, df in data.items():
        if not df.empty:
            print(f"- {table_name}: {len(df)} records")
        else:
            print(f"- {table_name}: No data")

if __name__ == "__main__":
    main()
