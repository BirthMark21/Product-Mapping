#!/usr/bin/env python3
"""
Product Segmentation Analyzer
Performs user-level and product-level segmentation analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import sys
import os

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

class ProductSegmentationAnalyzer:
    def __init__(self, data):
        self.data = data
        self.user_segments = {}
        self.product_segments = {}
        
    def prepare_user_data(self):
        """Prepare user-level data for segmentation"""
        print("Preparing user-level data for segmentation...")
        
        # Merge orders with cart items to get user behavior
        orders_df = self.data['orders'].copy()
        cart_items_df = self.data['personal_cart_items'].copy()
        
        if orders_df.empty or cart_items_df.empty:
            print("WARNING: No orders or cart items data available")
            return pd.DataFrame()
        
        # Convert date columns
        orders_df['created_at'] = pd.to_datetime(orders_df['created_at'], errors='coerce')
        cart_items_df['created_at'] = pd.to_datetime(cart_items_df['created_at'], errors='coerce')
        
        # Get current date for recency calculation
        current_date = datetime.now()
        
        # Calculate user metrics
        user_metrics = []
        
        # Group by user (assuming user_id is in orders or derived from cart_id)
        if 'user_id' in orders_df.columns:
            user_col = 'user_id'
        else:
            # If no direct user_id, use personal_cart_id as proxy
            user_col = 'personal_cart_id'
        
        for user_id in orders_df[user_col].unique():
            if pd.isna(user_id):
                continue
                
            user_orders = orders_df[orders_df[user_col] == user_id]
            user_cart_items = cart_items_df[cart_items_df['cart_id'].isin(user_orders['personal_cart_id'])]
            
            if user_orders.empty:
                continue
            
            # Calculate RFM metrics
            recency = (current_date - user_orders['created_at'].max()).days
            frequency = len(user_orders)
            monetary = user_orders['total_amount'].sum() if 'total_amount' in user_orders.columns else 0
            
            # Product diversity
            unique_products = user_cart_items['product_id'].nunique() if not user_cart_items.empty else 0
            total_quantity = user_cart_items['quantity'].sum() if not user_cart_items.empty else 0
            
            # Average order value
            avg_order_value = monetary / frequency if frequency > 0 else 0
            
            # Days since first order
            days_since_first = (current_date - user_orders['created_at'].min()).days
            
            user_metrics.append({
                'user_id': user_id,
                'recency': recency,
                'frequency': frequency,
                'monetary': monetary,
                'unique_products': unique_products,
                'total_quantity': total_quantity,
                'avg_order_value': avg_order_value,
                'days_since_first': days_since_first,
                'total_orders': frequency
            })
        
        user_df = pd.DataFrame(user_metrics)
        print(f"SUCCESS: Prepared {len(user_df)} users for segmentation")
        return user_df
    
    def perform_user_rfm_segmentation(self, user_df):
        """Perform RFM (Recency, Frequency, Monetary) segmentation"""
        print("Performing RFM segmentation...")
        
        if user_df.empty:
            print("ERROR: No user data available for RFM segmentation")
            return pd.DataFrame()
        
        # Calculate RFM scores (1-5 scale)
        user_df['recency_score'] = pd.qcut(user_df['recency'], 5, labels=[5,4,3,2,1])
        user_df['frequency_score'] = pd.qcut(user_df['frequency'], 5, labels=[1,2,3,4,5])
        user_df['monetary_score'] = pd.qcut(user_df['monetary'], 5, labels=[1,2,3,4,5])
        
        # Convert to numeric
        user_df['recency_score'] = user_df['recency_score'].astype(int)
        user_df['frequency_score'] = user_df['frequency_score'].astype(int)
        user_df['monetary_score'] = user_df['monetary_score'].astype(int)
        
        # Create RFM segments
        def assign_rfm_segment(row):
            r, f, m = row['recency_score'], row['frequency_score'], row['monetary_score']
            
            if r >= 4 and f >= 4 and m >= 4:
                return "Champions"
            elif r >= 3 and f >= 4 and m >= 4:
                return "Loyal Customers"
            elif r >= 4 and f >= 2 and m >= 3:
                return "Potential Loyalists"
            elif r >= 4 and f >= 1 and m >= 1:
                return "New Customers"
            elif r >= 3 and f >= 2 and m >= 2:
                return "Promising"
            elif r >= 2 and f >= 2 and m >= 2:
                return "Need Attention"
            elif r >= 2 and f >= 1 and m >= 1:
                return "About to Sleep"
            elif r >= 1 and f >= 1 and m >= 1:
                return "At Risk"
            else:
                return "Lost"
        
        user_df['rfm_segment'] = user_df.apply(assign_rfm_segment, axis=1)
        
        # Calculate segment statistics
        segment_stats = user_df.groupby('rfm_segment').agg({
            'user_id': 'count',
            'recency': 'mean',
            'frequency': 'mean',
            'monetary': 'mean',
            'unique_products': 'mean',
            'avg_order_value': 'mean'
        }).round(2)
        
        print("RFM SEGMENTATION RESULTS:")
        print(segment_stats)
        
        self.user_segments['rfm'] = user_df
        return user_df
    
    def perform_user_behavioral_segmentation(self, user_df):
        """Perform behavioral segmentation based on product preferences"""
        print("Performing behavioral segmentation...")
        
        if user_df.empty:
            print("ERROR: No user data available for behavioral segmentation")
            return pd.DataFrame()
        
        # Prepare features for clustering
        features = ['recency', 'frequency', 'monetary', 'unique_products', 'total_quantity', 'avg_order_value']
        X = user_df[features].fillna(0)
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Perform K-means clustering
        n_clusters = 5
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        user_df['behavioral_cluster'] = kmeans.fit_predict(X_scaled)
        
        # Assign cluster names based on characteristics
        cluster_names = {}
        for cluster in range(n_clusters):
            cluster_data = user_df[user_df['behavioral_cluster'] == cluster]
            avg_monetary = cluster_data['monetary'].mean()
            avg_frequency = cluster_data['frequency'].mean()
            avg_recency = cluster_data['recency'].mean()
            
            if avg_monetary > user_df['monetary'].quantile(0.8) and avg_frequency > user_df['frequency'].quantile(0.8):
                cluster_names[cluster] = "High Value Frequent Buyers"
            elif avg_monetary > user_df['monetary'].quantile(0.6):
                cluster_names[cluster] = "High Value Customers"
            elif avg_frequency > user_df['frequency'].quantile(0.6):
                cluster_names[cluster] = "Frequent Buyers"
            elif avg_recency < user_df['recency'].quantile(0.4):
                cluster_names[cluster] = "Recent Active Users"
            else:
                cluster_names[cluster] = "Regular Customers"
        
        user_df['behavioral_segment'] = user_df['behavioral_cluster'].map(cluster_names)
        
        # Calculate segment statistics
        segment_stats = user_df.groupby('behavioral_segment').agg({
            'user_id': 'count',
            'recency': 'mean',
            'frequency': 'mean',
            'monetary': 'mean',
            'unique_products': 'mean',
            'avg_order_value': 'mean'
        }).round(2)
        
        print("BEHAVIORAL SEGMENTATION RESULTS:")
        print(segment_stats)
        
        self.user_segments['behavioral'] = user_df
        return user_df
    
    def prepare_product_data(self):
        """Prepare product-level data for segmentation"""
        print("Preparing product-level data for segmentation...")
        
        # Merge product data
        products_df = self.data['products'].copy()
        product_names_df = self.data['product_names'].copy()
        product_variations_df = self.data['product_variations'].copy()
        product_prices_df = self.data['product_variation_prices'].copy()
        categories_df = self.data['categories'].copy()
        
        if products_df.empty or product_names_df.empty:
            print("WARNING: No products or product names data available")
            return pd.DataFrame()
        
        # Merge products with names
        product_data = products_df.merge(
            product_names_df, 
            left_on='name_id', 
            right_on='id', 
            how='left',
            suffixes=('_product', '_name')
        )
        
        # Add category information
        if not categories_df.empty:
            product_data = product_data.merge(
                categories_df[['id', 'name']], 
                left_on='category_id', 
                right_on='id', 
                how='left',
                suffixes=('', '_category')
            )
            product_data = product_data.rename(columns={'name': 'category_name'})
        
        # Add variation and pricing data
        if not product_variations_df.empty:
            variation_stats = product_variations_df.groupby('product_id').agg({
                'id': 'count',
                'stock': 'sum',
                'weight': 'mean'
            }).reset_index()
            variation_stats.columns = ['product_id', 'variation_count', 'total_stock', 'avg_weight']
            
            product_data = product_data.merge(variation_stats, on='product_id', how='left')
        else:
            product_data['variation_count'] = 0
            product_data['total_stock'] = 0
            product_data['avg_weight'] = 0
        
        # Add pricing data
        if not product_prices_df.empty:
            price_stats = product_prices_df.groupby('product_variation_id').agg({
                'price': ['min', 'max', 'mean']
            }).reset_index()
            price_stats.columns = ['product_variation_id', 'min_price', 'max_price', 'avg_price']
            
            # Get product_id from variations
            if not product_variations_df.empty:
                price_variations = product_variations_df[['id', 'product_id']].merge(
                    price_stats, 
                    left_on='id', 
                    right_on='product_variation_id', 
                    how='left'
                )
                
                product_price_stats = price_variations.groupby('product_id').agg({
                    'min_price': 'min',
                    'max_price': 'max',
                    'avg_price': 'mean'
                }).reset_index()
                
                product_data = product_data.merge(product_price_stats, on='product_id', how='left')
        
        # Calculate product metrics
        product_data['has_variations'] = product_data['variation_count'] > 0
        product_data['price_range'] = product_data['max_price'] - product_data['min_price']
        product_data['price_range'] = product_data['price_range'].fillna(0)
        
        print(f"SUCCESS: Prepared {len(product_data)} products for segmentation")
        return product_data
    
    def perform_product_category_segmentation(self, product_df):
        """Segment products by category and characteristics"""
        print("Performing product category segmentation...")
        
        if product_df.empty:
            print("ERROR: No product data available for category segmentation")
            return pd.DataFrame()
        
        # Category-based segmentation
        if 'category_name' in product_df.columns:
            category_segments = product_df.groupby('category_name').agg({
                'id': 'count',
                'variation_count': 'mean',
                'total_stock': 'sum',
                'avg_price': 'mean',
                'price_range': 'mean'
            }).round(2)
            
            print("CATEGORY SEGMENTATION RESULTS:")
            print(category_segments)
        
        # Price-based segmentation
        if 'avg_price' in product_df.columns:
            product_df['price_segment'] = pd.cut(
                product_df['avg_price'], 
                bins=[0, 50, 100, 200, 500, float('inf')], 
                labels=['Budget', 'Economy', 'Mid-range', 'Premium', 'Luxury']
            )
        
        # Stock-based segmentation
        if 'total_stock' in product_df.columns:
            product_df['stock_segment'] = pd.cut(
                product_df['total_stock'], 
                bins=[0, 10, 50, 100, 500, float('inf')], 
                labels=['Low Stock', 'Limited', 'Adequate', 'High Stock', 'Overstocked']
            )
        
        # Variation-based segmentation
        if 'variation_count' in product_df.columns:
            product_df['variation_segment'] = pd.cut(
                product_df['variation_count'], 
                bins=[0, 1, 3, 5, 10, float('inf')], 
                labels=['Single', 'Few', 'Moderate', 'Many', 'Extensive']
            )
        
        self.product_segments['category'] = product_df
        return product_df
    
    def perform_product_clustering(self, product_df):
        """Perform K-means clustering on products"""
        print("Performing product clustering...")
        
        if product_df.empty:
            print("ERROR: No product data available for clustering")
            return pd.DataFrame()
        
        # Prepare features for clustering
        features = ['variation_count', 'total_stock', 'avg_price', 'price_range']
        available_features = [f for f in features if f in product_df.columns]
        
        if not available_features:
            print("WARNING: No suitable features for clustering")
            return product_df
        
        X = product_df[available_features].fillna(0)
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Perform K-means clustering
        n_clusters = 4
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        product_df['product_cluster'] = kmeans.fit_predict(X_scaled)
        
        # Assign cluster names based on characteristics
        cluster_names = {}
        for cluster in range(n_clusters):
            cluster_data = product_df[product_df['product_cluster'] == cluster]
            
            avg_price = cluster_data['avg_price'].mean() if 'avg_price' in cluster_data.columns else 0
            avg_stock = cluster_data['total_stock'].mean() if 'total_stock' in cluster_data.columns else 0
            avg_variations = cluster_data['variation_count'].mean() if 'variation_count' in cluster_data.columns else 0
            
            if avg_price > product_df['avg_price'].quantile(0.8) if 'avg_price' in product_df.columns else 0:
                cluster_names[cluster] = "High-Value Products"
            elif avg_stock > product_df['total_stock'].quantile(0.8) if 'total_stock' in product_df.columns else 0:
                cluster_names[cluster] = "High-Stock Products"
            elif avg_variations > product_df['variation_count'].quantile(0.8) if 'variation_count' in product_df.columns else 0:
                cluster_names[cluster] = "Multi-Variation Products"
            else:
                cluster_names[cluster] = "Standard Products"
        
        product_df['cluster_segment'] = product_df['product_cluster'].map(cluster_names)
        
        # Calculate cluster statistics
        cluster_stats = product_df.groupby('cluster_segment').agg({
            'id': 'count',
            'variation_count': 'mean',
            'total_stock': 'mean',
            'avg_price': 'mean'
        }).round(2)
        
        print("PRODUCT CLUSTERING RESULTS:")
        print(cluster_stats)
        
        self.product_segments['clustering'] = product_df
        return product_df
    
    def generate_segmentation_summary(self):
        """Generate comprehensive segmentation summary"""
        print("="*60)
        print("SEGMENTATION SUMMARY")
        print("="*60)
        
        summary = {}
        
        # User segmentation summary
        if 'rfm' in self.user_segments:
            rfm_summary = self.user_segments['rfm']['rfm_segment'].value_counts()
            summary['user_rfm'] = rfm_summary.to_dict()
            print(f"USER RFM SEGMENTS: {len(rfm_summary)} segments")
        
        if 'behavioral' in self.user_segments:
            behavioral_summary = self.user_segments['behavioral']['behavioral_segment'].value_counts()
            summary['user_behavioral'] = behavioral_summary.to_dict()
            print(f"USER BEHAVIORAL SEGMENTS: {len(behavioral_summary)} segments")
        
        # Product segmentation summary
        if 'category' in self.product_segments:
            if 'price_segment' in self.product_segments['category'].columns:
                price_summary = self.product_segments['category']['price_segment'].value_counts()
                summary['product_price'] = price_summary.to_dict()
                print(f"PRODUCT PRICE SEGMENTS: {len(price_summary)} segments")
            
            if 'stock_segment' in self.product_segments['category'].columns:
                stock_summary = self.product_segments['category']['stock_segment'].value_counts()
                summary['product_stock'] = stock_summary.to_dict()
                print(f"PRODUCT STOCK SEGMENTS: {len(stock_summary)} segments")
        
        if 'clustering' in self.product_segments:
            cluster_summary = self.product_segments['clustering']['cluster_segment'].value_counts()
            summary['product_clusters'] = cluster_summary.to_dict()
            print(f"PRODUCT CLUSTERS: {len(cluster_summary)} clusters")
        
        return summary

def main():
    """Main function to test segmentation analysis"""
    from product_data_fetcher import ProductDataFetcher
    
    # Fetch data
    fetcher = ProductDataFetcher()
    data = fetcher.fetch_all_data()
    
    # Perform segmentation
    analyzer = ProductSegmentationAnalyzer(data)
    
    # User-level segmentation
    user_df = analyzer.prepare_user_data()
    if not user_df.empty:
        analyzer.perform_user_rfm_segmentation(user_df)
        analyzer.perform_user_behavioral_segmentation(user_df)
    
    # Product-level segmentation
    product_df = analyzer.prepare_product_data()
    if not product_df.empty:
        analyzer.perform_product_category_segmentation(product_df)
        analyzer.perform_product_clustering(product_df)
    
    # Generate summary
    analyzer.generate_segmentation_summary()

if __name__ == "__main__":
    main()
