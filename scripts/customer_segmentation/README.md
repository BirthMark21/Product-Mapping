# Customer Segmentation System

This folder contains a comprehensive customer segmentation system that analyzes customer behavior, purchasing patterns, and product preferences to create meaningful customer segments.

## Features:
- **Customer Behavior Analysis** - Analyze purchasing patterns, frequency, and preferences
- **Product Preference Analysis** - Understand which products customers prefer
- **Segmentation Algorithms** - RFM Analysis, K-Means Clustering, Behavioral Segmentation
- **Superset API Integration** - Fetch data from ClickHouse using existing Superset API
- **Export Capabilities** - Export segments to CSV, JSON, and database tables

## Files:
- `customer_data_fetcher.py` - Fetch customer data from ClickHouse via Superset API
- `segmentation_analyzer.py` - Core segmentation algorithms and analysis
- `segment_exporter.py` - Export segments to various formats
- `segment_visualizer.py` - Create visualizations and reports
- `api_client.py` - API client for external integrations
- `config.json` - Configuration for segmentation parameters

## Tables Used:
- `product_variations` - Product variations and stock information
- `product_variation_prices` - Pricing information
- `categories` - Product categories
- `carts` - Shopping cart data
- `orders` - Order history
- `groups_carts` - Group purchasing data
- `groups` - Group information
- `group_cart_variations` - Group cart items
- `group_deals` - Group deal information
- `personal_cart_items` - Personal cart items
- `products` - Product information
- `product_names` - Product names and details

## Usage:
1. Run `python customer_data_fetcher.py` to fetch data
2. Run `python segmentation_analyzer.py` to perform segmentation
3. Run `python segment_exporter.py` to export results
4. Run `python segment_visualizer.py` to create visualizations
