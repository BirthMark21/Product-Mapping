# Product Segmentation Jupyter Notebook

This notebook provides an interactive way to perform product segmentation analysis using Superset API to fetch data from ClickHouse.

## Features:
- **Interactive Data Fetching** - Fetch data from ClickHouse via Superset API
- **User-Level Segmentation** - RFM analysis and behavioral segmentation
- **Product-Level Segmentation** - Stock, weight, and clustering analysis
- **Interactive Visualizations** - Charts and dashboards using Plotly
- **Cell-by-Cell Execution** - Run each analysis step independently

## How to Use:

### 1. Install Requirements:
```bash
pip install -r notebook_requirements.txt
```

### 2. Open Jupyter Notebook:
```bash
jupyter notebook product_segmentation.ipynb
```

### 3. Run Cells:
- **Cell 1**: Import libraries
- **Cell 2**: Configure Superset API
- **Cell 3**: Define API function
- **Cell 4**: Fetch product variations data
- **Cell 5**: Fetch product names
- **Cell 6**: Create basic product visualizations
- **Cell 7**: Fetch orders data
- **Cell 8**: Perform user-level segmentation
- **Cell 9**: Create RFM segmentation and visualization
- **Cell 10**: Create product-level segmentation
- **Cell 11**: Create product segmentation visualizations
- **Cell 12**: Create interactive dashboard

## What You'll See:
- **Product Variations Analysis** - Stock, weight, and status distributions
- **RFM Segmentation** - User behavior analysis with Champions, Loyal Customers, etc.
- **Product Segmentation** - Stock levels, weight categories, and product clusters
- **Interactive Dashboard** - All segments in one interactive view

## Benefits:
- **Step-by-Step Analysis** - Understand each step of the segmentation process
- **Interactive Charts** - Hover, zoom, and explore data points
- **Real-time Data** - Fetch fresh data from ClickHouse
- **Customizable** - Modify queries and parameters as needed

## Data Sources:
- `product_variations` - Product variations and stock
- `product_names` - Product names and details
- `orders` - Order history for user analysis

## Outputs:
- Static matplotlib charts
- Interactive Plotly dashboards
- Segmentation results and statistics
- Data insights and recommendations
