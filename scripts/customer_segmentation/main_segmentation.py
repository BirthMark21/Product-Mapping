#!/usr/bin/env python3
"""
Main Product Segmentation Pipeline
Orchestrates the complete product segmentation process
"""

import sys
import os
from datetime import datetime

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from product_data_fetcher import ProductDataFetcher
from segmentation_analyzer import ProductSegmentationAnalyzer
from segment_exporter import SegmentExporter
from segment_visualizer import SegmentVisualizer

def main():
    """Main segmentation pipeline"""
    print("="*80)
    print("PRODUCT SEGMENTATION PIPELINE")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    try:
        # Step 1: Fetch data
        print("STEP 1: FETCHING DATA FROM CLICKHOUSE VIA SUPERSET API")
        print("-" * 60)
        fetcher = ProductDataFetcher()
        data = fetcher.fetch_all_data()
        
        if not data or all(df.empty for df in data.values()):
            print("ERROR: No data fetched from ClickHouse")
            return False
        
        print(f"SUCCESS: Fetched data from {len([k for k, v in data.items() if not v.empty])} tables")
        print("")
        
        # Step 2: Perform segmentation analysis
        print("STEP 2: PERFORMING SEGMENTATION ANALYSIS")
        print("-" * 60)
        analyzer = ProductSegmentationAnalyzer(data)
        
        # User-level segmentation
        print("Performing user-level segmentation...")
        user_df = analyzer.prepare_user_data()
        if not user_df.empty:
            print(f"Analyzing {len(user_df)} users...")
            analyzer.perform_user_rfm_segmentation(user_df)
            analyzer.perform_user_behavioral_segmentation(user_df)
            print("SUCCESS: User-level segmentation completed")
        else:
            print("WARNING: No user data available for segmentation")
        
        # Product-level segmentation
        print("Performing product-level segmentation...")
        product_df = analyzer.prepare_product_data()
        if not product_df.empty:
            print(f"Analyzing {len(product_df)} products...")
            analyzer.perform_product_category_segmentation(product_df)
            analyzer.perform_product_clustering(product_df)
            print("SUCCESS: Product-level segmentation completed")
        else:
            print("WARNING: No product data available for segmentation")
        
        print("")
        
        # Step 3: Export results
        print("STEP 3: EXPORTING SEGMENTATION RESULTS")
        print("-" * 60)
        exporter = SegmentExporter(analyzer)
        exported_files = exporter.export_all("exports")
        print(f"SUCCESS: Exported {len(exported_files)} files")
        print("")
        
        # Step 4: Create visualizations
        print("STEP 4: CREATING VISUALIZATIONS")
        print("-" * 60)
        visualizer = SegmentVisualizer(analyzer)
        visualization_files = visualizer.create_all_visualizations("exports")
        print(f"SUCCESS: Created {len(visualization_files)} visualization files")
        print("")
        
        # Step 5: Generate summary
        print("STEP 5: GENERATING SUMMARY")
        print("-" * 60)
        summary = analyzer.generate_segmentation_summary()
        
        print("="*80)
        print("SEGMENTATION PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total files created: {len(exported_files) + len(visualization_files)}")
        print("")
        print("FILES CREATED:")
        for file in exported_files + visualization_files:
            print(f"- {file}")
        print("")
        print("SUMMARY:")
        for key, value in summary.items():
            print(f"- {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Segmentation pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Product segmentation completed successfully!")
    else:
        print("\n❌ Product segmentation failed!")
        sys.exit(1)
