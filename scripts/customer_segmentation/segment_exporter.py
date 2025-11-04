#!/usr/bin/env python3
"""
Product Segmentation Exporter
Exports segmentation results to various formats (CSV, JSON, Database)
"""

import pandas as pd
import json
import sys
import os
from datetime import datetime

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import config.setting as settings
from utils.db_connector import get_db_engine

class SegmentExporter:
    def __init__(self, analyzer):
        self.analyzer = analyzer
        self.export_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def export_user_segments_to_csv(self, output_dir="exports"):
        """Export user segments to CSV files"""
        print("Exporting user segments to CSV...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        exported_files = []
        
        # Export RFM segments
        if 'rfm' in self.analyzer.user_segments:
            rfm_df = self.analyzer.user_segments['rfm']
            rfm_file = f"{output_dir}/user_rfm_segments_{self.export_timestamp}.csv"
            rfm_df.to_csv(rfm_file, index=False)
            exported_files.append(rfm_file)
            print(f"SUCCESS: Exported RFM segments to {rfm_file}")
        
        # Export behavioral segments
        if 'behavioral' in self.analyzer.user_segments:
            behavioral_df = self.analyzer.user_segments['behavioral']
            behavioral_file = f"{output_dir}/user_behavioral_segments_{self.export_timestamp}.csv"
            behavioral_df.to_csv(behavioral_file, index=False)
            exported_files.append(behavioral_file)
            print(f"SUCCESS: Exported behavioral segments to {behavioral_file}")
        
        return exported_files
    
    def export_product_segments_to_csv(self, output_dir="exports"):
        """Export product segments to CSV files"""
        print("Exporting product segments to CSV...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        exported_files = []
        
        # Export category segments
        if 'category' in self.analyzer.product_segments:
            category_df = self.analyzer.product_segments['category']
            category_file = f"{output_dir}/product_category_segments_{self.export_timestamp}.csv"
            category_df.to_csv(category_file, index=False)
            exported_files.append(category_file)
            print(f"SUCCESS: Exported category segments to {category_file}")
        
        # Export clustering segments
        if 'clustering' in self.analyzer.product_segments:
            clustering_df = self.analyzer.product_segments['clustering']
            clustering_file = f"{output_dir}/product_clustering_segments_{self.export_timestamp}.csv"
            clustering_df.to_csv(clustering_file, index=False)
            exported_files.append(clustering_file)
            print(f"SUCCESS: Exported clustering segments to {clustering_file}")
        
        return exported_files
    
    def export_segments_to_json(self, output_dir="exports"):
        """Export segments to JSON format"""
        print("Exporting segments to JSON...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        segments_data = {
            "export_timestamp": self.export_timestamp,
            "user_segments": {},
            "product_segments": {},
            "summary": self.analyzer.generate_segmentation_summary()
        }
        
        # Export user segments
        if 'rfm' in self.analyzer.user_segments:
            rfm_df = self.analyzer.user_segments['rfm']
            segments_data["user_segments"]["rfm"] = {
                "segment_counts": rfm_df['rfm_segment'].value_counts().to_dict(),
                "segment_details": rfm_df.groupby('rfm_segment').agg({
                    'recency': 'mean',
                    'frequency': 'mean',
                    'monetary': 'mean',
                    'unique_products': 'mean'
                }).round(2).to_dict()
            }
        
        if 'behavioral' in self.analyzer.user_segments:
            behavioral_df = self.analyzer.user_segments['behavioral']
            segments_data["user_segments"]["behavioral"] = {
                "segment_counts": behavioral_df['behavioral_segment'].value_counts().to_dict(),
                "segment_details": behavioral_df.groupby('behavioral_segment').agg({
                    'recency': 'mean',
                    'frequency': 'mean',
                    'monetary': 'mean',
                    'unique_products': 'mean'
                }).round(2).to_dict()
            }
        
        # Export product segments
        if 'category' in self.analyzer.product_segments:
            category_df = self.analyzer.product_segments['category']
            segments_data["product_segments"]["category"] = {
                "total_products": len(category_df),
                "categories": category_df['category_name'].value_counts().to_dict() if 'category_name' in category_df.columns else {},
                "price_segments": category_df['price_segment'].value_counts().to_dict() if 'price_segment' in category_df.columns else {},
                "stock_segments": category_df['stock_segment'].value_counts().to_dict() if 'stock_segment' in category_df.columns else {}
            }
        
        if 'clustering' in self.analyzer.product_segments:
            clustering_df = self.analyzer.product_segments['clustering']
            segments_data["product_segments"]["clustering"] = {
                "cluster_counts": clustering_df['cluster_segment'].value_counts().to_dict(),
                "cluster_details": clustering_df.groupby('cluster_segment').agg({
                    'variation_count': 'mean',
                    'total_stock': 'mean',
                    'avg_price': 'mean'
                }).round(2).to_dict()
            }
        
        # Save to JSON file
        json_file = f"{output_dir}/segmentation_results_{self.export_timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(segments_data, f, indent=2, default=str)
        
        print(f"SUCCESS: Exported segments to {json_file}")
        return json_file
    
    def export_to_database(self):
        """Export segments to database tables"""
        print("Exporting segments to database...")
        
        try:
            # Get database engine
            engine = get_db_engine('hub')
            
            if engine is None:
                print("ERROR: Could not connect to database")
                return False
            
            # Export user RFM segments
            if 'rfm' in self.analyzer.user_segments:
                rfm_df = self.analyzer.user_segments['rfm']
                table_name = f"user_rfm_segments_{self.export_timestamp}"
                rfm_df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=100)
                print(f"SUCCESS: Exported RFM segments to database table: {table_name}")
            
            # Export user behavioral segments
            if 'behavioral' in self.analyzer.user_segments:
                behavioral_df = self.analyzer.user_segments['behavioral']
                table_name = f"user_behavioral_segments_{self.export_timestamp}"
                behavioral_df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=100)
                print(f"SUCCESS: Exported behavioral segments to database table: {table_name}")
            
            # Export product category segments
            if 'category' in self.analyzer.product_segments:
                category_df = self.analyzer.product_segments['category']
                table_name = f"product_category_segments_{self.export_timestamp}"
                category_df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=100)
                print(f"SUCCESS: Exported category segments to database table: {table_name}")
            
            # Export product clustering segments
            if 'clustering' in self.analyzer.product_segments:
                clustering_df = self.analyzer.product_segments['clustering']
                table_name = f"product_clustering_segments_{self.export_timestamp}"
                clustering_df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=100)
                print(f"SUCCESS: Exported clustering segments to database table: {table_name}")
            
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to export to database: {e}")
            return False
    
    def create_segmentation_report(self, output_dir="exports"):
        """Create a comprehensive segmentation report"""
        print("Creating segmentation report...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        report_content = []
        report_content.append("# Product Segmentation Report")
        report_content.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_content.append("")
        
        # User segmentation summary
        report_content.append("## User Segmentation Summary")
        report_content.append("")
        
        if 'rfm' in self.analyzer.user_segments:
            rfm_df = self.analyzer.user_segments['rfm']
            rfm_summary = rfm_df['rfm_segment'].value_counts()
            report_content.append("### RFM Segments:")
            for segment, count in rfm_summary.items():
                report_content.append(f"- {segment}: {count} users")
            report_content.append("")
        
        if 'behavioral' in self.analyzer.user_segments:
            behavioral_df = self.analyzer.user_segments['behavioral']
            behavioral_summary = behavioral_df['behavioral_segment'].value_counts()
            report_content.append("### Behavioral Segments:")
            for segment, count in behavioral_summary.items():
                report_content.append(f"- {segment}: {count} users")
            report_content.append("")
        
        # Product segmentation summary
        report_content.append("## Product Segmentation Summary")
        report_content.append("")
        
        if 'category' in self.analyzer.product_segments:
            category_df = self.analyzer.product_segments['category']
            report_content.append(f"### Total Products: {len(category_df)}")
            
            if 'price_segment' in category_df.columns:
                price_summary = category_df['price_segment'].value_counts()
                report_content.append("### Price Segments:")
                for segment, count in price_summary.items():
                    report_content.append(f"- {segment}: {count} products")
                report_content.append("")
            
            if 'stock_segment' in category_df.columns:
                stock_summary = category_df['stock_segment'].value_counts()
                report_content.append("### Stock Segments:")
                for segment, count in stock_summary.items():
                    report_content.append(f"- {segment}: {count} products")
                report_content.append("")
        
        if 'clustering' in self.analyzer.product_segments:
            clustering_df = self.analyzer.product_segments['clustering']
            cluster_summary = clustering_df['cluster_segment'].value_counts()
            report_content.append("### Product Clusters:")
            for segment, count in cluster_summary.items():
                report_content.append(f"- {segment}: {count} products")
            report_content.append("")
        
        # Recommendations
        report_content.append("## Recommendations")
        report_content.append("")
        report_content.append("### For User Segments:")
        report_content.append("- **Champions**: Maintain loyalty programs and premium services")
        report_content.append("- **At Risk**: Implement retention campaigns and special offers")
        report_content.append("- **New Customers**: Focus on onboarding and first-time buyer incentives")
        report_content.append("- **Lost**: Consider win-back campaigns with attractive offers")
        report_content.append("")
        
        report_content.append("### For Product Segments:")
        report_content.append("- **High-Value Products**: Focus on premium marketing and quality assurance")
        report_content.append("- **High-Stock Products**: Implement inventory management and promotion strategies")
        report_content.append("- **Multi-Variation Products**: Optimize product presentation and variation selection")
        report_content.append("- **Standard Products**: Focus on competitive pricing and availability")
        report_content.append("")
        
        # Save report
        report_file = f"{output_dir}/segmentation_report_{self.export_timestamp}.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_content))
        
        print(f"SUCCESS: Created segmentation report: {report_file}")
        return report_file
    
    def export_all(self, output_dir="exports"):
        """Export all segments in all formats"""
        print("="*60)
        print("EXPORTING ALL SEGMENTATION RESULTS")
        print("="*60)
        
        exported_files = []
        
        # Export to CSV
        csv_files = self.export_user_segments_to_csv(output_dir)
        exported_files.extend(csv_files)
        
        csv_files = self.export_product_segments_to_csv(output_dir)
        exported_files.extend(csv_files)
        
        # Export to JSON
        json_file = self.export_segments_to_json(output_dir)
        exported_files.append(json_file)
        
        # Export to database
        db_success = self.export_to_database()
        
        # Create report
        report_file = self.create_segmentation_report(output_dir)
        exported_files.append(report_file)
        
        print("="*60)
        print("EXPORT COMPLETED")
        print("="*60)
        print(f"Exported {len(exported_files)} files:")
        for file in exported_files:
            print(f"- {file}")
        
        if db_success:
            print("- Database tables created successfully")
        else:
            print("- Database export failed")
        
        return exported_files

def main():
    """Main function to test export functionality"""
    from product_data_fetcher import ProductDataFetcher
    from segmentation_analyzer import ProductSegmentationAnalyzer
    
    # Fetch data and perform segmentation
    fetcher = ProductDataFetcher()
    data = fetcher.fetch_all_data()
    
    analyzer = ProductSegmentationAnalyzer(data)
    
    # Perform segmentation
    user_df = analyzer.prepare_user_data()
    if not user_df.empty:
        analyzer.perform_user_rfm_segmentation(user_df)
        analyzer.perform_user_behavioral_segmentation(user_df)
    
    product_df = analyzer.prepare_product_data()
    if not product_df.empty:
        analyzer.perform_product_category_segmentation(product_df)
        analyzer.perform_product_clustering(product_df)
    
    # Export results
    exporter = SegmentExporter(analyzer)
    exporter.export_all()

if __name__ == "__main__":
    main()
