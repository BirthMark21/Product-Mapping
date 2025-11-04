#!/usr/bin/env python3
"""
Product Segmentation Visualizer
Creates visualizations and charts for segmentation analysis
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from datetime import datetime

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

class SegmentVisualizer:
    def __init__(self, analyzer):
        self.analyzer = analyzer
        self.export_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def create_user_rfm_visualization(self, output_dir="exports"):
        """Create RFM segmentation visualizations"""
        print("Creating RFM segmentation visualizations...")
        
        if 'rfm' not in self.analyzer.user_segments:
            print("WARNING: No RFM segments available for visualization")
            return []
        
        rfm_df = self.analyzer.user_segments['rfm']
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('RFM Segmentation Analysis', fontsize=16, fontweight='bold')
        
        # 1. RFM Segment Distribution
        segment_counts = rfm_df['rfm_segment'].value_counts()
        axes[0, 0].pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%', startangle=90)
        axes[0, 0].set_title('RFM Segment Distribution')
        
        # 2. Recency vs Frequency
        scatter = axes[0, 1].scatter(rfm_df['recency'], rfm_df['frequency'], 
                                   c=rfm_df['recency_score'], cmap='viridis', alpha=0.6)
        axes[0, 1].set_xlabel('Recency (Days)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].set_title('Recency vs Frequency')
        plt.colorbar(scatter, ax=axes[0, 1], label='Recency Score')
        
        # 3. Frequency vs Monetary
        scatter2 = axes[1, 0].scatter(rfm_df['frequency'], rfm_df['monetary'], 
                                    c=rfm_df['monetary_score'], cmap='plasma', alpha=0.6)
        axes[1, 0].set_xlabel('Frequency')
        axes[1, 0].set_ylabel('Monetary Value')
        axes[1, 0].set_title('Frequency vs Monetary Value')
        plt.colorbar(scatter2, ax=axes[1, 0], label='Monetary Score')
        
        # 4. Segment Statistics
        segment_stats = rfm_df.groupby('rfm_segment').agg({
            'recency': 'mean',
            'frequency': 'mean',
            'monetary': 'mean'
        }).round(2)
        
        segment_stats.plot(kind='bar', ax=axes[1, 1])
        axes[1, 1].set_title('Average Metrics by Segment')
        axes[1, 1].set_xlabel('RFM Segment')
        axes[1, 1].set_ylabel('Average Value')
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        plot_file = f"{output_dir}/rfm_segmentation_{self.export_timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"SUCCESS: Created RFM visualization: {plot_file}")
        return [plot_file]
    
    def create_behavioral_segmentation_visualization(self, output_dir="exports"):
        """Create behavioral segmentation visualizations"""
        print("Creating behavioral segmentation visualizations...")
        
        if 'behavioral' not in self.analyzer.user_segments:
            print("WARNING: No behavioral segments available for visualization")
            return []
        
        behavioral_df = self.analyzer.user_segments['behavioral']
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Behavioral Segmentation Analysis', fontsize=16, fontweight='bold')
        
        # 1. Behavioral Segment Distribution
        segment_counts = behavioral_df['behavioral_segment'].value_counts()
        axes[0, 0].pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%', startangle=90)
        axes[0, 0].set_title('Behavioral Segment Distribution')
        
        # 2. Recency vs Monetary by Segment
        for segment in behavioral_df['behavioral_segment'].unique():
            segment_data = behavioral_df[behavioral_df['behavioral_segment'] == segment]
            axes[0, 1].scatter(segment_data['recency'], segment_data['monetary'], 
                             label=segment, alpha=0.6)
        axes[0, 1].set_xlabel('Recency (Days)')
        axes[0, 1].set_ylabel('Monetary Value')
        axes[0, 1].set_title('Recency vs Monetary by Segment')
        axes[0, 1].legend()
        
        # 3. Frequency vs Unique Products
        for segment in behavioral_df['behavioral_segment'].unique():
            segment_data = behavioral_df[behavioral_df['behavioral_segment'] == segment]
            axes[1, 0].scatter(segment_data['frequency'], segment_data['unique_products'], 
                             label=segment, alpha=0.6)
        axes[1, 0].set_xlabel('Frequency')
        axes[1, 0].set_ylabel('Unique Products')
        axes[1, 0].set_title('Frequency vs Product Diversity')
        axes[1, 0].legend()
        
        # 4. Segment Statistics
        segment_stats = behavioral_df.groupby('behavioral_segment').agg({
            'recency': 'mean',
            'frequency': 'mean',
            'monetary': 'mean',
            'unique_products': 'mean'
        }).round(2)
        
        segment_stats.plot(kind='bar', ax=axes[1, 1])
        axes[1, 1].set_title('Average Metrics by Behavioral Segment')
        axes[1, 1].set_xlabel('Behavioral Segment')
        axes[1, 1].set_ylabel('Average Value')
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        plot_file = f"{output_dir}/behavioral_segmentation_{self.export_timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"SUCCESS: Created behavioral visualization: {plot_file}")
        return [plot_file]
    
    def create_product_segmentation_visualization(self, output_dir="exports"):
        """Create product segmentation visualizations"""
        print("Creating product segmentation visualizations...")
        
        if 'category' not in self.analyzer.product_segments:
            print("WARNING: No product segments available for visualization")
            return []
        
        product_df = self.analyzer.product_segments['category']
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Product Segmentation Analysis', fontsize=16, fontweight='bold')
        
        # 1. Category Distribution
        if 'category_name' in product_df.columns:
            category_counts = product_df['category_name'].value_counts().head(10)
            axes[0, 0].bar(range(len(category_counts)), category_counts.values)
            axes[0, 0].set_xticks(range(len(category_counts)))
            axes[0, 0].set_xticklabels(category_counts.index, rotation=45, ha='right')
            axes[0, 0].set_title('Top 10 Categories')
            axes[0, 0].set_ylabel('Number of Products')
        
        # 2. Price Distribution
        if 'avg_price' in product_df.columns:
            axes[0, 1].hist(product_df['avg_price'].dropna(), bins=20, alpha=0.7, edgecolor='black')
            axes[0, 1].set_xlabel('Average Price')
            axes[0, 1].set_ylabel('Frequency')
            axes[0, 1].set_title('Price Distribution')
        
        # 3. Stock vs Price
        if 'total_stock' in product_df.columns and 'avg_price' in product_df.columns:
            scatter = axes[1, 0].scatter(product_df['total_stock'], product_df['avg_price'], alpha=0.6)
            axes[1, 0].set_xlabel('Total Stock')
            axes[1, 0].set_ylabel('Average Price')
            axes[1, 0].set_title('Stock vs Price')
        
        # 4. Variation Count Distribution
        if 'variation_count' in product_df.columns:
            variation_counts = product_df['variation_count'].value_counts().sort_index()
            axes[1, 1].bar(variation_counts.index, variation_counts.values)
            axes[1, 1].set_xlabel('Number of Variations')
            axes[1, 1].set_ylabel('Number of Products')
            axes[1, 1].set_title('Product Variation Distribution')
        
        plt.tight_layout()
        
        # Save plot
        plot_file = f"{output_dir}/product_segmentation_{self.export_timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"SUCCESS: Created product visualization: {plot_file}")
        return [plot_file]
    
    def create_clustering_visualization(self, output_dir="exports"):
        """Create product clustering visualizations"""
        print("Creating clustering visualizations...")
        
        if 'clustering' not in self.analyzer.product_segments:
            print("WARNING: No clustering segments available for visualization")
            return []
        
        clustering_df = self.analyzer.product_segments['clustering']
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Product Clustering Analysis', fontsize=16, fontweight='bold')
        
        # 1. Cluster Distribution
        cluster_counts = clustering_df['cluster_segment'].value_counts()
        axes[0, 0].pie(cluster_counts.values, labels=cluster_counts.index, autopct='%1.1f%%', startangle=90)
        axes[0, 0].set_title('Cluster Distribution')
        
        # 2. Price vs Stock by Cluster
        if 'avg_price' in clustering_df.columns and 'total_stock' in clustering_df.columns:
            for cluster in clustering_df['cluster_segment'].unique():
                cluster_data = clustering_df[clustering_df['cluster_segment'] == cluster]
                axes[0, 1].scatter(cluster_data['total_stock'], cluster_data['avg_price'], 
                                 label=cluster, alpha=0.6)
            axes[0, 1].set_xlabel('Total Stock')
            axes[0, 1].set_ylabel('Average Price')
            axes[0, 1].set_title('Stock vs Price by Cluster')
            axes[0, 1].legend()
        
        # 3. Variation Count vs Price by Cluster
        if 'variation_count' in clustering_df.columns and 'avg_price' in clustering_df.columns:
            for cluster in clustering_df['cluster_segment'].unique():
                cluster_data = clustering_df[clustering_df['cluster_segment'] == cluster]
                axes[1, 0].scatter(cluster_data['variation_count'], cluster_data['avg_price'], 
                                 label=cluster, alpha=0.6)
            axes[1, 0].set_xlabel('Variation Count')
            axes[1, 0].set_ylabel('Average Price')
            axes[1, 0].set_title('Variations vs Price by Cluster')
            axes[1, 0].legend()
        
        # 4. Cluster Statistics
        cluster_stats = clustering_df.groupby('cluster_segment').agg({
            'variation_count': 'mean',
            'total_stock': 'mean',
            'avg_price': 'mean'
        }).round(2)
        
        cluster_stats.plot(kind='bar', ax=axes[1, 1])
        axes[1, 1].set_title('Average Metrics by Cluster')
        axes[1, 1].set_xlabel('Cluster Segment')
        axes[1, 1].set_ylabel('Average Value')
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        plot_file = f"{output_dir}/product_clustering_{self.export_timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"SUCCESS: Created clustering visualization: {plot_file}")
        return [plot_file]
    
    def create_interactive_dashboard(self, output_dir="exports"):
        """Create interactive Plotly dashboard"""
        print("Creating interactive dashboard...")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('RFM Segments', 'Behavioral Segments', 'Product Categories', 'Product Clusters'),
            specs=[[{"type": "pie"}, {"type": "pie"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # Add RFM segments
        if 'rfm' in self.analyzer.user_segments:
            rfm_df = self.analyzer.user_segments['rfm']
            rfm_counts = rfm_df['rfm_segment'].value_counts()
            fig.add_trace(
                go.Pie(labels=rfm_counts.index, values=rfm_counts.values, name="RFM"),
                row=1, col=1
            )
        
        # Add behavioral segments
        if 'behavioral' in self.analyzer.user_segments:
            behavioral_df = self.analyzer.user_segments['behavioral']
            behavioral_counts = behavioral_df['behavioral_segment'].value_counts()
            fig.add_trace(
                go.Pie(labels=behavioral_counts.index, values=behavioral_counts.values, name="Behavioral"),
                row=1, col=2
            )
        
        # Add product categories
        if 'category' in self.analyzer.product_segments:
            category_df = self.analyzer.product_segments['category']
            if 'category_name' in category_df.columns:
                category_counts = category_df['category_name'].value_counts().head(10)
                fig.add_trace(
                    go.Bar(x=category_counts.index, y=category_counts.values, name="Categories"),
                    row=2, col=1
                )
        
        # Add product clusters
        if 'clustering' in self.analyzer.product_segments:
            clustering_df = self.analyzer.product_segments['clustering']
            cluster_counts = clustering_df['cluster_segment'].value_counts()
            fig.add_trace(
                go.Bar(x=cluster_counts.index, y=cluster_counts.values, name="Clusters"),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            title_text="Product Segmentation Dashboard",
            showlegend=True,
            height=800
        )
        
        # Save interactive dashboard
        dashboard_file = f"{output_dir}/segmentation_dashboard_{self.export_timestamp}.html"
        fig.write_html(dashboard_file)
        
        print(f"SUCCESS: Created interactive dashboard: {dashboard_file}")
        return [dashboard_file]
    
    def create_all_visualizations(self, output_dir="exports"):
        """Create all visualizations"""
        print("="*60)
        print("CREATING ALL SEGMENTATION VISUALIZATIONS")
        print("="*60)
        
        all_files = []
        
        # Create individual visualizations
        rfm_files = self.create_user_rfm_visualization(output_dir)
        all_files.extend(rfm_files)
        
        behavioral_files = self.create_behavioral_segmentation_visualization(output_dir)
        all_files.extend(behavioral_files)
        
        product_files = self.create_product_segmentation_visualization(output_dir)
        all_files.extend(product_files)
        
        clustering_files = self.create_clustering_visualization(output_dir)
        all_files.extend(clustering_files)
        
        # Create interactive dashboard
        dashboard_files = self.create_interactive_dashboard(output_dir)
        all_files.extend(dashboard_files)
        
        print("="*60)
        print("VISUALIZATION COMPLETED")
        print("="*60)
        print(f"Created {len(all_files)} visualization files:")
        for file in all_files:
            print(f"- {file}")
        
        return all_files

def main():
    """Main function to test visualization functionality"""
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
    
    # Create visualizations
    visualizer = SegmentVisualizer(analyzer)
    visualizer.create_all_visualizations()

if __name__ == "__main__":
    main()
