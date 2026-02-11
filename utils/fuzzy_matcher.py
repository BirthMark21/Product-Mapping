#!/usr/bin/env python3
"""
Fuzzy Product Matcher
Handles fuzzy string matching for product names to reduce manual mapping work
"""

from fuzzywuzzy import fuzz, process
import logging
import re
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class FuzzyProductMatcher:
    """
    Fuzzy matching for product names
    
    Uses fuzzy string matching to find similar parent products even when
    there are typos, spacing variations, or minor differences.
    """
    
    def __init__(self, parent_mapping: dict, child_to_parent_map: dict, threshold: int = 85, dry_run: bool = True):
        """
        Initialize fuzzy matcher
        
        Args:
            parent_mapping: Dictionary of parent -> children mappings
            child_to_parent_map: Reverse mapping of child -> parent
            threshold: Minimum similarity score (0-100) to accept a match
            dry_run: If True, log matches but don't actually use them
        """
        self.parent_names = list(parent_mapping.keys())
        self.child_to_parent_map = child_to_parent_map
        self.threshold = threshold
        self.dry_run = dry_run
        
        # Statistics
        self.stats = {
            'exact_matches': 0,
            'fuzzy_matches': 0,
            'no_matches': 0,
            'total_queries': 0
        }
        
        # Cache for performance
        self.fuzzy_cache = {}
        
        logger.info(f"🔍 Fuzzy Matcher initialized:")
        logger.info(f"   • Parent products: {len(self.parent_names)}")
        logger.info(f"   • Similarity threshold: {self.threshold}%")
        logger.info(f"   • Mode: {'DRY RUN (logging only)' if self.dry_run else 'ACTIVE'}")
    
    def find_parent(self, product_name: str) -> str:
        """
        Find parent product for a given product name
        
        Process:
        1. Try exact match first (fast)
        2. If not found, try fuzzy match (slower but smarter)
        3. If still not found, return original name (self-mapping)
        
        Args:
            product_name: Raw product name to match
        
        Returns:
            Parent product name
        """
        self.stats['total_queries'] += 1
        
        if not product_name:
            return product_name
        
        # Step 1: Try exact match first (current behavior)
        cleaned = re.sub(r'\s+', ' ', str(product_name)).strip().lower()
        
        if cleaned in self.child_to_parent_map:
            self.stats['exact_matches'] += 1
            return self.child_to_parent_map[cleaned]
        
        # Step 2: Try fuzzy match
        fuzzy_parent, score = self._find_fuzzy_match(product_name)
        
        if fuzzy_parent and score >= self.threshold:
            self.stats['fuzzy_matches'] += 1
            
            if self.dry_run:
                logger.info(f"   [DRY RUN] Would fuzzy match '{product_name}' → '{fuzzy_parent}' ({score}%)")
                # In dry run, return original name (don't actually use fuzzy match)
                self.stats['no_matches'] += 1
                return product_name
            else:
                logger.info(f"   🔍 Fuzzy matched '{product_name}' → '{fuzzy_parent}' ({score}%)")
                return fuzzy_parent
        
        # Step 3: No match found, return original (self-mapping)
        self.stats['no_matches'] += 1
        return product_name
    
    def _find_fuzzy_match(self, product_name: str) -> Tuple[Optional[str], int]:
        """
        Find best fuzzy match for a product name
        
        Args:
            product_name: Product name to match
        
        Returns:
            (best_match_name, similarity_score)
        """
        # Check cache
        if product_name in self.fuzzy_cache:
            return self.fuzzy_cache[product_name]
        
        if not self.parent_names:
            return None, 0
        
        # Use token_sort_ratio for better matching
        # This handles word order differences: "Apple Mango" vs "Mango Apple"
        best_match, score = process.extractOne(
            product_name,
            self.parent_names,
            scorer=fuzz.token_sort_ratio
        )
        
        # Cache result
        self.fuzzy_cache[product_name] = (best_match, score)
        
        return best_match, score
    
    def get_stats(self) -> dict:
        """Get matching statistics"""
        return self.stats.copy()
    
    def print_stats(self):
        """Print matching statistics"""
        logger.info("\n📊 FUZZY MATCHING STATISTICS")
        logger.info("=" * 80)
        logger.info(f"   • Total queries: {self.stats['total_queries']:,}")
        logger.info(f"   • Exact matches: {self.stats['exact_matches']:,}")
        logger.info(f"   • Fuzzy matches: {self.stats['fuzzy_matches']:,}")
        logger.info(f"   • No matches (self-mapped): {self.stats['no_matches']:,}")
        
        if self.stats['total_queries'] > 0:
            exact_rate = (self.stats['exact_matches'] / self.stats['total_queries'] * 100)
            fuzzy_rate = (self.stats['fuzzy_matches'] / self.stats['total_queries'] * 100)
            logger.info(f"   • Exact match rate: {exact_rate:.1f}%")
            logger.info(f"   • Fuzzy match rate: {fuzzy_rate:.1f}%")
            
            if self.dry_run and self.stats['fuzzy_matches'] > 0:
                logger.info(f"\n   💡 DRY RUN: {self.stats['fuzzy_matches']} products would benefit from fuzzy matching")
                logger.info(f"      Set dry_run=False to enable fuzzy matching")
        
        logger.info("=" * 80)
    
    def export_fuzzy_matches(self, output_file: str = 'logs/fuzzy_matches_review.csv'):
        """
        Export fuzzy matches to CSV for manual review
        
        Args:
            output_file: Path to output CSV file
        """
        import pandas as pd
        import os
        
        if not self.fuzzy_cache:
            logger.warning("No fuzzy matches to export")
            return
        
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Convert cache to DataFrame
        data = []
        for product_name, (matched_parent, score) in self.fuzzy_cache.items():
            if score >= self.threshold:
                data.append({
                    'original_product': product_name,
                    'matched_parent': matched_parent,
                    'similarity_score': score,
                    'recommendation': 'ACCEPT' if score >= 90 else 'REVIEW'
                })
        
        if data:
            df = pd.DataFrame(data)
            df = df.sort_values('similarity_score', ascending=False)
            df.to_csv(output_file, index=False)
            logger.info(f"📝 Exported {len(data)} fuzzy matches to: {output_file}")
        else:
            logger.info("No fuzzy matches above threshold to export")
