#!/usr/bin/env python3
"""
Data Quality Validator
Validates product data before processing to prevent bad data from entering the system
"""

import pandas as pd
import re
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ProductDataValidator:
    """Comprehensive data quality validation for product data"""
    
    def __init__(self):
        self.validation_rules = {
            'min_name_length': 2,
            'max_name_length': 255,
            'invalid_patterns': [
                r'^test',
                r'^dummy',
                r'^sample',
                r'^xxx',
                r'^\d+$',  # Only numbers
                r'^[^a-zA-Z0-9]+$',  # Only special chars
            ],
            'suspicious_chars': ['�', '???', 'NULL', 'null', 'undefined', 'None']
        }
        self.issues = []
        self.stats = {
            'total_input': 0,
            'null_names': 0,
            'test_data': 0,
            'encoding_issues': 0,
            'invalid_length': 0,
            'future_dates': 0,
            'invalid_dates': 0,
            'duplicates': 0,
            'total_removed': 0
        }
    
    def validate_product_name(self, name: str) -> tuple[bool, list]:
        """
        Validate a single product name
        
        Returns:
            (is_valid, issues_list)
        """
        issues = []
        
        if not name or not isinstance(name, str):
            return False, ["Name is null or not a string"]
        
        # Check length
        if len(name) < self.validation_rules['min_name_length']:
            issues.append(f"Name too short: '{name}'")
        
        if len(name) > self.validation_rules['max_name_length']:
            issues.append(f"Name too long: '{name[:50]}...'")
        
        # Check for invalid patterns
        for pattern in self.validation_rules['invalid_patterns']:
            if re.match(pattern, name, re.IGNORECASE):
                issues.append(f"Invalid pattern '{pattern}' in '{name}'")
        
        # Check for suspicious characters
        for char in self.validation_rules['suspicious_chars']:
            if char in name:
                issues.append(f"Suspicious character '{char}' in '{name}'")
        
        return len(issues) == 0, issues
    
    def validate_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Validate entire DataFrame and return cleaned version
        
        Args:
            df: Input DataFrame with columns: raw_product_id, raw_product_name, created_at, source_db
        
        Returns:
            (cleaned_df, stats_dict)
        """
        logger.info("\n🔍 RUNNING DATA QUALITY CHECKS")
        logger.info("=" * 80)
        
        self.stats['total_input'] = len(df)
        original_count = len(df)
        
        if df.empty:
            logger.warning("   ⚠️  Input DataFrame is empty")
            return df, self.stats
        
        # 1. Check for nulls
        logger.info("   → Checking for null product names...")
        null_mask = df['raw_product_name'].isnull()
        self.stats['null_names'] = null_mask.sum()
        
        if self.stats['null_names'] > 0:
            logger.warning(f"   ⚠️  Found {self.stats['null_names']} null product names - removing")
            df = df[~null_mask]
        
        # 2. Check for test/dummy data
        logger.info("   → Checking for test/dummy data...")
        test_pattern = r'^(test|dummy|sample|xxx)'
        test_mask = df['raw_product_name'].str.contains(test_pattern, case=False, na=False, regex=True)
        self.stats['test_data'] = test_mask.sum()
        
        if self.stats['test_data'] > 0:
            logger.warning(f"   ⚠️  Found {self.stats['test_data']} test/dummy products - removing")
            df = df[~test_mask]
        
        # 3. Check for encoding issues
        logger.info("   → Checking for encoding issues...")
        encoding_mask = df['raw_product_name'].str.contains('�', na=False)
        self.stats['encoding_issues'] = encoding_mask.sum()
        
        if self.stats['encoding_issues'] > 0:
            logger.warning(f"   ⚠️  Found {self.stats['encoding_issues']} products with encoding issues - removing")
            df = df[~encoding_mask]
        
        # 4. Length validation
        logger.info("   → Validating product name lengths...")
        length_mask = df['raw_product_name'].str.len().between(
            self.validation_rules['min_name_length'],
            self.validation_rules['max_name_length']
        )
        invalid_length = (~length_mask).sum()
        self.stats['invalid_length'] = invalid_length
        
        if invalid_length > 0:
            logger.warning(f"   ⚠️  Found {invalid_length} products with invalid length - removing")
            df = df[length_mask]
        
        # 5. Validate timestamps
        logger.info("   → Validating timestamps...")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        
        # Check for future dates
        future_mask = df['created_at'] > (datetime.now(df['created_at'].dt.tz) + timedelta(days=1))
        self.stats['future_dates'] = future_mask.sum()
        
        if self.stats['future_dates'] > 0:
            logger.warning(f"   ⚠️  Found {self.stats['future_dates']} products with future dates - removing")
            df = df[~future_mask]
        
        # Check for invalid dates
        null_dates_mask = df['created_at'].isnull()
        self.stats['invalid_dates'] = null_dates_mask.sum()
        
        if self.stats['invalid_dates'] > 0:
            logger.warning(f"   ⚠️  Found {self.stats['invalid_dates']} products with invalid timestamps - removing")
            df = df[~null_dates_mask]
        
        # 6. Check for duplicates within same source
        logger.info("   → Checking for duplicates...")
        duplicates = df.groupby(['source_db', 'raw_product_name']).size()
        duplicates = duplicates[duplicates > 1]
        self.stats['duplicates'] = len(duplicates)
        
        if self.stats['duplicates'] > 0:
            logger.warning(f"   ⚠️  Found {self.stats['duplicates']} duplicate product names within same source - KEEPING ALL")
            # User requested to keep duplicates
            # df = df.drop_duplicates(subset=['source_db', 'raw_product_name'], keep='first')
        
        # Calculate total removed
        self.stats['total_removed'] = original_count - len(df)
        
        # Generate report
        self._generate_report()
        
        return df, self.stats
    
    def _generate_report(self):
        """Generate validation report"""
        logger.info("\n📊 DATA QUALITY REPORT")
        logger.info("=" * 80)
        logger.info(f"   • Total input records: {self.stats['total_input']:,}")
        logger.info(f"   • Null names removed: {self.stats['null_names']:,}")
        logger.info(f"   • Test data removed: {self.stats['test_data']:,}")
        logger.info(f"   • Encoding issues removed: {self.stats['encoding_issues']:,}")
        logger.info(f"   • Invalid length removed: {self.stats['invalid_length']:,}")
        logger.info(f"   • Future dates removed: {self.stats['future_dates']:,}")
        logger.info(f"   • Invalid dates removed: {self.stats['invalid_dates']:,}")
        logger.info(f"   • Duplicates detected (kept): {self.stats['duplicates']:,}")
        logger.info(f"   • Total removed: {self.stats['total_removed']:,}")
        logger.info(f"   • Final valid records: {self.stats['total_input'] - self.stats['total_removed']:,}")
        
        if self.stats['total_removed'] > 0:
            removal_rate = (self.stats['total_removed'] / self.stats['total_input'] * 100)
            logger.info(f"   • Removal rate: {removal_rate:.2f}%")
            
            if removal_rate > 10:
                logger.warning(f"   ⚠️  High removal rate ({removal_rate:.1f}%) - investigate data quality issues")
        else:
            logger.info("   ✅ All data quality checks passed!")
        
        logger.info("=" * 80)
