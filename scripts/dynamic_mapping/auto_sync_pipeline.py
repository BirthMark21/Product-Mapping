#!/usr/bin/env python3
"""
Auto-Sync Pipeline
Monitors mapping changes and automatically runs ETL pipeline
"""

import sys
import os
import time
import subprocess
from pathlib import Path
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.dynamic_mapping.dynamic_mapping_manager import DynamicMappingManager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AutoSyncPipeline:
    """Automatically syncs mappings and runs ETL pipeline"""
    
    def __init__(self, check_interval: int = 60):
        self.check_interval = check_interval
        self.manager = DynamicMappingManager()
        self.last_hash = self.manager.get_file_hash()
        self.pipeline_script = Path(__file__).parent.parent.parent / 'pipeline' / 'production_etl.py'
    
    def check_for_changes(self) -> bool:
        """Check if mapping file has changed"""
        current_hash = self.manager.get_file_hash()
        
        if current_hash != self.last_hash:
            logger.info("🔔 Mapping file changed detected!")
            self.last_hash = current_hash
            return True
        
        return False
    
    def validate_mappings(self) -> bool:
        """Validate mappings before running pipeline"""
        logger.info("🔍 Validating mappings...")
        
        # Reload mappings
        self.manager = DynamicMappingManager()
        
        # Validate
        is_valid = self.manager.validate_mappings()
        
        if not is_valid:
            logger.error("❌ Mapping validation failed! Pipeline will not run.")
            return False
        
        logger.info("✅ Mappings are valid")
        return True
    
    def export_mappings_to_python(self):
        """Export mappings to Python format"""
        logger.info("📤 Exporting mappings to Python format...")
        try:
            self.manager.export_to_python()
            logger.info("✅ Mappings exported successfully")
        except Exception as e:
            logger.error(f"❌ Failed to export mappings: {e}")
            raise
    
    def run_etl_pipeline(self) -> bool:
        """Run the ETL pipeline"""
        logger.info("🚀 Running ETL pipeline...")
        logger.info(f"   Script: {self.pipeline_script}")
        
        try:
            result = subprocess.run(
                [sys.executable, str(self.pipeline_script)],
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            # Print pipeline output
            if result.stdout:
                print(result.stdout)
            
            if result.returncode == 0:
                logger.info("✅ ETL pipeline completed successfully")
                return True
            else:
                logger.error(f"❌ ETL pipeline failed with code {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ ETL pipeline timed out after 10 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to run ETL pipeline: {e}")
            return False
    
    def run_pipeline_sequence(self):
        """Run the complete pipeline sequence"""
        logger.info("\n" + "=" * 80)
        logger.info("🔄 STARTING AUTO-SYNC PIPELINE SEQUENCE")
        logger.info("=" * 80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        
        # Step 1: Validate mappings
        if not self.validate_mappings():
            logger.error("❌ Pipeline sequence aborted due to validation failure")
            return False
        
        # Step 2: Export to Python
        try:
            self.export_mappings_to_python()
        except Exception as e:
            logger.error(f"❌ Pipeline sequence aborted: {e}")
            return False
        
        # Step 3: Run ETL pipeline
        success = self.run_etl_pipeline()
        
        if success:
            logger.info("\n" + "=" * 80)
            logger.info("🎉 AUTO-SYNC PIPELINE SEQUENCE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
        else:
            logger.error("\n" + "=" * 80)
            logger.error("❌ AUTO-SYNC PIPELINE SEQUENCE FAILED")
            logger.error("=" * 80)
        
        return success
    
    def run_continuous_monitoring(self):
        """Continuously monitor for changes and run pipeline"""
        logger.info("\n" + "🔄" * 40)
        logger.info("AUTO-SYNC PIPELINE - CONTINUOUS MONITORING MODE")
        logger.info("🔄" * 40)
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info(f"Monitoring file: {self.manager.mapping_file}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        try:
            while True:
                if self.check_for_changes():
                    logger.info(f"\n{'🔔' * 40}")
                    logger.info("CHANGE DETECTED - TRIGGERING PIPELINE")
                    logger.info(f"{'🔔' * 40}\n")
                    
                    self.run_pipeline_sequence()
                else:
                    logger.debug(f"No changes detected. Next check in {self.check_interval}s...")
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("\n\n⏹️  Monitoring stopped by user")
            logger.info("=" * 80)
    
    def run_single_check(self):
        """Run a single check and pipeline if changes detected"""
        logger.info("\n" + "=" * 80)
        logger.info("AUTO-SYNC PIPELINE - SINGLE CHECK MODE")
        logger.info("=" * 80)
        
        if self.check_for_changes():
            logger.info("🔔 Changes detected - running pipeline")
            return self.run_pipeline_sequence()
        else:
            logger.info("✅ No changes detected - skipping pipeline")
            return True


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Auto-Sync Pipeline')
    parser.add_argument('--monitor', action='store_true', 
                       help='Run in continuous monitoring mode')
    parser.add_argument('--interval', type=int, default=60,
                       help='Check interval in seconds (default: 60)')
    parser.add_argument('--force', action='store_true',
                       help='Force run pipeline regardless of changes')
    
    args = parser.parse_args()
    
    pipeline = AutoSyncPipeline(check_interval=args.interval)
    
    if args.force:
        logger.info("🔥 FORCE MODE - Running pipeline regardless of changes")
        success = pipeline.run_pipeline_sequence()
    elif args.monitor:
        pipeline.run_continuous_monitoring()
        success = True
    else:
        success = pipeline.run_single_check()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
