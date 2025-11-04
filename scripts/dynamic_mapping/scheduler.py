#!/usr/bin/env python3
"""
Scheduled trigger for dynamic mapping pipeline
Runs at specified intervals using cron-like scheduling
"""

import sys
import os
import time
import schedule
import subprocess
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def run_pipeline():
    """Run the complete pipeline"""
    
    print("⏰ Scheduled pipeline execution")
    print("=" * 40)
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Run validation
        result1 = subprocess.run([sys.executable, 'validate_mapping.py'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result1.returncode != 0:
            print(f"❌ Validation failed: {result1.stderr}")
            return False
        
        # Run master table update
        result2 = subprocess.run([sys.executable, 'update_mapping.py'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result2.returncode != 0:
            print(f"❌ Master table update failed: {result2.stderr}")
            return False
        
        # Run remote table update
        result3 = subprocess.run([sys.executable, 'apply_dynamic_mapping.py'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result3.returncode != 0:
            print(f"❌ Remote table update failed: {result3.stderr}")
            return False
        
        print("✅ Scheduled pipeline completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Scheduled pipeline failed: {e}")
        return False

def setup_schedule():
    """Setup scheduled tasks"""
    
    print("📅 Setting up scheduled tasks...")
    
    # Run every hour
    schedule.every().hour.do(run_pipeline)
    
    # Run every day at 2 AM
    schedule.every().day.at("02:00").do(run_pipeline)
    
    # Run every Monday at 9 AM
    schedule.every().monday.at("09:00").do(run_pipeline)
    
    print("✅ Scheduled tasks configured:")
    print("  - Every hour")
    print("  - Daily at 2:00 AM")
    print("  - Weekly on Monday at 9:00 AM")

def run_scheduler():
    """Run the scheduler"""
    
    print("🔄 Starting scheduler...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\n🛑 Scheduler stopped by user")
    except Exception as e:
        print(f"❌ Scheduler error: {e}")

def main():
    """Main function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Dynamic Mapping Scheduler')
    parser.add_argument('--setup', action='store_true',
                       help='Setup scheduled tasks')
    parser.add_argument('--run', action='store_true',
                       help='Run scheduler')
    parser.add_argument('--test', action='store_true',
                       help='Test pipeline execution')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_schedule()
    elif args.run:
        setup_schedule()
        run_scheduler()
    elif args.test:
        print("🧪 Testing pipeline execution...")
        run_pipeline()
    else:
        print("Usage: python scheduler.py --setup --run")
        print("       python scheduler.py --test")

if __name__ == "__main__":
    main()
