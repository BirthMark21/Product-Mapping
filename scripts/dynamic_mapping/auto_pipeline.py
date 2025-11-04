#!/usr/bin/env python3
"""
Automatic Pipeline for Dynamic Mapping System
Detects changes and triggers updates automatically
"""

import sys
import os
import json
import time
import hashlib
import subprocess
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def get_file_hash(file_path):
    """Get MD5 hash of a file to detect changes"""
    
    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()
            return hashlib.md5(file_content).hexdigest()
    except Exception as e:
        print(f"ERROR: Failed to get hash for {file_path}: {e}")
        return None

def load_last_hash():
    """Load last known hash from state file"""
    
    state_file = os.path.join(os.path.dirname(__file__), '.pipeline_state')
    
    try:
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                return f.read().strip()
        return None
    except Exception as e:
        print(f"ERROR: Failed to load last hash: {e}")
        return None

def save_last_hash(file_hash):
    """Save current hash to state file"""
    
    state_file = os.path.join(os.path.dirname(__file__), '.pipeline_state')
    
    try:
        with open(state_file, 'w') as f:
            f.write(file_hash)
        return True
    except Exception as e:
        print(f"ERROR: Failed to save hash: {e}")
        return False

def run_script(script_name, description):
    """Run a script and return success status"""
    
    print(f"🔄 {description}...")
    
    try:
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            return True
        else:
            print(f"❌ {description} failed")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to run {script_name}: {e}")
        return False

def check_for_changes():
    """Check if mapping configuration has changed"""
    
    config_file = os.path.join(os.path.dirname(__file__), 'mapping_config.json')
    
    if not os.path.exists(config_file):
        print("ERROR: mapping_config.json not found")
        return False
    
    # Get current file hash
    current_hash = get_file_hash(config_file)
    if not current_hash:
        return False
    
    # Get last known hash
    last_hash = load_last_hash()
    
    if last_hash is None:
        print("📝 First run detected - no previous hash found")
        return True
    
    if current_hash != last_hash:
        print("🔄 Changes detected in mapping configuration")
        return True
    else:
        print("✅ No changes detected")
        return False

def run_pipeline():
    """Run the complete pipeline"""
    
    print("🚀 Starting Dynamic Mapping Pipeline")
    print("=" * 50)
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Validate configuration
    if not run_script('validate_mapping.py', 'Validating mapping configuration'):
        print("❌ Pipeline stopped: Configuration validation failed")
        return False
    
    # Step 2: Update master tables
    if not run_script('update_mapping.py', 'Updating master tables'):
        print("❌ Pipeline stopped: Master table update failed")
        return False
    
    # Step 3: Apply to remote tables
    if not run_script('apply_dynamic_mapping.py', 'Applying mappings to remote tables'):
        print("❌ Pipeline stopped: Remote table update failed")
        return False
    
    # Step 4: Save current hash
    config_file = os.path.join(os.path.dirname(__file__), 'mapping_config.json')
    current_hash = get_file_hash(config_file)
    if current_hash:
        save_last_hash(current_hash)
        print("💾 Pipeline state saved")
    
    print("✅ Pipeline completed successfully!")
    return True

def run_continuous_monitoring(interval=60):
    """Run continuous monitoring for changes"""
    
    print("🔄 Starting continuous monitoring...")
    print(f"⏱️  Check interval: {interval} seconds")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Checking for changes...")
            
            if check_for_changes():
                print("🚀 Changes detected! Running pipeline...")
                run_pipeline()
            else:
                print("😴 No changes detected, sleeping...")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Monitoring error: {e}")

def run_single_check():
    """Run a single check and update if needed"""
    
    print("🔍 Running single check...")
    
    if check_for_changes():
        print("🚀 Changes detected! Running pipeline...")
        return run_pipeline()
    else:
        print("✅ No changes detected, nothing to do")
        return True

def main():
    """Main function with command line options"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Dynamic Mapping Auto Pipeline')
    parser.add_argument('--mode', choices=['single', 'continuous'], default='single',
                       help='Run mode: single check or continuous monitoring')
    parser.add_argument('--interval', type=int, default=60,
                       help='Check interval in seconds for continuous mode')
    parser.add_argument('--force', action='store_true',
                       help='Force pipeline run even if no changes detected')
    
    args = parser.parse_args()
    
    if args.force:
        print("🔧 Force mode: Running pipeline regardless of changes")
        return run_pipeline()
    
    if args.mode == 'continuous':
        run_continuous_monitoring(args.interval)
    else:
        run_single_check()

if __name__ == "__main__":
    main()
