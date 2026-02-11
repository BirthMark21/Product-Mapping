#!/usr/bin/env python3
"""
Git-based trigger for dynamic mapping pipeline
Triggers when mapping_config.json is updated in git repository
"""

import sys
import os
import json
import subprocess
import argparse
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def check_git_changes():
    """Check if mapping_config.json has been modified in git"""
    
    try:
        # Check if we're in a git repository
        result = subprocess.run(['git', 'status', '--porcelain'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result.returncode != 0:
            print("❌ Not in a git repository")
            return False
        
        # Check if mapping_config.json is modified
        modified_files = result.stdout.strip().split('\n')
        config_file = 'mapping_config.json'
        
        for file_status in modified_files:
            if file_status.strip().endswith(config_file):
                print(f"🔄 Git changes detected in {config_file}")
                return True
        
        print("✅ No git changes detected")
        return False
        
    except Exception as e:
        print(f"❌ Git check failed: {e}")
        return False

def run_pipeline():
    """Run the complete pipeline"""
    
    print("🚀 Git-triggered pipeline execution")
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
        
        print("✅ Git-triggered pipeline completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Git-triggered pipeline failed: {e}")
        return False

def setup_git_hooks():
    """Setup git hooks for automatic triggering"""
    
    print("🔧 Setting up git hooks...")
    
    try:
        # Create pre-commit hook
        hook_content = '''#!/bin/bash
# Pre-commit hook for dynamic mapping
echo "🔄 Checking for mapping changes..."

# Check if mapping_config.json is being committed
if git diff --cached --name-only | grep -q "mapping_config.json"; then
    echo "📝 mapping_config.json changes detected"
    echo "🚀 Triggering dynamic mapping pipeline..."
    
    # Run pipeline
    python scripts/dynamic_mapping/auto_pipeline.py --force
    
    if [ $? -eq 0 ]; then
        echo "✅ Pipeline completed successfully"
    else
        echo "❌ Pipeline failed - commit aborted"
        exit 1
    fi
fi
'''
        
        hook_path = os.path.join(os.path.dirname(__file__), '..', '..', '.git', 'hooks', 'pre-commit')
        
        with open(hook_path, 'w') as f:
            f.write(hook_content)
        
        # Make hook executable
        os.chmod(hook_path, 0o755)
        
        print("✅ Git hooks configured")
        print("📝 Pre-commit hook will trigger pipeline when mapping_config.json is modified")
        
    except Exception as e:
        print(f"❌ Failed to setup git hooks: {e}")

def main():
    """Main function"""
    
    parser = argparse.ArgumentParser(description='Git-based Dynamic Mapping Trigger')
    parser.add_argument('--check', action='store_true',
                       help='Check for git changes and run pipeline if needed')
    parser.add_argument('--setup', action='store_true',
                       help='Setup git hooks for automatic triggering')
    parser.add_argument('--force', action='store_true',
                       help='Force pipeline run regardless of git changes')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_git_hooks()
    elif args.force:
        print("🔧 Force mode: Running pipeline regardless of git changes")
        run_pipeline()
    elif args.check:
        if check_git_changes():
            run_pipeline()
        else:
            print("✅ No git changes detected, nothing to do")
    else:
        print("Usage: python git_trigger.py --check")
        print("       python git_trigger.py --setup")
        print("       python git_trigger.py --force")

if __name__ == "__main__":
    main()
