#!/usr/bin/env python3
"""
Run all database migrations in order
"""

import sys
import os
import subprocess

def run_migration(script_name: str, description: str):
    """Run a single migration script"""
    print(f"\n{'=' * 80}")
    print(f"📄 {description}")
    print(f"   Script: {script_name}")
    print('=' * 80)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            return True
        else:
            print(f"❌ {description} - FAILED")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to run {script_name}: {e}")
        return False


def main():
    """Run all migrations in order"""
    print("\n" + "🚀" * 40)
    print("DATABASE MIGRATION RUNNER")
    print("🚀" * 40)
    
    migrations = [
        ('001_create_canonical_master.py', 'Create canonical_products_master table'),
        ('002_add_parent_columns_to_products.py', 'Add parent columns to products table'),
        ('003_add_parent_columns_to_product_names.py', 'Add parent columns to product_names table')
    ]
    
    results = []
    
    for script, description in migrations:
        success = run_migration(script, description)
        results.append((description, success))
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 MIGRATION SUMMARY")
    print("=" * 80)
    
    for description, success in results:
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"   {status}: {description}")
    
    total = len(results)
    successful = sum(1 for _, success in results if success)
    
    print(f"\n   Total: {successful}/{total} migrations successful")
    
    if successful == total:
        print("\n🎉 All migrations completed successfully!")
        print("\n📝 Next steps:")
        print("   1. Run: python pipeline/multi_source_standardization.py")
        print("   2. PeerDB will auto-sync Production PostgreSQL → ClickHouse")
    else:
        print(f"\n⚠️  {total - successful} migration(s) failed")
    
    print("=" * 80)
    
    return successful == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
