#!/usr/bin/env python3
"""
Master script to add parent_product_id to all 6 local tables at once
Runs all individual add_parent_id scripts in sequence
"""

import subprocess
import sys
import os
from datetime import datetime

def run_script(script_path, table_name):
    """Run a single add_parent_id script"""
    
    print(f"\nRunning {table_name} parent ID assignment...")
    print("=" * 60)
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(__file__))  # Go to Mapping directory
        )
        
        # Print the output
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print(f"SUCCESS: {table_name} parent ID assignment completed successfully!")
            return True
        else:
            print(f"ERROR: {table_name} parent ID assignment failed with return code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"ERROR: Error running {table_name} script: {e}")
        return False

def main():
    """Main function to run all add_parent_id scripts"""
    
    print("ADD PARENT ID TO ALL LOCAL TABLES")
    print("=" * 60)
    print("Source: canonical_products_master table")
    print("Goal: Add parent_product_id to all 6 local tables")
    print("=" * 60)
    
    # Define all scripts to run
    scripts = [
        {
            "path": "scripts/farm_prices/add_parent_id_to_farm_prices.py",
            "table": "farm_prices"
        },
        {
            "path": "scripts/supermarket/add_parent_id_to_supermarket_prices.py", 
            "table": "supermarket_prices"
        },
        {
            "path": "scripts/distribution_center/add_parent_id_to_distribution_center_prices.py",
            "table": "distribution_center_prices"
        },
        {
            "path": "scripts/local_shop/add_parent_id_to_local_shop_prices.py",
            "table": "local_shop_prices"
        },
        {
            "path": "scripts/ecommerce/add_parent_id_to_ecommerce_prices.py",
            "table": "ecommerce_prices"
        },
        {
            "path": "scripts/sunday_market/add_parent_id_to_sunday_market_prices.py",
            "table": "sunday_market_prices"
        }
    ]
    
    # Track results
    results = {}
    start_time = datetime.now()
    
    try:
        # Run each script
        for script_info in scripts:
            script_path = script_info["path"]
            table_name = script_info["table"]
            
            print(f"\n{'='*80}")
            print(f"Processing: {table_name.upper()}")
            print(f"Script: {script_path}")
            print(f"{'='*80}")
            
            success = run_script(script_path, table_name)
            results[table_name] = success
            
            if success:
                print(f"SUCCESS: {table_name} completed successfully")
            else:
                print(f"ERROR: {table_name} failed")
        
        # Calculate total time
        end_time = datetime.now()
        total_time = end_time - start_time
        
        # Show final summary
        print(f"\n{'='*80}")
        print("FINAL SUMMARY")
        print(f"{'='*80}")
        
        successful_tables = [table for table, success in results.items() if success]
        failed_tables = [table for table, success in results.items() if not success]
        
        print(f"SUCCESS: Successfully processed: {len(successful_tables)} tables")
        for table in successful_tables:
            print(f"   - {table}")
        
        if failed_tables:
            print(f"ERROR: Failed to process: {len(failed_tables)} tables")
            for table in failed_tables:
                print(f"   - {table}")
        
        print(f"\nTotal execution time: {total_time}")
        print(f"Success rate: {len(successful_tables)}/{len(scripts)} ({len(successful_tables)/len(scripts)*100:.1f}%)")
        
        if len(successful_tables) == len(scripts):
            print(f"\nALL TABLES PROCESSED SUCCESSFULLY!")
            print(f"All 6 local tables now have parent_product_id columns")
            print(f"Parent IDs match canonical_products_master table exactly")
        else:
            print(f"\nPARTIAL SUCCESS!")
            print(f"{len(successful_tables)}/{len(scripts)} tables processed successfully")
            print(f"ERROR: Some tables failed - check individual script outputs above")
        
        print(f"\nProcessed Tables:")
        for table in successful_tables:
            print(f"   SUCCESS: public.{table}")
        for table in failed_tables:
            print(f"   ERROR: public.{table}")
        
    except Exception as e:
        print(f"\nERROR: Error in main process: {e}")
        print("Please check individual script outputs above for details.")

if __name__ == "__main__":
    main()
