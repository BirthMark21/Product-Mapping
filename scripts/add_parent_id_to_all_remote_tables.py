#!/usr/bin/env python3
"""
Master script to add parent_product_id to all 6 remote Supabase tables
Using parent IDs from their respective master tables
"""

import subprocess
import sys
import os

def run_remote_script(folder_name, script_name):
    """Run a remote Supabase update script and return success status"""
    try:
        script_path = f"scripts/{folder_name}/{script_name}"
        print(f"Running {script_path}...")
        
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode == 0:
            print(f"SUCCESS: {script_path} completed successfully")
            return True
        else:
            print(f"ERROR: {script_path} failed with return code {result.returncode}")
            print(f"Error output: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"ERROR: Failed to run {script_path}: {e}")
        return False

def main():
    """Main function to run all remote Supabase update scripts"""
    
    print("Adding parent_product_id to all 6 REMOTE Supabase tables")
    print("USING PARENT IDs FROM THEIR RESPECTIVE MASTER TABLES")
    print("=" * 60)
    
    # Define the six folders and their remote update scripts
    remote_scripts = [
        ("farm_prices", "add_parent_id_to_remote_farm_prices.py"),
        ("supermarket", "add_parent_id_to_remote_supermarket_prices.py"),
        ("distribution_center", "add_parent_id_to_remote_distribution_center_prices.py"),
        ("local_shop", "add_parent_id_to_remote_local_shop_prices.py"),
        ("ecommerce", "add_parent_id_to_remote_ecommerce_prices.py"),
        ("sunday_market", "add_parent_id_to_remote_sunday_market_prices.py")
    ]
    
    success_count = 0
    total_count = len(remote_scripts)
    
    for folder_name, script_name in remote_scripts:
        print(f"\n--- Processing REMOTE {folder_name} ---")
        if run_remote_script(folder_name, script_name):
            success_count += 1
        print("-" * 40)
    
    # Summary
    print(f"\nSUMMARY:")
    print(f"Successfully updated: {success_count}/{total_count} remote Supabase tables")
    print(f"Failed: {total_count - success_count}/{total_count} remote tables")
    
    if success_count == total_count:
        print("SUCCESS: All remote Supabase tables updated successfully!")
    else:
        print("WARNING: Some remote updates failed. Check the error messages above.")

if __name__ == "__main__":
    main()
