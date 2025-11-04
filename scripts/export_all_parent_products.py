#!/usr/bin/env python3
"""
Master script to export parent products from all six master tables
"""

import subprocess
import sys
import os

def run_export_script(folder_name, script_name):
    """Run an export script and return success status"""
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
    """Main function to run all export scripts"""
    
    print("Exporting parent products from all six master tables")
    print("=" * 60)
    
    # Define the six folders and their export scripts
    export_scripts = [
        ("farm_prices", "export_parent_products.py"),
        ("supermarket", "export_parent_products.py"),
        ("distribution_center", "export_parent_products.py"),
        ("local_shop", "export_parent_products.py"),
        ("ecommerce", "export_parent_products.py"),
        ("sunday_market", "export_parent_products.py")
    ]
    
    success_count = 0
    total_count = len(export_scripts)
    
    for folder_name, script_name in export_scripts:
        print(f"\n--- Processing {folder_name} ---")
        if run_export_script(folder_name, script_name):
            success_count += 1
        print("-" * 40)
    
    # Summary
    print(f"\nSUMMARY:")
    print(f"Successfully exported: {success_count}/{total_count} tables")
    print(f"Failed: {total_count - success_count}/{total_count} tables")
    
    if success_count == total_count:
        print("SUCCESS: All parent product exports completed successfully!")
    else:
        print("WARNING: Some exports failed. Check the error messages above.")

if __name__ == "__main__":
    main()
