import pandas as pd
import os

def analyze_and_compare_products(benchmark_file, chip_chip_file, output_dir='product_analysis_exports'):
    """
    Reads two CSV files, extracts and analyzes the 'product_name' column,
    and exports the results into a designated folder.

    Args:
        benchmark_file (str): The file path for the benchmark dataset.
        chip_chip_file (str): The file path for the chip-chip dataset.
        output_dir (str): The directory to save the output files.
    """
    # --- 1. Setup and Error Handling ---
    try:
        # Create the output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")

        # Read both CSV files
        print("Reading CSV files...")
        benchmark_df = pd.read_csv(benchmark_file)
        chip_chip_df = pd.read_csv(chip_chip_file)
        print("Successfully read both CSV files.")

    except FileNotFoundError as e:
        print(f"\n--- ERROR ---")
        print(f"Error: {e}.")
        print("A file could not be found. Please check that the file paths at the bottom of the script are correct.")
        print("---------------")
        return
    except KeyError:
        print("Error: A 'product_name' column was not found in one of the files.")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

    # --- 2. Export 'product_name' Columns ---
    print("\nExporting full 'product_name' columns...")
    benchmark_df[['product_name']].to_csv(os.path.join(output_dir, 'benchmark_product_names.csv'), index=False)
    chip_chip_df[['product_name']].to_csv(os.path.join(output_dir, 'chip_chip_product_names.csv'), index=False)
    print(" -> Done: 'benchmark_product_names.csv' and 'chip_chip_product_names.csv' created.")

    # --- 3. Find and Export Unique Products ---
    print("\nFinding and exporting unique product names...")
    unique_benchmark_products = benchmark_df['product_name'].unique()
    unique_chip_chip_products = chip_chip_df['product_name'].unique()

    unique_benchmark_df = pd.DataFrame(unique_benchmark_products, columns=['unique_product_name'])
    unique_chip_chip_df = pd.DataFrame(unique_chip_chip_products, columns=['unique_product_name'])

    unique_benchmark_df.to_csv(os.path.join(output_dir, 'unique_benchmark_products.csv'), index=False)
    unique_chip_chip_df.to_csv(os.path.join(output_dir, 'unique_chip_chip_products.csv'), index=False)
    print(f" -> Done: 'unique_benchmark_products.csv' ({len(unique_benchmark_df)} unique items) created.")
    print(f" -> Done: 'unique_chip_chip_products.csv' ({len(unique_chip_chip_df)} unique items) created.")

    # --- 4. Create and Export Comparison Table ---
    print("\nCreating and exporting comparison table...")
    set_benchmark = set(unique_benchmark_products)
    set_chip_chip = set(unique_chip_chip_products)

    common_products = sorted(list(set_benchmark.intersection(set_chip_chip)))
    only_in_benchmark = sorted(list(set_benchmark.difference(set_chip_chip)))
    only_in_chip_chip = sorted(list(set_chip_chip.difference(set_benchmark)))

    comparison_dict = {
        'common_products': pd.Series(common_products),
        'only_in_benchmark_dataset': pd.Series(only_in_benchmark),
        'only_in_chip_chip_dataset': pd.Series(only_in_chip_chip)
    }
    comparison_df = pd.concat(comparison_dict, axis=1)

    comparison_df.to_csv(os.path.join(output_dir, 'product_comparison.csv'), index=False)
    print(" -> Done: 'product_comparison.csv' created.")
    print(f"\n--- Analysis complete. All files are in the '{output_dir}' directory. ---")


# --- Main execution block ---
if __name__ == "__main__":
    # Based on your screenshot, BOTH files are inside the 'chipoai_exports' folder.
    # The script should be run from the 'Mapping' folder.

    benchmark_file_path = 'chipoai_exports/Bench-Mark-Dataset.csv'
    
    # CORRECTED PATH for the second file
    chip_chip_file_path = 'chipoai_exports/Chip-Chip-Dataset-Ecommerce.csv'

    analyze_and_compare_products(benchmark_file_path, chip_chip_file_path)