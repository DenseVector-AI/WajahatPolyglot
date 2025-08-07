import pandas as pd
import json
import random
import os

def csv_to_jsonl_sample(csv_path, output_path, sample_size=10000):
    """
    Take a random sample from CSV and convert to JSONL format
    """
    print(f"Reading CSV file: {csv_path}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        print(f"Total rows in CSV: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        # Check if we have enough data
        if len(df) < sample_size:
            print(f"Warning: CSV has only {len(df)} rows, taking all available data")
            sample_size = len(df)
        
        # Take random sample
        print(f"Taking random sample of {sample_size} rows...")
        sample_df = df.sample(n=sample_size, random_state=42)  # random_state for reproducibility
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Convert to JSONL and write (only instruction, input, output fields)
        print(f"Writing JSONL file: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            for _, row in sample_df.iterrows():
                # Extract only instruction, input, output fields
                json_obj = {
                    "instruction": row.get('instruction', ''),
                    "input": row.get('input', ''),
                    "output": row.get('output', '')
                }
                f.write(json.dumps(json_obj, ensure_ascii=False) + '\n')
        
        print(f"Successfully created JSONL file with {sample_size} entries!")
        
        # Show statistics
        print("\n" + "="*50)
        print("SAMPLING STATISTICS")
        print("="*50)
        print(f"Original CSV rows: {len(df)}")
        print(f"Sample size: {sample_size}")
        print(f"Sampling ratio: {sample_size/len(df)*100:.2f}%")
        print("="*50)
        
        # Show preview of first few entries
        print("\nPREVIEW (First 3 entries):")
        print("-"*30)
        for i, (_, row) in enumerate(sample_df.head(3).iterrows(), 1):
            json_obj = {
                "instruction": row.get('instruction', ''),
                "input": row.get('input', ''),
                "output": row.get('output', '')
            }
            print(f"Entry {i}: {json.dumps(json_obj, ensure_ascii=False)}")
        
        return sample_size
        
    except FileNotFoundError:
        print(f"Error: File {csv_path} not found!")
        return 0
    except Exception as e:
        print(f"Error processing file: {e}")
        return 0

def main():
    # File paths
    csv_path = r"E:\MCS_Project\Final10KEach\dolly_15k_ur_train.csv"
    output_path = r"E:\MCS_Project\Final10KEach\dolly_10k_random_sample.jsonl"
    
    # Check if input file exists
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} does not exist!")
        return
    
    # Take sample and convert to JSONL
    sample_size = csv_to_jsonl_sample(csv_path, output_path, sample_size=10000)
    
    if sample_size > 0:
        print(f"\n✅ Success! Created {output_path} with {sample_size} random entries")
    else:
        print("\n❌ Failed to create sample file")

if __name__ == "__main__":
    main()