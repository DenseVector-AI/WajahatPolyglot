import os
import pandas as pd
import glob
import random
import json

base_dir = r"E:\MCS_Project\Llama_8b_finetuneDataset\Datasetwith1750SamplesEach"
output_csv = os.path.join(base_dir, "merged_1750_samples.csv")
output_jsonl = os.path.join(base_dir, "merged_1750_samples.jsonl")

# --- Merge CSVs ---
csv_files = glob.glob(os.path.join(base_dir, "**", "*.csv"), recursive=True)
csv_dfs = []
for f in csv_files:
    try:
        df = pd.read_csv(f)
        csv_dfs.append(df)
    except Exception as e:
        print(f"Error reading {f}: {e}")

if csv_dfs:
    merged_csv = pd.concat(csv_dfs, ignore_index=True)
    merged_csv = merged_csv.sample(frac=1, random_state=42).reset_index(drop=True)  # Shuffle
    merged_csv.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Merged CSV saved to {output_csv} with {len(merged_csv)} rows.")
else:
    print("No CSV files found.")

# --- Merge JSONL and JSONs ---
jsonl_files = glob.glob(os.path.join(base_dir, "**", "*.jsonl"), recursive=True)
json_files = glob.glob(os.path.join(base_dir, "**", "*.json"), recursive=True)
jsonl_records = []

# Read JSONL files
for f in jsonl_files:
    try:
        with open(f, "r", encoding="utf-8") as infile:
            for line in infile:
                if line.strip():
                    jsonl_records.append(json.loads(line))
    except Exception as e:
        print(f"Error reading {f}: {e}")

# Read JSON files (expecting a list of dicts)
for f in json_files:
    try:
        with open(f, "r", encoding="utf-8") as infile:
            data = json.load(infile)
            if isinstance(data, list):
                jsonl_records.extend(data)
            else:
                print(f"Warning: {f} does not contain a list, skipping.")
    except Exception as e:
        print(f"Error reading {f}: {e}")

if jsonl_records:
    random.seed(42)
    random.shuffle(jsonl_records)
    with open(output_jsonl, "w", encoding="utf-8") as outfile:
        for record in jsonl_records:
            outfile.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"Merged JSONL saved to {output_jsonl} with {len(jsonl_records)} records.")
else:
    print("No JSONL or JSON files found.")