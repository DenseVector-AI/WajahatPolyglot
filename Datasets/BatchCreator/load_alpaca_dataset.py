import pandas as pd
import json
import os

alpaca_parquet_path = "hf://datasets/tatsu-lab/alpaca/data/train-00000-of-00001-a09b74b3ef9c3b56.parquet"

print(f"Loading Alpaca dataset from: {alpaca_parquet_path}")
try:
    alpaca_df = pd.read_parquet(alpaca_parquet_path, engine="pyarrow")
    print(f"Alpaca dataset loaded. Total entries: {len(alpaca_df)}")
except Exception as e:
    print(f"Error loading Parquet file: {e}")
    exit()

# filter to only entries with empty "input"
alpaca_filtered_all = alpaca_df[alpaca_df["input"].str.strip() == ""]
print(f"Total Alpaca entries with empty input: {len(alpaca_filtered_all)}")

# take entries 15 000–30 000 instead of the first 15 000
alpaca_subset = alpaca_filtered_all.iloc[15000:30000][["instruction", "input", "output"]]
print(f"Selected {len(alpaca_subset)} Alpaca entries (entries 15 000 through 29 999).")

final_dataset = []
for _, row in alpaca_subset.iterrows():
    final_dataset.append({
        "instruction": row["instruction"],
        "input":       row["input"],
        "output":      row["output"],
    })
print(f"Total entries in final dataset (Alpaca only): {len(final_dataset)}")

# Define output path in current directory
current_dir = os.getcwd()
output_filename = "alpaca_data.jsonl"
output_path = os.path.join(current_dir, output_filename)

print(f"Writing dataset to: {output_path}")
try:
    with open(output_path, "w", encoding="utf-8") as fout:
        for record in final_dataset:
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"Done! Wrote {len(final_dataset)} Alpaca entries to {output_path}")
except Exception as e:
    print(f"Error writing file '{output_path}': {e}")
