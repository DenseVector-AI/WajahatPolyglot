import pandas as pd
import json
import os
import random

alpaca_parquet_path = "hf://datasets/tatsu-lab/alpaca/data/train-00000-of-00001-a09b74b3ef9c3b56.parquet"

print(f"Loading Alpaca dataset from: {alpaca_parquet_path}")
try:
    alpaca_df = pd.read_parquet(alpaca_parquet_path, engine="pyarrow")
    print(f"Alpaca dataset loaded. Total entries: {len(alpaca_df)}")
except Exception as e:
    print(f"Error loading Parquet file: {e}")
    exit()

# Filter to only entries with non-empty "input"
alpaca_filtered = alpaca_df[alpaca_df["input"].str.strip() != ""]
print(f"Total Alpaca entries with non-empty input: {len(alpaca_filtered)}")

# Skip first 30k entries and take remaining
if len(alpaca_filtered) <= 30000:
    print(f"Warning: Dataset has only {len(alpaca_filtered)} entries, which is <= 30k. No entries to skip.")
    alpaca_after_30k = alpaca_filtered
else:
    alpaca_after_30k = alpaca_filtered.iloc[30000:]
    print(f"Entries available after skipping first 30k: {len(alpaca_after_30k)}")

# Take random 7k sample from remaining entries
sample_size = 7000
if len(alpaca_after_30k) < sample_size:
    print(f"Warning: Only {len(alpaca_after_30k)} entries available after 30k, taking all available")
    sample_size = len(alpaca_after_30k)

print(f"Taking random sample of {sample_size} entries from position 30k onward...")
alpaca_subset = alpaca_after_30k.sample(n=sample_size, random_state=42)[["instruction", "input", "output"]]
print(f"Selected {len(alpaca_subset)} Alpaca entries (random sample from 30k+ range).")

# Create final dataset
final_dataset = [
    {"instruction": row["instruction"], "input": row["input"], "output": row["output"]}
    for _, row in alpaca_subset.iterrows()
]
print(f"Total entries in final dataset: {len(final_dataset)}")

# Show statistics
print("\n" + "="*50)
print("SAMPLING STATISTICS")
print("="*50)
print(f"Original dataset size: {len(alpaca_df)}")
print(f"Entries with non-empty input: {len(alpaca_filtered)}")
print(f"Entries after skipping first 30k: {len(alpaca_after_30k)}")
print(f"Random sample size: {sample_size}")
print("="*50)

# Show preview of first few entries
print("\nPREVIEW (First 3 entries):")
print("-"*30)
for i, record in enumerate(final_dataset[:3], 1):
    print(f"Entry {i}: {json.dumps(record, ensure_ascii=False)}")

# Write out to JSONL
output_path = os.path.join(os.getcwd(), "alpaca_7k_random_sample_from_30k.jsonl")
print(f"\nWriting dataset to: {output_path}")
try:
    with open(output_path, "w", encoding="utf-8") as fout:
        for record in final_dataset:
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"✅ Done! Wrote {len(final_dataset)} entries to {output_path}")
except Exception as e:
    print(f"❌ Error writing file '{output_path}': {e}")