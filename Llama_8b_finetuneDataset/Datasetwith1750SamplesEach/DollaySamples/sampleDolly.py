import pandas as pd
import os

# Input and output paths
input_csv = r"E:\MCS_Project\Llama_8b_finetuneDataset\dolly_15k_ur_train.csv"
output_dir = r"E:\MCS_Project\Llama_8b_finetuneDataset\Datasetwith1750SamplesEach\DollaySamples"
os.makedirs(output_dir, exist_ok=True)

output_csv = os.path.join(output_dir, "dolly_1750_samples.csv")
output_jsonl = os.path.join(output_dir, "dolly_1750_samples.jsonl")
output_json = os.path.join(output_dir, "dolly_1750_samples.json")

# Read the CSV
df = pd.read_csv(input_csv)

# Sample 1750 random rows
df_sampled = df.sample(n=1750, random_state=42)

# Keep only the required columns (if they exist)
required_columns = ["instruction", "input", "output"]
df_sampled = df_sampled[required_columns]

# Save to CSV
df_sampled.to_csv(output_csv, index=False, encoding="utf-8")

# Save to JSONL (one object per line)
df_sampled.to_json(output_jsonl, orient="records", lines=True, force_ascii=False)

# Save to standard JSON array
df_sampled.to_json(output_json, orient="records", force_ascii=False)

print(f"Saved {len(df_sampled)} samples to:\n{output_csv}\n{output_jsonl}\n{output_json}")