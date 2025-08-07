from datasets import load_dataset
import json
import os

def load_mathinstruct_sample(sample_size=4000):
    """
    Load MathInstruct dataset and take first 4k with instruction/input/output format
    """
    print("Loading MathInstruct dataset...")
    try:
        ds = load_dataset("TIGER-Lab/MathInstruct")
        print(f"Dataset loaded successfully!")
        
        # Get the train split (or first available split)
        available_splits = list(ds.keys())
        print(f"Available splits: {available_splits}")
        
        # Use first available split
        split_name = available_splits[0]
        train_data = ds[split_name]
        print(f"Using split: {split_name}")
        print(f"Total entries in dataset: {len(train_data)}")
        
        # Convert to pandas DataFrame for easier handling
        df = train_data.to_pandas()
        print(f"Available columns: {list(df.columns)}")
        
        # Check if we have enough data
        if len(df) < sample_size:
            print(f"Warning: Dataset has only {len(df)} entries, taking all available")
            sample_size = len(df)
        
        # Take first 4k entries
        print(f"Taking first {sample_size} entries...")
        sample_df = df.head(sample_size)
        
        # Create final dataset with instruction, input, output format
        print("Converting to instruction-input-output format...")
        final_dataset = []
        
        for _, row in sample_df.iterrows():
            # Handle different possible column names
            instruction = ""
            input_text = ""
            output_text = ""
            
            # Try to find instruction column
            for col in ['instruction', 'question', 'problem', 'query', 'prompt']:
                if col in row and row[col]:
                    instruction = str(row[col]).strip()
                    break
            
            # Try to find output column
            for col in ['output', 'answer', 'solution', 'response', 'completion']:
                if col in row and row[col]:
                    output_text = str(row[col]).strip()
                    break
            
            # Try to find input column (usually empty for math problems)
            for col in ['input', 'context', 'background']:
                if col in row and row[col]:
                    input_text = str(row[col]).strip()
                    break
            
            # Ensure we have instruction and output at minimum
            if instruction and output_text:
                entry = {
                    "instruction": instruction,
                    "input": input_text,  # Empty string if no input found
                    "output": output_text
                }
                final_dataset.append(entry)
            else:
                print(f"Skipping entry - missing instruction or output")
        
        print(f"Created {len(final_dataset)} valid entries")
        
        # Show statistics
        print("\n" + "="*50)
        print("SAMPLING STATISTICS")
        print("="*50)
        print(f"Original dataset size: {len(train_data)}")
        print(f"First sample size: {sample_size}")
        print(f"Valid entries created: {len(final_dataset)}")
        print(f"Entries with non-empty input: {sum(1 for entry in final_dataset if entry['input'])}")
        print(f"Entries with empty input: {sum(1 for entry in final_dataset if not entry['input'])}")
        print("="*50)
        
        # Show preview
        print("\nPREVIEW (First 3 entries):")
        print("-"*30)
        for i, entry in enumerate(final_dataset[:3], 1):
            print(f"Entry {i}:")
            print(f"  Instruction: {entry['instruction'][:100]}...")
            print(f"  Input: '{entry['input'][:50]}...' " if entry['input'] else "  Input: (empty)")
            print(f"  Output: {entry['output'][:100]}...")
            print()
        
        return final_dataset
        
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return []

def save_to_jsonl(data, output_path):
    """
    Save data to JSONL file
    """
    print(f"Writing dataset to: {output_path}")
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            for entry in data:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(f"✅ Successfully wrote {len(data)} entries to {output_path}")
        return True
    except Exception as e:
        print(f"❌ Error writing file: {e}")
        return False

def main():
    # Load first 4k sample
    sample_data = load_mathinstruct_sample(sample_size=4000)
    
    if not sample_data:
        print("Failed to load sample data")
        return
    
    # Save to JSONL file
    output_path = os.path.join(os.getcwd(), "mathinstruct_4k_sample.jsonl")
    save_to_jsonl(sample_data, output_path)
    
    print(f"\n🎉 Complete! Created {len(sample_data)} math instruction examples in JSONL format")

if __name__ == "__main__":
    main()