import json
import os

def merge_jsonl_files(file1_path, file2_path, output_path):
    """
    Merge two JSONL files and return statistics
    """
    file1_entries = []
    file2_entries = []
    
    # Read first file
    print(f"Reading {file1_path}...")
    try:
        with open(file1_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        file1_entries.append(entry)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Error parsing line {line_num} in {file1_path}: {e}")
        
        print(f"File 1 entries: {len(file1_entries)}")
    except FileNotFoundError:
        print(f"Error: File {file1_path} not found!")
        return
    except Exception as e:
        print(f"Error reading {file1_path}: {e}")
        return
    
    # Read second file
    print(f"Reading {file2_path}...")
    try:
        with open(file2_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        file2_entries.append(entry)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Error parsing line {line_num} in {file2_path}: {e}")
        
        print(f"File 2 entries: {len(file2_entries)}")
    except FileNotFoundError:
        print(f"Error: File {file2_path} not found!")
        return
    except Exception as e:
        print(f"Error reading {file2_path}: {e}")
        return
    
    # Merge entries
    merged_entries = file1_entries + file2_entries
    total_entries = len(merged_entries)
    
    # Write merged file
    print(f"Writing merged file to {output_path}...")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for entry in merged_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"Successfully merged files!")
    except Exception as e:
        print(f"Error writing merged file: {e}")
        return
    
    # Print statistics
    print("\n" + "="*50)
    print("MERGE STATISTICS")
    print("="*50)
    print(f"File 1 entries: {len(file1_entries)}")
    print(f"File 2 entries: {len(file2_entries)}")
    print(f"Total entries: {total_entries}")
    print("="*50)
    
    # Show preview of first few entries
    print("\nPREVIEW (First 3 entries):")
    print("-"*30)
    for i, entry in enumerate(merged_entries[:3], 1):
        print(f"Entry {i}: {json.dumps(entry, ensure_ascii=False)}")
    
    if total_entries > 3:
        print(f"... and {total_entries - 3} more entries")

def main():
    # File paths
    file1_path = r"E:\MCS_Project\BatchCreator\batchesWithInput\Results\merged_results\merged_alpaca_data.jsonl"
    file2_path = r"E:\MCS_Project\BatchCreator\batchesWithInput\Results\merged_results\merged_instruction_output.jsonl"
    output_path = r"E:\MCS_Project\BatchCreator\batchesWithInput\Results\merged_results\final_merged_data10k.jsonl"
    
    # Check if files exist
    if not os.path.exists(file1_path):
        print(f"Error: {file1_path} does not exist!")
        return
    
    if not os.path.exists(file2_path):
        print(f"Error: {file2_path} does not exist!")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Merge files
    merge_jsonl_files(file1_path, file2_path, output_path)

if __name__ == "__main__":
    main()