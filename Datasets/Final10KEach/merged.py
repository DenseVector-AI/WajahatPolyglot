import json
import os
import random
import glob

def merge_and_shuffle_jsonl_files(folder_path, output_path, shuffle=True):
    """
    Merge all JSONL files in a folder and optionally shuffle them
    """
    print(f"Scanning folder: {folder_path}")
    
    # Find all JSONL files in the folder
    jsonl_pattern = os.path.join(folder_path, "*.jsonl")
    jsonl_files = glob.glob(jsonl_pattern)
    
    if not jsonl_files:
        print("No JSONL files found in the folder!")
        return False
    
    print(f"Found {len(jsonl_files)} JSONL files:")
    for file in jsonl_files:
        print(f"  - {os.path.basename(file)}")
    
    all_entries = []
    file_stats = {}
    
    # Read all JSONL files
    for file_path in jsonl_files:
        filename = os.path.basename(file_path)
        print(f"\nReading {filename}...")
        
        file_entries = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:
                        try:
                            entry = json.loads(line)
                            # Ensure entry has required fields
                            if 'instruction' in entry and 'output' in entry:
                                # Ensure input field exists (set to empty if missing)
                                if 'input' not in entry:
                                    entry['input'] = ""
                                file_entries.append(entry)
                            else:
                                print(f"  Warning: Entry at line {line_num} missing required fields")
                        except json.JSONDecodeError as e:
                            print(f"  Warning: Error parsing line {line_num}: {e}")
            
            file_stats[filename] = len(file_entries)
            all_entries.extend(file_entries)
            print(f"  Loaded {len(file_entries)} entries from {filename}")
            
        except Exception as e:
            print(f"  Error reading {filename}: {e}")
            continue
    
    if not all_entries:
        print("No valid entries found in any file!")
        return False
    
    print(f"\nTotal entries loaded: {len(all_entries)}")
    
    # Shuffle if requested
    if shuffle:
        print("Shuffling all entries...")
        random.seed(42)  # For reproducible shuffling
        random.shuffle(all_entries)
        print("Entries shuffled!")
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write merged file
    print(f"\nWriting merged file to: {output_path}")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for entry in all_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"✅ Successfully created merged file with {len(all_entries)} entries!")
        
    except Exception as e:
        print(f"❌ Error writing merged file: {e}")
        return False
    
    # Show detailed statistics
    print("\n" + "="*60)
    print("MERGE STATISTICS")
    print("="*60)
    print("File breakdown:")
    for filename, count in file_stats.items():
        percentage = (count / len(all_entries)) * 100
        print(f"  {filename:<30} : {count:>6} entries ({percentage:>5.1f}%)")
    
    print(f"\nTotal files merged: {len(file_stats)}")
    print(f"Total entries: {len(all_entries)}")
    print(f"Shuffled: {'Yes' if shuffle else 'No'}")
    print("="*60)
    
    # Show preview
    print("\nPREVIEW (First 5 entries after merge/shuffle):")
    print("-"*40)
    for i, entry in enumerate(all_entries[:5], 1):
        print(f"Entry {i}:")
        print(f"  Instruction: {entry['instruction'][:80]}...")
        print(f"  Input: '{entry['input'][:30]}...'" if entry['input'] else "  Input: (empty)")
        print(f"  Output: {entry['output'][:80]}...")
        print()
    
    return True

def main():
    # Folder containing JSONL files
    folder_path = r"E:\MCS_Project\Final10KEach"
    
    # Output merged file
    output_path = os.path.join(folder_path, "merged_shuffled_all.jsonl")
    
    print("🔄 Merging and Shuffling All JSONL Files")
    print("="*50)
    
    # Check if folder exists
    if not os.path.exists(folder_path):
        print(f"Error: Folder {folder_path} does not exist!")
        return
    
    # Merge and shuffle all JSONL files
    success = merge_and_shuffle_jsonl_files(folder_path, output_path, shuffle=True)
    
    if success:
        print(f"\n🎉 Success! All JSONL files merged and shuffled!")
        print(f"📁 Output file: {output_path}")
        print("\n💡 The merged file contains all entries from:")
        print("   - All JSONL files in the folder")
        print("   - Shuffled randomly for better training distribution")
        print("   - Standardized instruction-input-output format")
    else:
        print("\n❌ Failed to merge files")

if __name__ == "__main__":
    main()