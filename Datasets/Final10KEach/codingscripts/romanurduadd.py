import json
import os

def add_sentence_to_instructions(input_path, output_path, sentence_to_add):
    """
    Add a sentence to the end of each instruction in a JSONL file
    """
    print(f"Reading JSONL file: {input_path}")
    
    try:
        # Check if input file exists
        if not os.path.exists(input_path):
            print(f"Error: File {input_path} does not exist!")
            return False
        
        # Read and process the JSONL file
        processed_entries = []
        total_entries = 0
        
        with open(input_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        # Parse JSON
                        entry = json.loads(line)
                        total_entries += 1
                        
                        # Add sentence to instruction
                        if 'instruction' in entry:
                            original_instruction = entry['instruction'].strip()
                            # Add the sentence at the end
                            entry['instruction'] = f"{original_instruction} {sentence_to_add}"
                        
                        processed_entries.append(entry)
                        
                    except json.JSONDecodeError as e:
                        print(f"Warning: Error parsing line {line_num}: {e}")
        
        print(f"Total entries processed: {total_entries}")
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write the modified entries to output file
        print(f"Writing modified file to: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            for entry in processed_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"✅ Successfully processed {len(processed_entries)} entries!")
        
        # Show statistics
        print("\n" + "="*50)
        print("PROCESSING STATISTICS")
        print("="*50)
        print(f"Input file: {input_path}")
        print(f"Output file: {output_path}")
        print(f"Total entries: {len(processed_entries)}")
        print(f"Sentence added: '{sentence_to_add}'")
        print("="*50)
        
        # Show preview of first few modified entries
        print("\nPREVIEW (First 3 modified instructions):")
        print("-"*30)
        for i, entry in enumerate(processed_entries[:3], 1):
            print(f"Entry {i}:")
            print(f"  Modified Instruction: {entry['instruction']}")
            print(f"  Input: {entry.get('input', 'N/A')}")
            print(f"  Output: {entry.get('output', 'N/A')[:50]}...")
            print()
        
        return True
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return False

def main():
    # File paths
    input_path = r"E:\MCS_Project\Final10KEach\RomanUrdu10k.jsonl"
    output_path = r"E:\MCS_Project\Final10KEach\RomanUrdu10kWithInstruction.jsonl"
    
    # Sentence to add
    sentence_to_add = "Is ka jawab Roman Urdu mein dijiye."
    
    print("Adding sentence to JSONL instructions...")
    print(f"Sentence to add: '{sentence_to_add}'")
    print()
    
    # Process the file
    success = add_sentence_to_instructions(input_path, output_path, sentence_to_add)
    
    if success:
        print(f"\n🎉 Success! Modified file saved as: {output_path}")
        print("\nYou can now:")
        print("1. Review the modified file")
        print("2. Replace the original file if satisfied")
        print("3. Delete the original and rename modified file")
    else:
        print("\n❌ Failed to process the file")

if __name__ == "__main__":
    main()