import os
import json
import re
from typing import Dict, List, Optional

class BatchResultsMerger:
    def __init__(self):
        """Initialize the batch results merger"""
        # Results directories
        self.base_results_dir = r"E:\MCS_Project\BatchCreator\batchesWithInput\Results"
        self.instruction_results_dir = os.path.join(self.base_results_dir, "instruction_results")
        self.input_results_dir = os.path.join(self.base_results_dir, "input_results")
        self.output_results_dir = os.path.join(self.base_results_dir, "output_results")
        
        # Merged output directory
        self.merged_results_dir = os.path.join(self.base_results_dir, "merged_results")
        os.makedirs(self.merged_results_dir, exist_ok=True)

    def extract_text_from_result(self, result_entry: Dict) -> str:
        """Extract the translated text from a batch result entry"""
        try:
            # Navigate through the correct structure: result -> message -> content
            if 'result' in result_entry and result_entry['result']:
                result_data = result_entry['result']
                if 'message' in result_data and result_data['message']:
                    message = result_data['message']
                    if 'content' in message and message['content']:
                        content = message['content']
                        if isinstance(content, list) and len(content) > 0:
                            # Get the text from the first content item
                            first_content = content[0]
                            if isinstance(first_content, dict) and 'text' in first_content:
                                text_content = first_content['text']
                                
                                # Try to extract JSON from the text (it's wrapped in ```json code blocks)
                                try:
                                    # Remove code block markers
                                    json_match = re.search(r'```json\s*\n(.*?)\n```', text_content, re.DOTALL)
                                    if json_match:
                                        json_str = json_match.group(1).strip()
                                        parsed_json = json.loads(json_str)
                                        if isinstance(parsed_json, dict) and 'text' in parsed_json:
                                            return parsed_json['text'].strip()
                                    
                                    # If no JSON block found, try to parse the entire text as JSON
                                    parsed_json = json.loads(text_content)
                                    if isinstance(parsed_json, dict) and 'text' in parsed_json:
                                        return parsed_json['text'].strip()
                                        
                                except json.JSONDecodeError:
                                    # If JSON parsing fails, return the raw text
                                    return text_content.strip()
                                
                                return text_content.strip()
            
            return ""
            
        except Exception as e:
            print(f"⚠️  Error extracting text from result: {e}")
            return ""

    def load_batch_results(self, file_path: str) -> List[Dict]:
        """Load results from a batch results file"""
        results = []
        if not os.path.exists(file_path):
            print(f"❌ Results file not found: {file_path}")
            return []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        result = json.loads(line)
                        results.append(result)
                    except json.JSONDecodeError as e:
                        print(f"❌ Error parsing line {line_num} in {file_path}: {e}")
                        continue
        except Exception as e:
            print(f"❌ Error reading file {file_path}: {e}")
            return []
        
        return results

    def extract_line_number_from_custom_id(self, custom_id: str) -> Optional[int]:
        """Extract line number from custom_id like 'alpaca_data_with_input_instruction_line_0'"""
        try:
            # Look for the pattern '_line_NUMBER' at the end
            match = re.search(r'_line_(\d+)$', custom_id)
            if match:
                return int(match.group(1))
        except (ValueError, AttributeError):
            pass
        return None

    def create_single_merged_file(self):
        """Create a single merged file from all available batches"""
        print("🔄 Creating single merged file from all available batches...")
        print("=" * 70)
        
        # Find all available batch numbers
        available_batches = []
        
        if os.path.exists(self.instruction_results_dir):
            for filename in os.listdir(self.instruction_results_dir):
                if filename.startswith('results_batch_') and filename.endswith('.jsonl'):
                    try:
                        batch_num = int(filename.replace('results_batch_', '').replace('.jsonl', ''))
                        available_batches.append(batch_num)
                    except ValueError:
                        continue
        
        if not available_batches:
            print("❌ No batch result files found in instruction_results directory")
            return
        
        available_batches.sort()
        print(f"📂 Found batch results for batches: {available_batches}")
        
        # Collect all results from all batches
        all_instruction_results = {}
        all_input_results = {}
        all_output_results = {}
        
        # Load all instruction results
        print("\n📖 Loading instruction results...")
        for batch_num in available_batches:
            instruction_file = os.path.join(self.instruction_results_dir, f"results_batch_{batch_num}.jsonl")
            results = self.load_batch_results(instruction_file)
            for result in results:
                custom_id = result.get('custom_id', '')
                line_num = self.extract_line_number_from_custom_id(custom_id)
                if line_num is not None:
                    all_instruction_results[line_num] = self.extract_text_from_result(result)
        
        # Load all input results
        print("📝 Loading input results...")
        for batch_num in available_batches:
            input_file = os.path.join(self.input_results_dir, f"results_batch_{batch_num}.jsonl")
            results = self.load_batch_results(input_file)
            for result in results:
                custom_id = result.get('custom_id', '')
                line_num = self.extract_line_number_from_custom_id(custom_id)
                if line_num is not None:
                    all_input_results[line_num] = self.extract_text_from_result(result)
        
        # Load all output results
        print("📤 Loading output results...")
        for batch_num in available_batches:
            output_file = os.path.join(self.output_results_dir, f"results_batch_{batch_num}.jsonl")
            results = self.load_batch_results(output_file)
            for result in results:
                custom_id = result.get('custom_id', '')
                line_num = self.extract_line_number_from_custom_id(custom_id)
                if line_num is not None:
                    all_output_results[line_num] = self.extract_text_from_result(result)
        
        print(f"✅ Loaded {len(all_instruction_results)} instruction, {len(all_input_results)} input, {len(all_output_results)} output results")
        
        # Find common line numbers
        common_lines = set(all_instruction_results.keys()) & set(all_input_results.keys()) & set(all_output_results.keys())
        
        if not common_lines:
            print("❌ No matching line numbers found across all result types")
            return
        
        print(f"🔗 Found {len(common_lines)} matching entries across all types")
        
        # Create merged results
        merged_results = []
        for line_num in sorted(common_lines):
            merged_entry = {
                "instruction": all_instruction_results[line_num],
                "input": all_input_results[line_num],
                "output": all_output_results[line_num]
            }
            merged_results.append(merged_entry)
        
        # Save single merged file
        output_file_path = os.path.join(self.merged_results_dir, "merged_alpaca_data.jsonl")
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for entry in merged_results:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"\n🎉 Success!")
        print(f"💾 Saved {len(merged_results)} merged entries to: {output_file_path}")
        print(f"📊 File contains entries in format: {{\"instruction\": \"...\", \"input\": \"...\", \"output\": \"...\"}}")
        
        return output_file_path

    def display_sample_entries(self, num_samples: int = 5):
        """Display sample entries from the merged results"""
        merged_file_path = os.path.join(self.merged_results_dir, "merged_alpaca_data.jsonl")
        
        if not os.path.exists(merged_file_path):
            print("❌ Merged results file not found. Please run merge first.")
            return
        
        print(f"\n📋 Sample entries from merged results:")
        print("=" * 80)
        
        with open(merged_file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= num_samples:
                    break
                
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        print(f"\n📝 Entry {i+1}:")
                        print(f"   Instruction: {entry.get('instruction', 'N/A')}")
                        print(f"   Input: {entry.get('input', 'N/A')}")
                        print(f"   Output: {entry.get('output', 'N/A')}")
                        print("-" * 60)
                    except json.JSONDecodeError:
                        print(f"  ❌ Error parsing entry {i+1}")

    def validate_sample_extraction(self, batch_num: int = 1):
        """Test the text extraction on a sample to debug"""
        print(f"\n🔍 Testing text extraction on batch {batch_num} samples...")
        
        for result_type, results_dir in [
            ("instruction", self.instruction_results_dir),
            ("input", self.input_results_dir), 
            ("output", self.output_results_dir)
        ]:
            file_path = os.path.join(results_dir, f"results_batch_{batch_num}.jsonl")
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                continue
                
            print(f"\n📂 Testing {result_type} results:")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 2:  # Only test first 2 entries
                        break
                        
                    line = line.strip()
                    if line:
                        try:
                            result_entry = json.loads(line)
                            custom_id = result_entry.get('custom_id', 'N/A')
                            extracted_text = self.extract_text_from_result(result_entry)
                            line_num = self.extract_line_number_from_custom_id(custom_id)
                            
                            print(f"  Entry {i+1}:")
                            print(f"    Custom ID: {custom_id}")
                            print(f"    Line Number: {line_num}")
                            print(f"    Extracted Text: {extracted_text}")
                            print()
                            
                        except json.JSONDecodeError as e:
                            print(f"  ❌ JSON Error on line {i+1}: {e}")


def main():
    """Main function to run the merger"""
    print("🔄 Single File Batch Results Merger")
    print("   Merges instruction, input, and output results into one file")
    print("=" * 60)
    
    merger = BatchResultsMerger()
    
    while True:
        print("\n" + "=" * 60)
        print("OPTIONS:")
        print("1. Create single merged file (merged_alpaca_data.jsonl)")
        print("2. Display sample merged entries")
        print("3. Test text extraction (debugging)")
        print("4. Exit")
        print("=" * 60)
        
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == '1':
            merger.create_single_merged_file()
            
        elif choice == '2':
            try:
                num_samples = int(input("How many sample entries to display (default 5): ").strip() or "5")
                merger.display_sample_entries(num_samples)
            except ValueError:
                merger.display_sample_entries(5)
                
        elif choice == '3':
            try:
                batch_num = int(input("Enter batch number to test (default 1): ").strip() or "1")
                merger.validate_sample_extraction(batch_num)
            except ValueError:
                merger.validate_sample_extraction(1)
                
        elif choice == '4':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please enter 1-4.")

if __name__ == "__main__":
    main()