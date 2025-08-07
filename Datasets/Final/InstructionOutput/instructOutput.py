import os
import json
import re
from typing import Dict, List, Optional

class InstructionOutputMerger:
    def __init__(self):
        """Initialize the instruction-output merger (no input folder)"""
        # Base directory for this merger
        self.base_dir = r"E:\MCS_Project\Final\InstructionOutput"
        self.instruction_dir = os.path.join(self.base_dir, "Instruction")
        self.output_dir = os.path.join(self.base_dir, "Output_process_batches_claude")
        
        # Merged output directory
        self.merged_results_dir = os.path.join(self.base_dir, "merged_results")
        os.makedirs(self.merged_results_dir, exist_ok=True)

    def extract_text_from_result(self, result_entry: Dict) -> str:
        """Extract the translated text from a batch result entry"""
        try:
            # Navigate through the correct structure: response -> body -> choices -> [0] -> message -> content
            if 'response' in result_entry and result_entry['response']:
                response_data = result_entry['response']
                if 'body' in response_data and response_data['body']:
                    body = response_data['body']
                    if 'choices' in body and body['choices'] and len(body['choices']) > 0:
                        choice = body['choices'][0]  # Get first choice
                        if 'message' in choice and choice['message']:
                            message = choice['message']
                            if 'content' in message and message['content']:
                                text_content = message['content']
                                
                                # Try to extract JSON from the text (it's wrapped in ```json code blocks or just plain text)
                                try:
                                    # Remove code block markers if present
                                    json_match = re.search(r'```?json\s*\n(.*?)\n```?', text_content, re.DOTALL)
                                    if json_match:
                                        json_str = json_match.group(1).strip()
                                        parsed_json = json.loads(json_str)
                                        if isinstance(parsed_json, dict) and 'text' in parsed_json:
                                            return parsed_json['text'].strip()
                                    
                                    # If no JSON block found, try to parse the entire text as JSON
                                    # First, clean up the content - remove "json\n" prefix if present
                                    cleaned_content = text_content.strip()
                                    if cleaned_content.startswith('json\n'):
                                        cleaned_content = cleaned_content[5:]  # Remove "json\n"
                                    
                                    parsed_json = json.loads(cleaned_content)
                                    if isinstance(parsed_json, dict) and 'text' in parsed_json:
                                        return parsed_json['text'].strip()
                                        
                                except json.JSONDecodeError:
                                    # If JSON parsing fails, return the raw text
                                    return text_content.strip()
                                
                                return text_content.strip()
            
            # Fallback: try the old structure in case some files use different format
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
        """Extract line number from custom_id"""
        try:
            # Look for the pattern '_line_NUMBER' at the end
            match = re.search(r'_line_(\d+)$', custom_id)
            if match:
                return int(match.group(1))
            
            # Alternative patterns that might be used
            # Pattern: 'alpaca_data_line_NUMBER'
            match = re.search(r'alpaca_data_line_(\d+)$', custom_id)
            if match:
                return int(match.group(1))
                
            # Pattern: just 'line_NUMBER'
            match = re.search(r'line_(\d+)$', custom_id)
            if match:
                return int(match.group(1))
                
        except (ValueError, AttributeError):
            pass
        return None

    def get_available_batch_files(self, directory: str) -> List[str]:
        """Get list of available batch result files"""
        batch_files = []
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                if filename.endswith('.jsonl'):
                    batch_files.append(filename)
        batch_files.sort()
        return batch_files

    def create_single_merged_file(self):
        """Create a single merged file with instruction, empty input, and output"""
        print("🔄 Creating single merged file (instruction + output, empty input)...")
        print("=" * 70)
        
        # Check if directories exist
        if not os.path.exists(self.instruction_dir):
            print(f"❌ Instruction directory not found: {self.instruction_dir}")
            return
        
        if not os.path.exists(self.output_dir):
            print(f"❌ Output directory not found: {self.output_dir}")
            return
        
        # Get available batch files
        instruction_files = self.get_available_batch_files(self.instruction_dir)
        output_files = self.get_available_batch_files(self.output_dir)
        
        print(f"📂 Found {len(instruction_files)} instruction files: {instruction_files}")
        print(f"📂 Found {len(output_files)} output files: {output_files}")
        
        if not instruction_files or not output_files:
            print("❌ No batch files found in one or both directories")
            return
        
        # Collect all results
        all_instruction_results = {}
        all_output_results = {}
        
        # Load all instruction results
        print("\n📖 Loading instruction results...")
        for filename in instruction_files:
            file_path = os.path.join(self.instruction_dir, filename)
            results = self.load_batch_results(file_path)
            print(f"   📄 Processing {filename}: {len(results)} entries")
            
            for result in results:
                custom_id = result.get('custom_id', '')
                line_num = self.extract_line_number_from_custom_id(custom_id)
                if line_num is not None:
                    extracted_text = self.extract_text_from_result(result)
                    all_instruction_results[line_num] = extracted_text
                    if len(all_instruction_results) <= 3:  # Debug first few entries
                        print(f"      Debug - Line {line_num}: '{extracted_text[:50]}...'")
        
        # Load all output results
        print("📤 Loading output results...")
        for filename in output_files:
            file_path = os.path.join(self.output_dir, filename)
            results = self.load_batch_results(file_path)
            print(f"   📄 Processing {filename}: {len(results)} entries")
            
            for result in results:
                custom_id = result.get('custom_id', '')
                line_num = self.extract_line_number_from_custom_id(custom_id)
                if line_num is not None:
                    extracted_text = self.extract_text_from_result(result)
                    all_output_results[line_num] = extracted_text
                    if len(all_output_results) <= 3:  # Debug first few entries
                        print(f"      Debug - Line {line_num}: '{extracted_text[:50]}...'")
        
        print(f"✅ Loaded {len(all_instruction_results)} instruction and {len(all_output_results)} output results")
        
        # Find common line numbers
        common_lines = set(all_instruction_results.keys()) & set(all_output_results.keys())
        
        if not common_lines:
            print("❌ No matching line numbers found between instruction and output results")
            
            # Show some sample line numbers for debugging
            inst_samples = list(all_instruction_results.keys())[:5]
            out_samples = list(all_output_results.keys())[:5]
            print(f"🔍 Sample instruction line numbers: {inst_samples}")
            print(f"🔍 Sample output line numbers: {out_samples}")
            return
        
        print(f"🔗 Found {len(common_lines)} matching entries")
        
        # Create merged results with empty input field
        merged_results = []
        for line_num in sorted(common_lines):
            merged_entry = {
                "instruction": all_instruction_results[line_num],
                "input": "",  # Always empty as requested
                "output": all_output_results[line_num]
            }
            merged_results.append(merged_entry)
        
        # Save single merged file
        output_file_path = os.path.join(self.merged_results_dir, "merged_instruction_output.jsonl")
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for entry in merged_results:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"\n🎉 Success!")
        print(f"💾 Saved {len(merged_results)} merged entries to: {output_file_path}")
        print(f"📊 File format: {{\"instruction\": \"...\", \"input\": \"\", \"output\": \"...\"}}")
        print(f"📝 Note: All 'input' fields are empty as requested")
        
        return output_file_path

    def display_sample_entries(self, num_samples: int = 5):
        """Display sample entries from the merged results"""
        merged_file_path = os.path.join(self.merged_results_dir, "merged_instruction_output.jsonl")
        
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
                        print(f"   Input: '{entry.get('input', 'N/A')}'")  # Show empty input in quotes
                        print(f"   Output: {entry.get('output', 'N/A')}")
                        print("-" * 60)
                    except json.JSONDecodeError:
                        print(f"  ❌ Error parsing entry {i+1}")

    def validate_directories_and_files(self):
        """Check what files are available in both directories"""
        print("🔍 Validating directories and files...")
        print("=" * 50)
        
        print(f"📂 Instruction directory: {self.instruction_dir}")
        if os.path.exists(self.instruction_dir):
            inst_files = [f for f in os.listdir(self.instruction_dir) if f.endswith('.jsonl')]
            print(f"   ✅ Directory exists")
            print(f"   📄 Files found: {len(inst_files)}")
            for f in inst_files[:5]:  # Show first 5 files
                print(f"      - {f}")
            if len(inst_files) > 5:
                print(f"      ... and {len(inst_files) - 5} more files")
        else:
            print(f"   ❌ Directory does not exist")
        
        print(f"\n📂 Output directory: {self.output_dir}")
        if os.path.exists(self.output_dir):
            out_files = [f for f in os.listdir(self.output_dir) if f.endswith('.jsonl')]
            print(f"   ✅ Directory exists")
            print(f"   📄 Files found: {len(out_files)}")
            for f in out_files[:5]:  # Show first 5 files
                print(f"      - {f}")
            if len(out_files) > 5:
                print(f"      ... and {len(out_files) - 5} more files")
        else:
            print(f"   ❌ Directory does not exist")

    def test_text_extraction(self):
        """Test text extraction on sample files"""
        print("🔍 Testing text extraction...")
        print("=" * 50)
        
        # Test instruction files
        inst_files = self.get_available_batch_files(self.instruction_dir)
        if inst_files:
            print(f"\n📖 Testing instruction file: {inst_files[0]}")
            file_path = os.path.join(self.instruction_dir, inst_files[0])
            results = self.load_batch_results(file_path)
            
            for i, result in enumerate(results[:3]):  # First 3 entries
                custom_id = result.get('custom_id', 'N/A')
                extracted_text = self.extract_text_from_result(result)
                line_num = self.extract_line_number_from_custom_id(custom_id)
                
                print(f"  Entry {i+1}:")
                print(f"    Custom ID: {custom_id}")
                print(f"    Line Number: {line_num}")
                print(f"    Raw Content Structure: {self._get_content_structure(result)}")
                print(f"    Extracted Text: '{extracted_text}'")
                print()
        
        # Test output files
        out_files = self.get_available_batch_files(self.output_dir)
        if out_files:
            print(f"📤 Testing output file: {out_files[0]}")
            file_path = os.path.join(self.output_dir, out_files[0])
            results = self.load_batch_results(file_path)
            
            for i, result in enumerate(results[:3]):  # First 3 entries
                custom_id = result.get('custom_id', 'N/A')
                extracted_text = self.extract_text_from_result(result)
                line_num = self.extract_line_number_from_custom_id(custom_id)
                
                print(f"  Entry {i+1}:")
                print(f"    Custom ID: {custom_id}")
                print(f"    Line Number: {line_num}")
                print(f"    Raw Content Structure: {self._get_content_structure(result)}")
                print(f"    Extracted Text: '{extracted_text}'")
                print()

    def _get_content_structure(self, result_entry: Dict) -> str:
        """Helper method to show the structure of content for debugging"""
        try:
            if 'response' in result_entry and 'body' in result_entry['response']:
                body = result_entry['response']['body']
                if 'choices' in body and body['choices']:
                    content = body['choices'][0]['message']['content']
                    return f"Content: {content[:100]}..." if len(content) > 100 else f"Content: {content}"
            elif 'result' in result_entry:
                return "Old structure format detected"
            return "Unknown structure"
        except:
            return "Error accessing structure"


def main():
    """Main function to run the instruction-output merger"""
    print("🔄 Instruction-Output Merger (No Input)")
    print("   Creates merged file with empty input fields")
    print("=" * 60)
    
    merger = InstructionOutputMerger()
    
    while True:
        print("\n" + "=" * 60)
        print("OPTIONS:")
        print("1. Create merged file (instruction + output, empty input)")
        print("2. Display sample merged entries")
        print("3. Validate directories and files")
        print("4. Test text extraction (debugging)")
        print("5. Exit")
        print("=" * 60)
        
        choice = input("Enter your choice (1-5): ").strip()
        
        if choice == '1':
            merger.create_single_merged_file()
            
        elif choice == '2':
            try:
                num_samples = int(input("How many sample entries to display (default 5): ").strip() or "5")
                merger.display_sample_entries(num_samples)
            except ValueError:
                merger.display_sample_entries(5)
                
        elif choice == '3':
            merger.validate_directories_and_files()
            
        elif choice == '4':
            merger.test_text_extraction()
                
        elif choice == '5':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    main()