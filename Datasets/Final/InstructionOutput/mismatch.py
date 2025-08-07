import os
import json
import re
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict

class FileEntriesMismatchFinder:
    def __init__(self):
        """Initialize the mismatch finder"""
        # Base directory for this merger
        self.base_dir = r"E:\MCS_Project\Final\InstructionOutput"
        self.instruction_dir = os.path.join(self.base_dir, "Instruction")
        self.output_dir = os.path.join(self.base_dir, "Output_process_batches_claude")
        
        # Report output directory
        self.reports_dir = os.path.join(self.base_dir, "mismatch_reports")
        os.makedirs(self.reports_dir, exist_ok=True)

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
                                
                                # Try to extract JSON from the text
                                try:
                                    # Remove code block markers if present
                                    json_match = re.search(r'```?json\s*\n(.*?)\n```?', text_content, re.DOTALL)
                                    if json_match:
                                        json_str = json_match.group(1).strip()
                                        parsed_json = json.loads(json_str)
                                        if isinstance(parsed_json, dict) and 'text' in parsed_json:
                                            return parsed_json['text'].strip()
                                    
                                    # If no JSON block found, try to parse the entire text as JSON
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
            
            return ""
            
        except Exception as e:
            return f"ERROR_EXTRACTING: {str(e)}"

    def load_batch_results(self, file_path: str) -> List[Dict]:
        """Load results from a batch results file"""
        results = []
        if not os.path.exists(file_path):
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
            # Look for various patterns
            patterns = [
                r'_line_(\d+)$',
                r'alpaca_data_line_(\d+)$',
                r'line_(\d+)$',
                r'data_line_(\d+)$'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, custom_id)
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

    def analyze_file_entries(self, file_path: str, file_type: str) -> Dict:
        """Analyze entries in a single file"""
        results = self.load_batch_results(file_path)
        
        analysis = {
            'file_path': file_path,
            'file_type': file_type,
            'total_entries': len(results),
            'line_numbers': set(),
            'custom_ids': [],
            'successful_extractions': 0,
            'failed_extractions': 0,
            'empty_extractions': 0,
            'error_entries': [],
            'duplicate_line_numbers': [],
            'entries_by_line': {}
        }
        
        line_number_counts = defaultdict(int)
        
        for i, result in enumerate(results):
            custom_id = result.get('custom_id', f'unknown_{i}')
            line_num = self.extract_line_number_from_custom_id(custom_id)
            extracted_text = self.extract_text_from_result(result)
            
            analysis['custom_ids'].append(custom_id)
            
            if line_num is not None:
                analysis['line_numbers'].add(line_num)
                line_number_counts[line_num] += 1
                analysis['entries_by_line'][line_num] = {
                    'custom_id': custom_id,
                    'extracted_text': extracted_text,
                    'text_length': len(extracted_text),
                    'entry_index': i
                }
            
            if extracted_text.startswith('ERROR_EXTRACTING'):
                analysis['failed_extractions'] += 1
                analysis['error_entries'].append({
                    'custom_id': custom_id,
                    'line_num': line_num,
                    'error': extracted_text
                })
            elif not extracted_text or extracted_text.strip() == '':
                analysis['empty_extractions'] += 1
            else:
                analysis['successful_extractions'] += 1
        
        # Find duplicates
        for line_num, count in line_number_counts.items():
            if count > 1:
                analysis['duplicate_line_numbers'].append((line_num, count))
        
        return analysis

    def find_mismatches(self) -> Dict:
        """Find mismatches between instruction and output files"""
        print("🔍 Analyzing file entries for mismatches...")
        print("=" * 60)
        
        # Get all files
        instruction_files = self.get_available_batch_files(self.instruction_dir)
        output_files = self.get_available_batch_files(self.output_dir)
        
        print(f"📂 Instruction files: {len(instruction_files)}")
        print(f"📂 Output files: {len(output_files)}")
        
        # Analyze all instruction files
        instruction_analysis = []
        all_instruction_lines = set()
        
        print("\n📖 Analyzing instruction files...")
        for filename in instruction_files:
            file_path = os.path.join(self.instruction_dir, filename)
            analysis = self.analyze_file_entries(file_path, 'instruction')
            instruction_analysis.append(analysis)
            all_instruction_lines.update(analysis['line_numbers'])
            print(f"   📄 {filename}: {analysis['total_entries']} entries, {len(analysis['line_numbers'])} line numbers")
        
        # Analyze all output files
        output_analysis = []
        all_output_lines = set()
        
        print("📤 Analyzing output files...")
        for filename in output_files:
            file_path = os.path.join(self.output_dir, filename)
            analysis = self.analyze_file_entries(file_path, 'output')
            output_analysis.append(analysis)
            all_output_lines.update(analysis['line_numbers'])
            print(f"   📄 {filename}: {analysis['total_entries']} entries, {len(analysis['line_numbers'])} line numbers")
        
        # Find mismatches
        missing_in_output = all_instruction_lines - all_output_lines
        missing_in_instruction = all_output_lines - all_instruction_lines
        common_lines = all_instruction_lines & all_output_lines
        
        mismatch_report = {
            'instruction_files': instruction_analysis,
            'output_files': output_analysis,
            'total_instruction_lines': len(all_instruction_lines),
            'total_output_lines': len(all_output_lines),
            'common_lines': len(common_lines),
            'missing_in_output': sorted(missing_in_output),
            'missing_in_instruction': sorted(missing_in_instruction),
            'mismatch_summary': {
                'total_mismatches': len(missing_in_output) + len(missing_in_instruction),
                'lines_only_in_instruction': len(missing_in_output),
                'lines_only_in_output': len(missing_in_instruction),
                'matching_lines': len(common_lines)
            }
        }
        
        return mismatch_report

    def generate_detailed_report(self, mismatch_report: Dict):
        """Generate a detailed mismatch report"""
        print("\n" + "=" * 80)
        print("📊 DETAILED MISMATCH ANALYSIS REPORT")
        print("=" * 80)
        
        # Summary
        summary = mismatch_report['mismatch_summary']
        print(f"\n🎯 SUMMARY:")
        print(f"   📈 Total instruction line numbers: {mismatch_report['total_instruction_lines']}")
        print(f"   📈 Total output line numbers: {mismatch_report['total_output_lines']}")
        print(f"   ✅ Matching line numbers: {summary['matching_lines']}")
        print(f"   ❌ Total mismatches: {summary['total_mismatches']}")
        print(f"   📝 Lines only in instructions: {summary['lines_only_in_instruction']}")
        print(f"   📤 Lines only in outputs: {summary['lines_only_in_output']}")
        
        # File-by-file analysis
        print(f"\n📖 INSTRUCTION FILES ANALYSIS:")
        for analysis in mismatch_report['instruction_files']:
            filename = os.path.basename(analysis['file_path'])
            print(f"   📄 {filename}:")
            print(f"      • Total entries: {analysis['total_entries']}")
            print(f"      • Successful extractions: {analysis['successful_extractions']}")
            print(f"      • Failed extractions: {analysis['failed_extractions']}")
            print(f"      • Empty extractions: {analysis['empty_extractions']}")
            print(f"      • Unique line numbers: {len(analysis['line_numbers'])}")
            if analysis['duplicate_line_numbers']:
                print(f"      • Duplicates: {analysis['duplicate_line_numbers']}")
        
        print(f"\n📤 OUTPUT FILES ANALYSIS:")
        for analysis in mismatch_report['output_files']:
            filename = os.path.basename(analysis['file_path'])
            print(f"   📄 {filename}:")
            print(f"      • Total entries: {analysis['total_entries']}")
            print(f"      • Successful extractions: {analysis['successful_extractions']}")
            print(f"      • Failed extractions: {analysis['failed_extractions']}")
            print(f"      • Empty extractions: {analysis['empty_extractions']}")
            print(f"      • Unique line numbers: {len(analysis['line_numbers'])}")
            if analysis['duplicate_line_numbers']:
                print(f"      • Duplicates: {analysis['duplicate_line_numbers']}")
        
        # Missing entries
        if mismatch_report['missing_in_output']:
            print(f"\n❌ LINE NUMBERS MISSING IN OUTPUT FILES:")
            missing_output = mismatch_report['missing_in_output']
            if len(missing_output) <= 20:
                print(f"   {missing_output}")
            else:
                print(f"   First 20: {missing_output[:20]}")
                print(f"   ... and {len(missing_output) - 20} more")
        
        if mismatch_report['missing_in_instruction']:
            print(f"\n❌ LINE NUMBERS MISSING IN INSTRUCTION FILES:")
            missing_instruction = mismatch_report['missing_in_instruction']
            if len(missing_instruction) <= 20:
                print(f"   {missing_instruction}")
            else:
                print(f"   First 20: {missing_instruction[:20]}")
                print(f"   ... and {len(missing_instruction) - 20} more")
        
        # Error details
        print(f"\n🚨 ERROR DETAILS:")
        for analysis in mismatch_report['instruction_files'] + mismatch_report['output_files']:
            if analysis['error_entries']:
                filename = os.path.basename(analysis['file_path'])
                print(f"   📄 {filename} ({analysis['file_type']}):")
                for error in analysis['error_entries'][:5]:  # Show first 5 errors
                    print(f"      • Line {error['line_num']}: {error['error']}")
                if len(analysis['error_entries']) > 5:
                    print(f"      ... and {len(analysis['error_entries']) - 5} more errors")

    def save_mismatch_report(self, mismatch_report: Dict):
        """Save the mismatch report to a JSON file"""
        report_file = os.path.join(self.reports_dir, "mismatch_report.json")
        
        # Convert sets to lists for JSON serialization
        json_report = json.loads(json.dumps(mismatch_report, default=list))
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(json_report, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Detailed report saved to: {report_file}")
        return report_file

    def find_gaps_and_ranges(self, line_numbers: Set[int]) -> Dict:
        """Find gaps and ranges in line numbers"""
        if not line_numbers:
            return {'ranges': [], 'gaps': [], 'min': None, 'max': None}
        
        sorted_lines = sorted(line_numbers)
        min_line = sorted_lines[0]
        max_line = sorted_lines[-1]
        
        # Find ranges
        ranges = []
        gaps = []
        
        start = sorted_lines[0]
        prev = sorted_lines[0]
        
        for current in sorted_lines[1:]:
            if current != prev + 1:
                # End of range
                ranges.append((start, prev))
                # Gap found
                if current - prev > 1:
                    gaps.append((prev + 1, current - 1))
                start = current
            prev = current
        
        # Add final range
        ranges.append((start, prev))
        
        return {
            'ranges': ranges,
            'gaps': gaps,
            'min': min_line,
            'max': max_line,
            'total_expected': max_line - min_line + 1,
            'total_actual': len(line_numbers),
            'missing_count': (max_line - min_line + 1) - len(line_numbers)
        }

    def analyze_line_number_patterns(self, mismatch_report: Dict):
        """Analyze line number patterns and gaps"""
        print(f"\n🔢 LINE NUMBER PATTERN ANALYSIS:")
        print("=" * 50)
        
        # Collect all line numbers from instruction files
        all_instruction_lines = set()
        for analysis in mismatch_report['instruction_files']:
            all_instruction_lines.update(analysis['line_numbers'])
        
        # Collect all line numbers from output files
        all_output_lines = set()
        for analysis in mismatch_report['output_files']:
            all_output_lines.update(analysis['line_numbers'])
        
        # Analyze instruction patterns
        inst_patterns = self.find_gaps_and_ranges(all_instruction_lines)
        print(f"\n📖 INSTRUCTION LINE PATTERNS:")
        print(f"   Range: {inst_patterns['min']} to {inst_patterns['max']}")
        print(f"   Expected total: {inst_patterns['total_expected']}")
        print(f"   Actual total: {inst_patterns['total_actual']}")
        print(f"   Missing: {inst_patterns['missing_count']}")
        
        if inst_patterns['gaps']:
            print(f"   Gaps found: {len(inst_patterns['gaps'])}")
            for gap_start, gap_end in inst_patterns['gaps'][:10]:  # Show first 10 gaps
                if gap_start == gap_end:
                    print(f"      • Missing line: {gap_start}")
                else:
                    print(f"      • Missing range: {gap_start}-{gap_end} ({gap_end - gap_start + 1} lines)")
            if len(inst_patterns['gaps']) > 10:
                print(f"      ... and {len(inst_patterns['gaps']) - 10} more gaps")
        
        # Analyze output patterns
        out_patterns = self.find_gaps_and_ranges(all_output_lines)
        print(f"\n📤 OUTPUT LINE PATTERNS:")
        print(f"   Range: {out_patterns['min']} to {out_patterns['max']}")
        print(f"   Expected total: {out_patterns['total_expected']}")
        print(f"   Actual total: {out_patterns['total_actual']}")
        print(f"   Missing: {out_patterns['missing_count']}")
        
        if out_patterns['gaps']:
            print(f"   Gaps found: {len(out_patterns['gaps'])}")
            for gap_start, gap_end in out_patterns['gaps'][:10]:  # Show first 10 gaps
                if gap_start == gap_end:
                    print(f"      • Missing line: {gap_start}")
                else:
                    print(f"      • Missing range: {gap_start}-{gap_end} ({gap_end - gap_start + 1} lines)")
            if len(out_patterns['gaps']) > 10:
                print(f"      ... and {len(out_patterns['gaps']) - 10} more gaps")

    def run_complete_analysis(self):
        """Run complete mismatch analysis"""
        print("🚀 Starting complete mismatch analysis...")
        
        # Find mismatches
        mismatch_report = self.find_mismatches()
        
        # Generate detailed console report
        self.generate_detailed_report(mismatch_report)
        
        # Analyze line number patterns
        self.analyze_line_number_patterns(mismatch_report)
        
        # Save report to file
        report_file = self.save_mismatch_report(mismatch_report)
        
        print(f"\n🎉 Analysis complete!")
        print(f"📊 Check the detailed report at: {report_file}")
        
        return mismatch_report


def main():
    """Main function to run the mismatch finder"""
    print("🔍 File Entries Mismatch Finder")
    print("   Analyzes mismatches between instruction and output files")
    print("=" * 70)
    
    finder = FileEntriesMismatchFinder()
    
    while True:
        print("\n" + "=" * 60)
        print("OPTIONS:")
        print("1. Run complete mismatch analysis")
        print("2. Analyze instruction files only")
        print("3. Analyze output files only")
        print("4. Quick summary only")
        print("5. Exit")
        print("=" * 60)
        
        choice = input("Enter your choice (1-5): ").strip()
        
        if choice == '1':
            finder.run_complete_analysis()
            
        elif choice == '2':
            print("📖 Analyzing instruction files...")
            instruction_files = finder.get_available_batch_files(finder.instruction_dir)
            for filename in instruction_files:
                file_path = os.path.join(finder.instruction_dir, filename)
                analysis = finder.analyze_file_entries(file_path, 'instruction')
                print(f"\n📄 {filename}:")
                print(f"   Total entries: {analysis['total_entries']}")
                print(f"   Successful: {analysis['successful_extractions']}")
                print(f"   Failed: {analysis['failed_extractions']}")
                print(f"   Empty: {analysis['empty_extractions']}")
                print(f"   Line numbers: {len(analysis['line_numbers'])}")
                
        elif choice == '3':
            print("📤 Analyzing output files...")
            output_files = finder.get_available_batch_files(finder.output_dir)
            for filename in output_files:
                file_path = os.path.join(finder.output_dir, filename)
                analysis = finder.analyze_file_entries(file_path, 'output')
                print(f"\n📄 {filename}:")
                print(f"   Total entries: {analysis['total_entries']}")
                print(f"   Successful: {analysis['successful_extractions']}")
                print(f"   Failed: {analysis['failed_extractions']}")
                print(f"   Empty: {analysis['empty_extractions']}")
                print(f"   Line numbers: {len(analysis['line_numbers'])}")
                
        elif choice == '4':
            mismatch_report = finder.find_mismatches()
            summary = mismatch_report['mismatch_summary']
            print(f"\n🎯 QUICK SUMMARY:")
            print(f"   Instruction lines: {mismatch_report['total_instruction_lines']}")
            print(f"   Output lines: {mismatch_report['total_output_lines']}")
            print(f"   Matching: {summary['matching_lines']}")
            print(f"   Mismatches: {summary['total_mismatches']}")
                
        elif choice == '5':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    main()