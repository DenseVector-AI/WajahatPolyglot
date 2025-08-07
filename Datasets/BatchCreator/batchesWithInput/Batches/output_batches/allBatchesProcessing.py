import os
import json
import time
import anthropic
from datetime import datetime
from typing import List, Dict, Optional, Any

class MultiFolderClaudeBatchProcessor:
    def __init__(self, api_key: str = None):
        """Initialize the Claude batch processor for multiple folder types"""
        # It's generally better to rely solely on ANTHROPIC_API_KEY environment variable
        # and avoid hardcoding a default key directly in the code for security.
        # If no API key is provided, the client will automatically look for the env var.
        self.client = anthropic.Anthropic(
            api_key=api_key or os.environ.get("ANTHROPIC_API_KEY")
        )
        
        if not self.client.api_key:
            raise ValueError(
                "Anthropic API key not found. "
                "Please provide it via api_key parameter or set the ANTHROPIC_API_KEY environment variable."
            )

        # Updated paths for the multi-folder structure
        self.base_batch_dir = r"E:\MCS_Project\BatchCreator\batchesWithInput\Batches"
        self.instruction_dir = os.path.join(self.base_batch_dir, "instruction_batches")
        self.input_dir = os.path.join(self.base_batch_dir, "input_batches")
        self.output_dir = os.path.join(self.base_batch_dir, "output_batches")
        
        # Results directories for each type
        self.base_results_dir = r"E:\MCS_Project\BatchCreator\batchesWithInput\Results"
        self.instruction_results_dir = os.path.join(self.base_results_dir, "instruction_results")
        self.input_results_dir = os.path.join(self.base_results_dir, "input_results")
        self.output_results_dir = os.path.join(self.base_results_dir, "output_results")
        
        # Status file
        self.status_file = os.path.join(self.base_results_dir, "batch_status.json")
        
        # Create directories if they don't exist
        for dir_path in [self.instruction_results_dir, self.input_results_dir, self.output_results_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # Load or initialize status tracking
        self.batch_status = self.load_status()

    def get_batch_directories(self) -> Dict[str, str]:
        """Get dictionary of batch type to directory mapping"""
        return {
            'instruction': self.instruction_dir,
            'input': self.input_dir,
            'output': self.output_dir
        }

    def get_results_directories(self) -> Dict[str, str]:
        """Get dictionary of batch type to results directory mapping"""
        return {
            'instruction': self.instruction_results_dir,
            'input': self.input_results_dir,
            'output': self.output_results_dir
        }

    def load_status(self) -> Dict:
        """Load batch processing status from file"""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                print(f"❌ Error loading batch status file {self.status_file}: {e}. Initializing new status.")
                return {}
        return {}

    def save_status(self):
        """Save batch processing status to file"""
        try:
            os.makedirs(os.path.dirname(self.status_file), exist_ok=True)
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(self.batch_status, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"❌ Error saving batch status file {self.status_file}: {e}")

    def load_batch_file(self, file_path: str) -> List[Dict]:
        """Load requests from a batch JSONL file"""
        requests = []
        if not os.path.exists(file_path):
            print(f"❌ Batch file not found: {file_path}")
            return []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        request = json.loads(line)
                        requests.append(request)
                    except json.JSONDecodeError as e:
                        print(f"❌ Error parsing line {line_num} in {file_path}: {e}")
                        continue
        except Exception as e:
            print(f"❌ An unexpected error occurred while reading {file_path}: {e}")
            return []
        
        print(f"📁 Loaded {len(requests)} requests from {os.path.basename(file_path)}")
        return requests

    def get_batch_key(self, batch_type: str, batch_file: str) -> str:
        """Generate a unique key for batch tracking"""
        return f"{batch_type}_{batch_file}"

    def submit_batch(self, batch_type: str, batch_file: str) -> Optional[str]:
        """Submit a single batch file for processing"""
        batch_dirs = self.get_batch_directories()
        file_path = os.path.join(batch_dirs[batch_type], batch_file)
        batch_key = self.get_batch_key(batch_type, batch_file)
        
        requests = self.load_batch_file(file_path)
        
        if not requests:
            print(f"⚠️  No valid requests found in {batch_type}/{batch_file}")
            return None

        # Check if batch is already submitted and active
        if (batch_key in self.batch_status and 
            self.batch_status[batch_key].get('batch_id') and
            self.batch_status[batch_key].get('status') not in ['failed', 'expired', 'canceled']):
            print(f"⏭️  Batch {batch_type}/{batch_file} already submitted and active (ID: {self.batch_status[batch_key]['batch_id']}). Skipping submission.")
            return self.batch_status[batch_key]['batch_id']

        try:
            print(f"🚀 Submitting batch: {batch_type}/{batch_file}")
            
            # Submit the batch
            message_batch = self.client.messages.batches.create(requests=requests)
            
            batch_id = message_batch.id
            print(f"✅ Batch submitted successfully!")
            print(f"   Batch ID: {batch_id}")
            print(f"   Status: {message_batch.processing_status}")
            request_counts_data = message_batch.request_counts.model_dump() if hasattr(message_batch.request_counts, 'model_dump') else str(message_batch.request_counts)
            print(f"   Request Count: {request_counts_data.get('total', 'N/A')}")
            
            # Update status tracking
            self.batch_status[batch_key] = {
                'batch_id': batch_id,
                'batch_type': batch_type,
                'batch_file': batch_file,
                'status': message_batch.processing_status,
                'submitted_at': datetime.now().isoformat(),
                'request_count': len(requests),
                'request_counts': request_counts_data,
                'results_downloaded': False
            }
            self.save_status()
            
            return batch_id
            
        except anthropic.APIError as e:
            print(f"❌ Anthropic API Error submitting batch {batch_type}/{batch_file}: {e}")
            error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
            self.batch_status[batch_key] = {
                'batch_type': batch_type,
                'batch_file': batch_file,
                'status': 'failed',
                'error': error_details,
                'submitted_at': datetime.now().isoformat()
            }
            self.save_status()
            return None
        except Exception as e:
            print(f"❌ General error submitting batch {batch_type}/{batch_file}: {e}")
            self.batch_status[batch_key] = {
                'batch_type': batch_type,
                'batch_file': batch_file,
                'status': 'failed',
                'error': str(e),
                'submitted_at': datetime.now().isoformat()
            }
            self.save_status()
            return None

    def check_batch_status(self, batch_id: str) -> Dict:
        """Check the status of a specific batch"""
        try:
            batch = self.client.messages.batches.retrieve(batch_id)
            request_counts_data = batch.request_counts.model_dump() if hasattr(batch.request_counts, 'model_dump') else str(batch.request_counts)
            return {
                'id': batch.id,
                'status': batch.processing_status,
                'request_counts': request_counts_data,
                'created_at': batch.created_at.isoformat() if batch.created_at else None,
                'expires_at': batch.expires_at.isoformat() if batch.expires_at else None
            }
        except anthropic.APIError as e:
            print(f"❌ Anthropic API Error checking batch {batch_id}: {e}")
            return {'status': 'error', 'error': e.response.json() if hasattr(e.response, 'json') else str(e)}
        except Exception as e:
            print(f"❌ General error checking batch {batch_id}: {e}")
            return {'status': 'error', 'error': str(e)}

    def check_all_batches(self, auto_download: bool = True):
        """Check status of all submitted batches and auto-download completed ones"""
        print("\n📊 Checking status of all batches...")
        print("=" * 80)
        
        newly_completed = []
        
        for batch_key in list(self.batch_status.keys()):
            info = self.batch_status[batch_key]
            batch_type = info.get('batch_type', 'unknown')
            batch_file = info.get('batch_file', 'unknown')
            
            if 'batch_id' in info and info.get('status') not in ['ended', 'expired', 'canceled', 'failed']:
                batch_id = info['batch_id']
                current_status = self.check_batch_status(batch_id)
                
                print(f"\n📁 {batch_type}/{batch_file}")
                print(f"   Batch ID: {batch_id}")
                print(f"   Status: {current_status.get('status', 'unknown')}")
                print(f"   Request Counts: {current_status.get('request_counts', 'N/A')}")
                
                # Update status if changed
                old_status = info.get('status')
                new_status = current_status.get('status')
                
                if new_status and new_status != old_status:
                    self.batch_status[batch_key]['status'] = new_status
                    self.batch_status[batch_key]['updated_at'] = datetime.now().isoformat()
                    self.batch_status[batch_key]['request_counts'] = current_status.get('request_counts', 'N/A')
                    
                    if new_status == 'ended' and old_status != 'ended':
                        newly_completed.append((batch_key, batch_id))
                        print(f"   🎉 COMPLETED! Ready for download.")
                elif new_status == 'ended' and not info.get('results_downloaded', False):
                    newly_completed.append((batch_key, batch_id))
                    print(f"   🎉 COMPLETED (Previously)! Ready for download.")

            elif info.get('status') == 'ended' and info.get('results_downloaded', False):
                print(f"\n📁 {batch_type}/{batch_file}")
                print(f"   Batch ID: {info.get('batch_id', 'N/A')}")
                print(f"   Status: {info.get('status', 'ended')}")
                print(f"   Request Counts: {info.get('request_counts', 'N/A')}")
                print(f"   💾 Results already downloaded to: {info.get('results_file', 'N/A')}")
            else:
                print(f"\n📁 {batch_type}/{batch_file}")
                print(f"   Status: {info.get('status', 'Not Submitted/Unknown')}")
                if 'error' in info:
                    print(f"   Error: {info['error']}")
        
        self.save_status()
        
        # Auto-download newly completed batches
        if auto_download and newly_completed:
            print(f"\n🔄 Auto-downloading {len(newly_completed)} newly completed batch(es)...")
            for batch_key, batch_id in newly_completed:
                if not self.batch_status[batch_key].get('results_downloaded', False):
                    print(f"\n📥 Auto-downloading: {batch_key}")
                    self.download_batch_results(batch_id, batch_key)

    def download_batch_results(self, batch_id: str, batch_key: str):
        """Download results for a completed batch"""
        try:
            batch = self.client.messages.batches.retrieve(batch_id)
            
            if batch.processing_status != 'ended':
                print(f"⚠️  Batch {batch_id} not ready (status: {batch.processing_status})")
                return False
            
            info = self.batch_status[batch_key]
            batch_type = info.get('batch_type', 'unknown')
            batch_file = info.get('batch_file', 'unknown')
            
            # Get results
            results_iterator = self.client.messages.batches.results(batch_id)
            
            # Determine results directory and file path
            results_dirs = self.get_results_directories()
            results_dir = results_dirs.get(batch_type, self.base_results_dir)
            results_file_name = f"results_{batch_file}"
            results_file_path = os.path.join(results_dir, results_file_name)

            has_results = False
            with open(results_file_path, 'w', encoding='utf-8') as f:
                for result in results_iterator:
                    has_results = True
                    try:
                        result_dict = result.model_dump(mode='json')
                    except AttributeError:
                        print(f"⚠️  Warning: result.model_dump() not found for a result object. Attempting manual serialization for {result.id}")
                        result_dict = {
                            'id': getattr(result, 'id', None),
                            'custom_id': getattr(result, 'custom_id', None),
                            'response': None,
                            'error': None
                        }
                        if hasattr(result, 'response') and result.response:
                            response = result.response
                            response_dict: Dict[str, Any] = {
                                'status_code': getattr(response, 'status_code', None),
                                'request_id': getattr(response, 'request_id', None),
                                'body': None
                            }
                            if hasattr(response, 'body') and response.body:
                                if hasattr(response.body, 'model_dump'):
                                    response_dict['body'] = response.body.model_dump(mode='json')
                                else:
                                    response_dict['body'] = str(response.body)
                            result_dict['response'] = response_dict
                        if hasattr(result, 'error') and result.error:
                            if hasattr(result.error, 'model_dump'):
                                result_dict['error'] = result.error.model_dump(mode='json')
                            else:
                                result_dict['error'] = str(result.error)

                    f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
            
            if has_results:
                print(f"💾 Results saved to: {results_file_path}")
                
                # Update status
                self.batch_status[batch_key]['results_downloaded'] = True
                self.batch_status[batch_key]['results_file'] = results_file_path
                self.batch_status[batch_key]['downloaded_at'] = datetime.now().isoformat()
                self.save_status()
                return True
            else:
                print(f"⚠️ No results found for batch {batch_id}. File not created.")
                return False
            
        except anthropic.APIError as e:
            print(f"❌ Anthropic API Error downloading results for {batch_id}: {e}")
            self.batch_status[batch_key]['download_error'] = e.response.json() if hasattr(e.response, 'json') else str(e)
            self.save_status()
            return False
        except Exception as e:
            print(f"❌ General error downloading results for {batch_id}: {e}")
            self.batch_status[batch_key]['download_error'] = str(e)
            self.save_status()
            return False

    def get_all_batch_files(self, limit: int = 5) -> Dict[str, List[str]]:
        """Get batch files from all directories, limited to first N batches"""
        batch_dirs = self.get_batch_directories()
        all_batches = {}
        
        def extract_batch_number(filename):
            try:
                return int(filename.replace('batch_', '').replace('.jsonl', ''))
            except ValueError:
                return float('inf')
        
        for batch_type, batch_dir in batch_dirs.items():
            if os.path.exists(batch_dir):
                batch_files = [f for f in os.listdir(batch_dir) if f.endswith('.jsonl')]
                batch_files.sort(key=extract_batch_number)
                all_batches[batch_type] = batch_files[:limit]
                print(f"📂 Found {len(all_batches[batch_type])} {batch_type} batch files (showing first {limit})")
            else:
                print(f"⚠️  Directory not found: {batch_dir}")
                all_batches[batch_type] = []
        
        return all_batches

    def submit_first_5_batches_all_types(self, delay_between_batches: int = 2, auto_check_interval: int = 30):
        """Submit first 5 batches from all three types (instruction, input, output)"""
        all_batches = self.get_all_batch_files(limit=5)
        
        if not any(all_batches.values()):
            print("❌ No batch files found in any directory.")
            return
        
        # Submit all batches
        submitted_batches_to_monitor = []
        total_submitted = 0
        
        for batch_type, batch_files in all_batches.items():
            if not batch_files:
                print(f"⚠️  No {batch_type} batch files found")
                continue
                
            print(f"\n🔄 Processing {batch_type} batches...")
            
            for i, batch_file in enumerate(batch_files, 1):
                print(f"\n--- Processing {batch_type} batch {i}/{len(batch_files)}: {batch_file} ---")
                
                batch_id = self.submit_batch(batch_type, batch_file)
                if batch_id:
                    batch_key = self.get_batch_key(batch_type, batch_file)
                    submitted_batches_to_monitor.append(batch_key)
                    total_submitted += 1
                    
                if i < len(batch_files) and batch_id:
                    print(f"⏳ Waiting {delay_between_batches} seconds before next batch...")
                    time.sleep(delay_between_batches)
        
        print(f"\n✅ Submission complete! {total_submitted} batches submitted/tracked for monitoring.")
        
        # Start automatic monitoring
        if submitted_batches_to_monitor:
            print(f"\n🔄 Starting automatic monitoring (checking every {auto_check_interval} seconds)")
            print("   Will automatically download completed batches!")
            print("   Press Ctrl+C to stop monitoring and return to menu\n")
            
            try:
                self.monitor_and_auto_download(submitted_batches_to_monitor, auto_check_interval)
            except KeyboardInterrupt:
                print("\n⏹️  Monitoring stopped by user. Returning to menu...")
                print("   You can resume monitoring by choosing option 2 (Check batch status)")
        else:
            print("\n⚠️ No new batches were submitted to monitor.")

    def monitor_and_auto_download(self, batch_keys_to_monitor: List[str], check_interval: int = 30):
        """Continuously monitor batches and auto-download when complete"""
        
        while True:
            completed_downloaded_count = sum(1 for bk in batch_keys_to_monitor 
                                             if self.batch_status.get(bk, {}).get('results_downloaded', False))
            
            total_to_monitor = len(batch_keys_to_monitor)
            
            print(f"\n🔍 Checking status... ({completed_downloaded_count}/{total_to_monitor} completed and downloaded)")
            
            # Check status with auto-download
            self.check_all_batches(auto_download=True)
            
            # Recount completed batches after checking
            completed_downloaded_count = sum(1 for bk in batch_keys_to_monitor 
                                             if self.batch_status.get(bk, {}).get('results_downloaded', False))
            
            if completed_downloaded_count >= total_to_monitor:
                print(f"\n🎉 ALL MONITORED BATCHES COMPLETED AND DOWNLOADED! ({completed_downloaded_count}/{total_to_monitor})")
                print("   All results have been automatically downloaded to their respective results directories")
                break
                
            print(f"⏳ Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)

    def download_all_completed_results(self):
        """Download results for all completed batches that haven't been downloaded yet"""
        print("\n💾 Downloading results for completed batches...")
        
        for batch_key in list(self.batch_status.keys()):
            info = self.batch_status[batch_key]
            if (info.get('status') == 'ended' and 
                'batch_id' in info and 
                not info.get('results_downloaded', False)):
                
                batch_type = info.get('batch_type', 'unknown')
                batch_file = info.get('batch_file', 'unknown')
                print(f"\n📁 Downloading results for {batch_type}/{batch_file}...")
                self.download_batch_results(info['batch_id'], batch_key)
            elif info.get('status') == 'ended' and info.get('results_downloaded', False):
                batch_type = info.get('batch_type', 'unknown')
                batch_file = info.get('batch_file', 'unknown')
                print(f"\n✔️ Results for {batch_type}/{batch_file} already downloaded.")

    def print_summary(self):
        """Print a summary of all batch operations"""
        if not self.batch_status:
            print("📊 No batches have been processed yet.")
            return
            
        print("\n" + "=" * 100)
        print("📊 MULTI-FOLDER BATCH PROCESSING SUMMARY")
        print("=" * 100)
        
        # Group by batch type
        type_summaries = {'instruction': [], 'input': [], 'output': []}
        total_requests = 0
        total_downloaded = 0
        
        for batch_key, info in self.batch_status.items():
            batch_type = info.get('batch_type', 'unknown')
            if batch_type in type_summaries:
                type_summaries[batch_type].append(info)
            total_requests += info.get('request_count', 0)
            if info.get('results_downloaded', False):
                total_downloaded += 1
        
        print(f"Total Batches Processed: {len(self.batch_status)}")
        print(f"Total Requests Across All Batches: {total_requests}")
        print(f"Total Batches with Results Downloaded: {total_downloaded}")
        
        # Print summary for each type
        for batch_type, batches in type_summaries.items():
            if not batches:
                continue
                
            print(f"\n📂 {batch_type.upper()} BATCHES ({len(batches)} total):")
            print("-" * 50)
            
            status_counts = {}
            for info in batches:
                status = info.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            for status, count in sorted(status_counts.items()):
                print(f"  {status:<12}: {count:>3}")
            
            print("\nBatch Details:")
            for info in sorted(batches, key=lambda x: x.get('batch_file', '')):
                batch_file = info.get('batch_file', 'unknown')
                status = info.get('status', 'unknown')
                count = info.get('request_count', 0)
                batch_id = info.get('batch_id', 'N/A')
                batch_id_display = batch_id[:15] + '...' if batch_id != 'N/A' and len(batch_id) > 15 else batch_id
                download_status = "✓ DL" if info.get('results_downloaded', False) else "PENDING" if status == 'ended' else ""
                print(f"    {batch_file:<15} | {status:<10} | {count:>4} reqs | {batch_id_display:<18} | {download_status}")
        
        print("=" * 100)


def main():
    """Main execution function - processes all folder types"""
    print("🤖 Multi-Folder Claude Batch Processing Tool")
    print("   Processes instruction, input, and output batches")
    print("=" * 70)
    
    my_api_key = "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"
    
    # Initialize processor
    try:
        processor = MultiFolderClaudeBatchProcessor(api_key=my_api_key)
    except ValueError as e:
        print(f"Initialization error: {e}")
        print("Please ensure your ANTHROPIC_API_KEY environment variable is set or pass it to the constructor.")
        return

    while True:
        print("\n" + "=" * 70)
        print("OPTIONS:")
        print("1. Submit first 5 batches from ALL types (instruction/input/output)")
        print("2. Check batch status (re-check all, auto-download missing)")
        print("3. Download completed results (manual for any not yet downloaded)")
        print("4. Show summary of all batches by type")
        print("5. Exit")
        print("=" * 70)
        
        choice = input("Enter your choice (1-5): ").strip()
        
        if choice == '1':
            print("\n🚀 Starting submission of first 5 batch files from all types...")
            processor.submit_first_5_batches_all_types()
            
        elif choice == '2':
            print("\nChecking status of all tracked batches...")
            processor.check_all_batches(auto_download=True)
            
        elif choice == '3':
            print("\nAttempting to download all completed, but not yet downloaded, results...")
            processor.download_all_completed_results()
            
        elif choice == '4':
            processor.print_summary()
                
        elif choice == '5':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    main()