import os
import json
import time
import anthropic
from datetime import datetime
from typing import List, Dict, Optional, Any

class ClaudeBatchProcessor:
    def __init__(self, api_key: str = None):
        """Initialize the Claude batch processor"""
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

        self.batch_dir = "Final/batch_all_Output_claude"  # Directory where batch files are stored
        self.results_dir = "Final/batch_results"
        self.status_file = "Final/batch_status.json"
        
        # Create directories if they don't exist
        os.makedirs(self.batch_dir, exist_ok=True) # Ensure batch_dir exists if it's for input
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Load or initialize status tracking
        self.batch_status = self.load_status()

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
                        # Basic validation for batch request structure if needed
                        # For Anthropic, each line should be a message or tool request object
                        requests.append(request)
                    except json.JSONDecodeError as e:
                        print(f"❌ Error parsing line {line_num} in {file_path}: {e}")
                        continue
        except Exception as e:
            print(f"❌ An unexpected error occurred while reading {file_path}: {e}")
            return []
        
        print(f"📁 Loaded {len(requests)} requests from {os.path.basename(file_path)}")
        return requests

    def submit_batch(self, batch_file: str) -> Optional[str]:
        """Submit a single batch file for processing"""
        file_path = os.path.join(self.batch_dir, batch_file)
        requests = self.load_batch_file(file_path)
        
        if not requests:
            print(f"⚠️  No valid requests found in {batch_file}")
            return None

        # Check if batch_file is already in status and has a successful batch_id
        if (batch_file in self.batch_status and 
            self.batch_status[batch_file].get('batch_id') and
            self.batch_status[batch_file].get('status') not in ['failed', 'expired', 'canceled']):
            print(f"⏭️  Batch {batch_file} already submitted and active (ID: {self.batch_status[batch_file]['batch_id']}). Skipping submission.")
            return self.batch_status[batch_file]['batch_id']

        try:
            print(f"🚀 Submitting batch: {batch_file}")
            
            # Submit the batch
            # Note: The Anthropic `batches.create` expects a list of dicts for requests
            message_batch = self.client.messages.batches.create(requests=requests)
            
            batch_id = message_batch.id
            print(f"✅ Batch submitted successfully!")
            print(f"   Batch ID: {batch_id}")
            print(f"   Status: {message_batch.processing_status}")
            # Ensure request_counts is properly handled for serialization
            request_counts_data = message_batch.request_counts.model_dump() if hasattr(message_batch.request_counts, 'model_dump') else str(message_batch.request_counts)
            print(f"   Request Count: {request_counts_data.get('total', 'N/A')}")
            
            # Update status tracking
            self.batch_status[batch_file] = {
                'batch_id': batch_id,
                'status': message_batch.processing_status,
                'submitted_at': datetime.now().isoformat(),
                'request_count': len(requests),
                'request_counts': request_counts_data,
                'results_downloaded': False # Initialize this for new submissions
            }
            self.save_status()
            
            return batch_id
            
        except anthropic.APIError as e:
            print(f"❌ Anthropic API Error submitting batch {batch_file}: {e}")
            error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
            self.batch_status[batch_file] = {
                'status': 'failed',
                'error': error_details,
                'submitted_at': datetime.now().isoformat()
            }
            self.save_status()
            return None
        except Exception as e:
            print(f"❌ General error submitting batch {batch_file}: {e}")
            self.batch_status[batch_file] = {
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
            # Use model_dump for robust dictionary conversion of Pydantic models
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
        
        # Iterate over a copy of keys because self.batch_status might be updated
        for batch_file in list(self.batch_status.keys()):
            info = self.batch_status[batch_file]
            if 'batch_id' in info and info.get('status') not in ['ended', 'expired', 'canceled', 'failed']:
                batch_id = info['batch_id']
                current_status = self.check_batch_status(batch_id)
                
                print(f"\n📁 {batch_file}")
                print(f"   Batch ID: {batch_id}")
                print(f"   Status: {current_status.get('status', 'unknown')}")
                print(f"   Request Counts: {current_status.get('request_counts', 'N/A')}")
                
                # Update status if changed
                old_status = info.get('status')
                new_status = current_status.get('status')
                
                if new_status and new_status != old_status:
                    self.batch_status[batch_file]['status'] = new_status
                    self.batch_status[batch_file]['updated_at'] = datetime.now().isoformat()
                    self.batch_status[batch_file]['request_counts'] = current_status.get('request_counts', 'N/A')
                    
                    # Check if newly completed
                    if new_status == 'ended' and old_status != 'ended':
                        newly_completed.append((batch_file, batch_id))
                        print(f"   🎉 COMPLETED! Ready for download.")
                elif new_status == 'ended' and not info.get('results_downloaded', False):
                    # Batch was already 'ended' but not downloaded in a previous run
                    newly_completed.append((batch_file, batch_id))
                    print(f"   🎉 COMPLETED (Previously)! Ready for download.")

            elif info.get('status') == 'ended' and info.get('results_downloaded', False):
                print(f"\n📁 {batch_file}")
                print(f"   Batch ID: {info.get('batch_id', 'N/A')}")
                print(f"   Status: {info.get('status', 'ended')}")
                print(f"   Request Counts: {info.get('request_counts', 'N/A')}")
                print(f"   💾 Results already downloaded to: {info.get('results_file', 'N/A')}")
            else:
                # For batches that are failed, expired, canceled, or not yet submitted
                print(f"\n📁 {batch_file}")
                print(f"   Status: {info.get('status', 'Not Submitted/Unknown')}")
                if 'error' in info:
                    print(f"   Error: {info['error']}")
        
        self.save_status()
        
        # Auto-download newly completed batches
        if auto_download and newly_completed:
            print(f"\n🔄 Auto-downloading {len(newly_completed)} newly completed batch(es)...")
            for batch_file, batch_id in newly_completed:
                # Double-check before downloading
                if not self.batch_status[batch_file].get('results_downloaded', False):
                    print(f"\n📥 Auto-downloading: {batch_file}")
                    self.download_batch_results(batch_id, batch_file)

    def download_batch_results(self, batch_id: str, batch_file: str):
        """Download results for a completed batch"""
        try:
            batch = self.client.messages.batches.retrieve(batch_id)
            
            if batch.processing_status != 'ended':
                print(f"⚠️  Batch {batch_id} not ready (status: {batch.processing_status})")
                return False
                
            # Get results - this returns an iterable of BatchResult objects
            results_iterator = self.client.messages.batches.results(batch_id)
            
            # Save results with proper serialization
            # Ensure the output filename is distinct, e.g., results_batch_1.jsonl
            results_file_name = f"results_{os.path.basename(batch_file)}"
            results_file_path = os.path.join(self.results_dir, results_file_name)

            has_results = False
            with open(results_file_path, 'w', encoding='utf-8') as f:
                for result in results_iterator:
                    has_results = True
                    # Most Anthropic client objects (Pydantic models) have .model_dump()
                    # to convert themselves into a JSON-serializable dictionary.
                    try:
                        result_dict = result.model_dump(mode='json') # Use mode='json' for better JSON compatibility
                    except AttributeError:
                        # Fallback for older clients or different object types.
                        # This part might need further refinement depending on the exact structure
                        # of the `result` object if model_dump() is not available.
                        # For a standard batch result, it usually contains 'id', 'custom_id', 'response', 'error'.
                        # The 'response' might contain a 'body' which is a Message object itself.
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
                                # If body is a Message object, try to dump it
                                if hasattr(response.body, 'model_dump'):
                                    response_dict['body'] = response.body.model_dump(mode='json')
                                else:
                                    response_dict['body'] = str(response.body) # Fallback to string representation
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
                self.batch_status[batch_file]['results_downloaded'] = True
                self.batch_status[batch_file]['results_file'] = results_file_path
                self.batch_status[batch_file]['downloaded_at'] = datetime.now().isoformat()
                self.save_status()
                return True
            else:
                print(f"⚠️ No results found for batch {batch_id}. File not created.")
                return False
            
        except anthropic.APIError as e:
            print(f"❌ Anthropic API Error downloading results for {batch_id}: {e}")
            self.batch_status[batch_file]['download_error'] = e.response.json() if hasattr(e.response, 'json') else str(e)
            self.save_status()
            return False
        except Exception as e:
            print(f"❌ General error downloading results for {batch_id}: {e}")
            self.batch_status[batch_file]['download_error'] = str(e)
            self.save_status()
            return False

    def download_all_completed_results(self):
        """Download results for all completed batches that haven't been downloaded yet"""
        print("\n💾 Downloading results for completed batches...")
        
        # Iterate over a copy of keys to avoid issues if self.batch_status is modified
        for batch_file in list(self.batch_status.keys()):
            info = self.batch_status[batch_file]
            if (info.get('status') == 'ended' and 
                'batch_id' in info and 
                not info.get('results_downloaded', False)):
                
                print(f"\n📁 Downloading results for {batch_file}...")
                self.download_batch_results(info['batch_id'], batch_file)
            elif info.get('status') == 'ended' and info.get('results_downloaded', False):
                print(f"\n✔️ Results for {batch_file} already downloaded.")
            elif 'batch_id' in info:
                print(f"\n Skipping {batch_file}: Status is '{info.get('status')}' (not 'ended').")
            else:
                print(f"\n Skipping {batch_file}: No batch ID or unknown status.")

    def submit_first_15_batches(self, delay_between_batches: int = 2, auto_check_interval: int = 30):
        """Submit only the first 15 batch files with automatic status checking"""
        batch_files = [f for f in os.listdir(self.batch_dir) if f.endswith('.jsonl')]
        
        # Sort numerically by batch number (batch_1, batch_2, batch_3, etc.)
        def extract_batch_number(filename):
            try:
                # Extract number from "batch_X.jsonl"
                return int(filename.replace('batch_', '').replace('.jsonl', ''))
            except ValueError:
                return float('inf')  # Put invalid files at the end
        
        batch_files.sort(key=extract_batch_number)
        
        if not batch_files:
            print(f"❌ No batch files found in {self.batch_dir}. Please place .jsonl files there.")
            return
        
        # Limit to first 15 batches
        batch_files = batch_files[:15]
        print(f"📦 Processing first {len(batch_files)} batch files")
        print(f"Files to process: {', '.join(batch_files)}")
        
        # Submit all batches first
        submitted_batches_to_monitor = []
        for i, batch_file in enumerate(batch_files, 1):
            print(f"\n--- Processing {i}/{len(batch_files)}: {batch_file} ---")
            
            # submit_batch now handles skipping already submitted ones
            batch_id = self.submit_batch(batch_file)
            if batch_id:
                submitted_batches_to_monitor.append(batch_file)
                
            if i < len(batch_files) and batch_id: # Only delay if submission was successful
                print(f"⏳ Waiting {delay_between_batches} seconds before next batch...")
                time.sleep(delay_between_batches)
        
        print(f"\n✅ Submission complete! {len(submitted_batches_to_monitor)} batches submitted/tracked for monitoring.")
        
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
    
    def monitor_and_auto_download(self, batch_files_to_monitor: List[str], check_interval: int = 30):
        """Continuously monitor batches and auto-download when complete"""
        
        while True:
            completed_downloaded_count = sum(1 for bf in batch_files_to_monitor 
                                             if self.batch_status.get(bf, {}).get('results_downloaded', False))
            
            total_to_monitor = len(batch_files_to_monitor)
            
            print(f"\n🔍 Checking status... ({completed_downloaded_count}/{total_to_monitor} completed and downloaded)")
            
            # Check status with auto-download
            self.check_all_batches(auto_download=True)
            
            # Recount completed batches after checking
            completed_downloaded_count = sum(1 for bf in batch_files_to_monitor 
                                             if self.batch_status.get(bf, {}).get('results_downloaded', False))
            
            if completed_downloaded_count >= total_to_monitor:
                print(f"\n🎉 ALL MONITORED BATCHES COMPLETED AND DOWNLOADED! ({completed_downloaded_count}/{total_to_monitor})")
                print("   All results have been automatically downloaded to ./batch_results/")
                break
                
            print(f"⏳ Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)

    def print_summary(self):
        """Print a summary of all batch operations"""
        if not self.batch_status:
            print("📊 No batches have been processed yet.")
            return
            
        print("\n" + "=" * 80)
        print("📊 BATCH PROCESSING SUMMARY")
        print("=" * 80)
        
        status_counts = {}
        total_requests = 0
        total_downloaded = 0
        
        # Consolidate status for display, handle duplicate entries
        unique_batches = {}
        for batch_file, info in self.batch_status.items():
            # Use batch_id as a primary key to handle potential duplicate batch_file entries
            # or if the same file was submitted multiple times leading to different IDs.
            batch_id = info.get('batch_id')
            if batch_id and batch_id not in unique_batches:
                unique_batches[batch_id] = {
                    'batch_file': batch_file,
                    'status': info.get('status', 'unknown'),
                    'request_count': info.get('request_count', 0),
                    'results_downloaded': info.get('results_downloaded', False),
                    'display_id': batch_id # For internal use in summary
                }
            elif not batch_id: # For failed submissions that didn't get an ID
                # Generate a pseudo-ID for display if no batch_id
                pseudo_id = f"NO_ID_{batch_file}"
                if pseudo_id not in unique_batches:
                     unique_batches[pseudo_id] = {
                        'batch_file': batch_file,
                        'status': info.get('status', 'unknown'),
                        'request_count': info.get('request_count', 0),
                        'results_downloaded': info.get('results_downloaded', False),
                        'display_id': "N/A" # For display only
                    }

        for batch_id_key, info in unique_batches.items():
            status = info['status']
            status_counts[status] = status_counts.get(status, 0) + 1
            total_requests += info['request_count']
            if info['results_downloaded']:
                total_downloaded += 1
            
        print(f"Total Unique Batches Processed: {len(unique_batches)}")
        print(f"Total Requests Across Batches: {total_requests}")
        print(f"Total Batches with Results Downloaded: {total_downloaded}")
        
        print("\nStatus Breakdown:")
        for status, count in sorted(status_counts.items()):
            print(f"  {status:<10}: {count:>4}")
            
        print("\nBatch Details:")
        # Sort batches for consistent display
        sorted_batches_for_display = sorted(unique_batches.values(), key=lambda x: extract_batch_number(x['batch_file']) if x['batch_file'].startswith('batch_') else float('inf'))

        for info in sorted_batches_for_display:
            status = info['status']
            count = info['request_count']
            batch_id_display = info['display_id'][:20] + '...' if info['display_id'] != 'N/A' and len(info['display_id']) > 20 else info['display_id']
            download_status = "DOWNLOADED" if info['results_downloaded'] else "PENDING DL" if status == 'ended' else ""
            print(f"  {info['batch_file']:<20} | {status:<12} | {count:>4} reqs | {batch_id_display:<23} | {download_status}")
        print("=" * 80)


def main():
    """Main execution function - processes first 15 batches only"""
    print("🤖 Claude Batch Processing Tool - First 15 Batches")
    print("=" * 60)
    
    my_api_key = "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" # Replace with your actual key
    # Initialize processor
    try:
        processor = ClaudeBatchProcessor(api_key=my_api_key)
    except ValueError as e:
        print(f"Initialization error: {e}")
        print("Please ensure your ANTHROPIC_API_KEY environment variable is set or pass it to the constructor.")
        return

    while True:
        print("\n" + "=" * 60)
        print("OPTIONS:")
        print("1. Submit first 15 batches (with auto-download & monitoring)")
        print("2. Check batch status (re-check all, auto-download missing)")
        print("3. Download completed results (manual for any not yet downloaded)")
        print("4. Show summary of all batches")
        print("5. Exit")
        print("=" * 60)
        
        choice = input("Enter your choice (1-5): ").strip()
        
        if choice == '1':
            print("\n🚀 Starting submission of first 15 batch files...")
            processor.submit_first_15_batches()
            
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