import time
import anthropic
import json
import os

# === CONFIGURATION ===
API_KEY = "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"  # <-- Replace with your anthropic API key
INPUT_FILE_PATH = "Final/batch_all_Output_claude/batch_1.jsonl"
OUTPUT_FILE_PATH = "Final/batch_1.jsonl"
COMPLETION_WINDOW = "24h"

client = anthropic.Anthropic(api_key=API_KEY)

# === STEP 1: Validate JSONL File ===
def validate_jsonl(file_path):
    print("🔍 Validating JSONL file...")
    with open(file_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            try:
                json.loads(line)
            except json.JSONDecodeError as e:
                print(f"❌ Line {i} is invalid: {e}")
                return False
    print("✅ JSONL file is valid.")
    return True


# === STEP 2: Upload File to anthropic ===
def upload_file(file_path):
    print("📤 Uploading file to anthropic...")
    with open(file_path, "rb") as f:
        response = client._files.create(file=f, purpose="batch")
    print(f"📁 Uploaded File ID: {response.id}")
    return response.id


# === STEP 3: Create Batch Job ===
def create_batch(file_id):
    print("🚀 Creating batch job...")
    response = client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/messages",
        completion_window=COMPLETION_WINDOW,
    )
    print(f"🆔 Batch Job ID: {response.id}")
    return response.id


# === STEP 4: Poll for Completion ===
def wait_for_batch_completion(batch_id, interval=10):
    print("⏳ Waiting for batch to complete...")
    while True:
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        print(f"🔄 Status: {status}")
        if status in ["completed", "failed", "cancelled"]:
            return batch
        time.sleep(interval)


# === STEP 5: Download Output ===
def download_output_file(file_id, output_path):
    print("📥 Downloading output...")
    file = client._files.retrieve(file_id)
    content = client._files.download(file.id)
    with open(output_path, "wb") as f:
        f.write(content.read())
    print(f"✅ Output saved to: {output_path}")


# === MAIN EXECUTION ===
def main():
    if not os.path.exists(INPUT_FILE_PATH):
        print(f"❌ File not found: {INPUT_FILE_PATH}")
        return

    if not validate_jsonl(INPUT_FILE_PATH):
        return

    file_id = upload_file(INPUT_FILE_PATH)
    batch_id = create_batch(file_id)
    batch_result = wait_for_batch_completion(batch_id)

    if batch_result.status == "completed":
        output_file_id = batch_result.output_file_id
        download_output_file(output_file_id, OUTPUT_FILE_PATH)
    else:
        print(f"❌ Batch failed with status: {batch_result.status}")


if __name__ == "__main__":
    main()
