import os
import json

# Directory where your source file (alpaca_data.jsonl) is located
BATCH_DIR = "."
# Directory where the prepared batch files will be saved
PREPARED_DIR = "./batch_all"
# Ensure output directory exists
os.makedirs(PREPARED_DIR, exist_ok=True)

# Utility to sanitize names for custom_id
def sanitize_custom_id(text):
    """Sanitize text for safe custom IDs by replacing non-alphanumeric chars."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)


def wrap_for_batch_chunk(lines_chunk, output_file_path, base_filename, start_idx_global=0, original_file_safe_name_for_id=""):
    """
    Process a chunk of lines and write prepared API requests to output JSONL.
    """
    print(f"Processing chunk for '{os.path.basename(output_file_path)}'...")
    with open(output_file_path, "w", encoding="utf-8") as fout:
        for local_idx, line in enumerate(lines_chunk):
            global_idx = start_idx_global + local_idx
            line = line.strip()
            if not line:
                print(f"⚠️  Skipping empty line {global_idx} in {base_filename}")
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError as err:
                print(f"❌ JSON decode error on line {global_idx} in {base_filename}: {err}")
                continue

            # Extract only the 'text' field (previously 'instruction')
            text_text = str(data.get("text", "")).strip()
            output_text = str(data.get("output", "")).strip()

            # Skip if mandatory fields missing
            if not text_text or not output_text:
                print(f"⚠️  Skipping line {global_idx} in {base_filename}: 'text' or 'output' missing or empty")
                continue

            # Build combined prompt using the renamed field
            combined_data_for_prompt = f"**Text** {text_text}\n"

            # Full LLM prompt template
            prompt_template = ''' Role and Objective
... (template unchanged) ... {alpaca}

**Output:**'''

            # Inject the actual prompt where placeholder exists
            prompt = prompt_template.replace("{alpaca}", combined_data_for_prompt)

            # Build the API request body exactly as before
            request = {
                "custom_id": f"{original_file_safe_name_for_id}_line_{global_idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4.1-2025-04-14",
                    "messages": [
                        {"role": "system", "content": "You are a translator that converts Urdu to Roman Urdu."},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 6805,
                    "temperature": 0.2
                }
            }

            # Serialize and write the request
            try:
                fout.write(json.dumps(request, ensure_ascii=False) + "\n")
            except Exception as json_err:
                print(f"❌ JSON dump error on line {global_idx} of {base_filename}: {json_err}")
                print("↳ Request that failed:", repr(request))

    print(f"✅ Prepared: {os.path.basename(output_file_path)}")


def process_file_in_batches(filename, batch_size=250):
    """
    Split a JSONL file into batches and process each.
    """
    file_path = os.path.join(BATCH_DIR, filename)
    if not os.path.exists(file_path):
        print(f"Error: Source file '{file_path}' not found.")
        return

    # Use sanitized base name for custom IDs only
    base_name = sanitize_custom_id(os.path.splitext(os.path.basename(file_path))[0])

    lines = open(file_path, 'r', encoding='utf-8').read().splitlines()
    total = len(lines)
    num_batches = (total + batch_size - 1) // batch_size

    print(f"\n--- Dividing '{filename}' into {num_batches} batches of {batch_size} lines each ---")

    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, total)
        chunk = lines[start:end]

        # New output filenames: batch_1.jsonl, batch_2.jsonl, etc.
        batch_number = i + 1
        out_name = f"batch_{batch_number}.jsonl"
        out_path = os.path.join(PREPARED_DIR, out_name)

        print(f"\n--- Batch {batch_number}/{num_batches} (lines {start}-{end-1}) ---")
        wrap_for_batch_chunk(
            lines_chunk=chunk,
            output_file_path=out_path,
            base_filename=os.path.basename(file_path),
            start_idx_global=start,
            original_file_safe_name_for_id=base_name
        )


if __name__ == "__main__":
    process_file_in_batches("alpaca_data.jsonl", batch_size=500)