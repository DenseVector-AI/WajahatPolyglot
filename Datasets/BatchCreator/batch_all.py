import os
import json

# Directory where your source file (alpaca_data.jsonl) is located
BATCH_DIR = "."
# Directory where the prepared batch files will be saved
PREPARED_DIR = "./batch_all_final"
os.makedirs(PREPARED_DIR, exist_ok=True)


def sanitize_custom_id(text):
    """Sanitizes text for use in custom_ids by replacing non-alphanumeric chars."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)

def wrap_for_batch_chunk(lines_chunk, output_file_path, base_filename, start_idx_global=0, original_file_safe_name_for_id=""):
    """
    Processes a chunk of lines and writes prepared API requests to a specified output file.

    Args:
        lines_chunk (list): A list of lines (strings) from the input JSONL file.
        output_file_path (str): The full path to the output JSONL file for this batch.
        base_filename (str): The base name of the original input file (for logging and debug).
        start_idx_global (int): The starting global index of this chunk within the original file.
        original_file_safe_name_for_id (str): The sanitized base name of the original file, used for custom_id.
    """
    print(f"Processing chunk for '{os.path.basename(output_file_path)}'...")
    with open(output_file_path, "w", encoding="utf-8") as fout:
        for local_idx, line in enumerate(lines_chunk):
            # Calculate the global index for unique custom_ids
            global_idx = start_idx_global + local_idx
            line = line.strip()

            if not line:
                print(f"⚠️  Skipping empty line {global_idx} in {base_filename}")
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError as err:
                print(f"❌ JSON decode error on line {global_idx} in {base_filename}: {err}")
                print("↳ Content:", repr(line))
                continue

            # Extracting all three fields: 'instruction', 'input', and 'output'
            instruction_text = str(data.get("instruction", "")).strip()
            input_text = str(data.get("input", "")).strip()
            output_text = str(data.get("output", "")).strip()

            # --- DEBUGGING SNIPPET: Inspect values before skipping ---
            if not instruction_text or not output_text:
                print(f"\n--- DEBUG: Skipping line {global_idx} in {base_filename} ---")
                print(f"  Raw 'instruction' value: '{data.get('instruction', 'KEY_NOT_FOUND')}' (repr: {repr(data.get('instruction'))})")
                print(f"  Stripped 'instruction_text': '{instruction_text}' (repr: {repr(instruction_text)})")
                print(f"  Raw 'output' value: '{data.get('output', 'KEY_NOT_FOUND')}' (repr: {repr(data.get('output'))})")
                print(f"  Stripped 'output_text': '{output_text}' (repr: {repr(output_text)})")
                print(f"-----------------------------------------------\n")
            # --- END DEBUGGING SNIPPET ---

            if not instruction_text or not output_text:
                print(f"⚠️  Skipping line {global_idx} in {base_filename}: 'instruction' or 'output' missing or empty")
                continue

            # Combine all three fields into a structured format for the prompt
            combined_data_for_prompt = (
                f"**Text**{instruction_text}\n"
            )

            # The full, detailed prompt template for the LLM
            prompt_template = """ # Role and Objective

You are a Professional English to Roman Urdu Translator specializing in instructional text translation.

Your role is to translate English text into Roman Urdu while preserving standard English terms and maintaining natural structure.

Your output must reflect accurate translation with proper Roman Urdu phonetics and unchanged formatting.

## Instructions

1. Translate the given English text into Roman Urdu.
2. Do not modify, explain, summarize, or interpret the content.
3. Keep original punctuation, line breaks, and word order intact.
4. Maintain standard English words that are commonly used in Roman Urdu contexts.
5. Ensure the translation sounds natural and clear for Roman Urdu speakers.

## English Term Preservation Rules

#### Standard Terms to Keep in English (correctly spelled)
- *Educational terms*: school, education, university, college, student, teacher, class, grade, curriculum, international
- *Technology terms*: computer, internet, mobile, software, hardware, website, email, digital, online
- *Professional terms*: doctor, engineer, manager, business, office, company, organization
- *Common nouns*: hospital, hotel, restaurant, market, station, airport, bank
- *Proper names*: Countries, cities, famous personalities, brand names
- *Scientific terms*: biology, chemistry, physics, mathematics, science, research
- *Modern concepts*: social media, smartphone, laptop, tablet, GPS, WiFi

#### Phonetic Translation Rules (for non-standard terms)

Map English sounds to Roman Urdu equivalents:

*Vowels:*
- a → a/aa     e → e/ee     i → i/ii     o → o/oo     u → u/uu

*Consonants:*
- th → th      sh → sh      ch → ch      ph → ph/f
- ck → k       gh → gh      kh → kh      ng → ng
- Silent letters often omitted in Roman Urdu

## Structural Preservation

- Keep original sentence structure and word order
- Retain all punctuation marks (periods, commas, question marks, etc.)
- Preserve line breaks and spacing
- Maintain capitalization patterns where relevant
- Do not merge or split sentences

## Quality Assurance Checklist

✓ Every English word/phrase has appropriate Roman Urdu rendering
✓ Standard English terms preserved correctly
✓ Formatting and structure maintained
✓ Natural Roman Urdu flow maintained
✓ Technical accuracy preserved

## Forbidden Actions

❌ Adding explanations or commentary
❌ Changing sentence structure or word order
❌ Over-translating standard English terms
❌ Modifying punctuation or formatting
❌ Combining or splitting original entries

# Examples

## Example 1

### Input:
Write a short essay about the importance of education.


### Output:
json
{
  "text": "Education ki ahmiyat ke bare mein aik chhota essay likhiye."
}


## Example 2

### Input:
Explain how computers work.


### Output:
json
{
  "text": "Computer kaise kaam karte hain wazahat kariye."
}


## Example 3

### Input:
List three benefits of exercise.


### Output:
json
{
  "text": "Exercise ke teen fayde ki list banayiye."
}


## Example 4

### Input:
Describe the process of photosynthesis in plants.


### Output:
json
{
  "text": "Plants mein photosynthesis ke process ki tafseel kariye."
}


## Example 5

### Input:
What are the main features of modern smartphones?


### Output:
json
{
  "text": "Modern smartphones ke asli features kya hain?"
}


# Processing Steps

For each text entry:

1. *Analyze the text*:
   - Identify command type (write, explain, list, describe, etc.)
   - Check for standard English terms to preserve
   - Identify the tone (imperative, interrogative, etc.)

2. *Apply translation rules*:
   - Preserve standard English terms as-is
   - Translate remaining content to Roman Urdu
   - Maintain natural flow and readability

3. *Quality check*:
   - Verify proper Roman Urdu phonetics
   - Ensure standard terms are preserved
   - Check for natural Roman Urdu flow
   - Confirm formatting is maintained

# Final Instructions

1. Process each text entry independently
2. Translate the English text into Roman Urdu
3. Output in the specified JSON format with "text" field
4. Keep standard English terms unchanged
5. Ensure natural Roman Urdu translation for all other content
6. Provide ONLY the JSON output with no additional commentary

## Required Output Format:

For any given English text, provide ONLY the translated text in this exact JSON format:
json
{
  "text": "translated_text_here"
}


Do not include any explanations, comments, or additional text. Only return the JSON object with the translated text.

*Remember*: Your goal is to create clear, natural Roman Urdu translations that maintain the instructional value while being accessible to Roman Urdu speakers."
{alpaca}

**Output:"""

            # Replace the placeholder with the combined data
            prompt = prompt_template.replace("{alpaca}", combined_data_for_prompt)

            # Construct the API request body
            request = {
                "custom_id": f"{original_file_safe_name_for_id}_line_{global_idx}", # Use the passed sanitized name here
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4.1-2025-04-14",
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a translator that converts Urdu to Roman Urdu."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 6805,
                    "temperature": 0.2
                }
            }

            try:
                json_line = json.dumps(request, ensure_ascii=False)
                fout.write(json_line + "\n")
            except Exception as json_err:
                print(f"❌ JSON dump error on line {global_idx} of {base_filename}: {json_err}")
                print("↳ Request that failed:", repr(request))

    print(f"✅ Prepared: {os.path.basename(output_file_path)}")


def process_file_in_batches(filename, batch_size=250):
    """
    Reads a JSONL file, divides its content into chunks, and processes each chunk
    into a separate prepared batch file.

    Args:
        filename (str): The name of the input JSONL file (e.g., "alpaca_data.jsonl").
        batch_size (int): The maximum number of lines per output batch file.
    """
    file_path = os.path.join(BATCH_DIR, filename)
    if not os.path.exists(file_path):
        print(f"Error: Source file '{file_path}' not found. Make sure it's in the same directory as the script.")
        return

    # Extract and sanitize base name for output files and custom_ids
    # This line is not strictly needed for the batch file naming itself if you want just "batch_X.jsonl"
    # but it's used for the custom_id, so keep it.
    base_file_name_for_output = sanitize_custom_id(os.path.basename(file_path).replace(".jsonl", ""))

    all_lines = []
    with open(file_path, "r", encoding="utf-8") as f_in:
        all_lines = f_in.readlines()

    total_lines = len(all_lines)
    num_batches = (total_lines + batch_size - 1) // batch_size

    print(f"\n--- Dividing '{filename}' into {num_batches} batches of {batch_size} lines each ---")

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_lines)
        current_batch_lines = all_lines[start_index:end_index]

        # MODIFICATION 1: Change how the batch_output_filename is generated
        # This will create filenames like "batch_1.jsonl", "batch_2.jsonl", etc.
        batch_output_filename = f"batch_{i+1}.jsonl"
        batch_output_path = os.path.join(PREPARED_DIR, batch_output_filename)

        print(f"\n--- Starting Batch {i+1}/{num_batches} (lines {start_index} to {end_index-1}) ---")
        wrap_for_batch_chunk(
            lines_chunk=current_batch_lines,
            output_file_path=batch_output_path,
            base_filename=os.path.basename(file_path),
            start_idx_global=start_index,
            original_file_safe_name_for_id=base_file_name_for_output
        )

# Main execution block
if __name__ == "__main__":
    source_file_name = r"E:\MCS_Project\BatchCreator\alpaca_data.jsonl" # Your input file name
    # MODIFICATION 2: Change the batch_size here to 500
    process_file_in_batches(source_file_name, batch_size=500)