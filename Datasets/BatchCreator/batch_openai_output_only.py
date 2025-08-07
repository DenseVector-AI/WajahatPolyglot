import os
import json

# === Configuration ===
BATCH_DIR = "."                              # where alpaca_data.jsonl lives
PREPARED_DIR = "./batch_all_Output"           # where to write your batches
os.makedirs(PREPARED_DIR, exist_ok=True)

# Lines you want to drop entirely (zero-based indices)
SKIP_LINE_INDICES = {3876, 4353, 6442, 12472, 14639}

def sanitize_custom_id(text):
    """Sanitizes text for use in custom_ids by replacing non-alphanumeric chars."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)

def wrap_for_batch_chunk(lines_chunk, output_file_path, base_filename, start_idx_global=0, original_file_safe_name_for_id=""):
    """
    Processes a chunk of lines and writes prepared API requests to a specified output file.
    Uses 'output' field instead of 'instruction'; only skips the five SKIP_LINE_INDICES.
    """
    print(f"Processing chunk for '{os.path.basename(output_file_path)}'...")
    with open(output_file_path, "w", encoding="utf-8") as fout:
        for local_idx, line in enumerate(lines_chunk):
            global_idx = start_idx_global + local_idx
            raw = line.strip()
            
            # 1) Skip only the explicitly listed bad lines:
            if global_idx in SKIP_LINE_INDICES:
                print(f"⚠️  Dropping line {global_idx} (in skip list)")
                continue

            if not raw:
                # blank lines get processed too (you said “do not miss any other output even if empty”)
                data = {}
            else:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError as err:
                    print(f"❌ JSON decode error on line {global_idx}: {err}")
                    print("↳ Content:", repr(raw))
                    continue

            # Extract the 'output' field only:
            output_text = str(data.get("output", "")).strip()

            # Build your prompt around the output_text:
            combined_data_for_prompt = f"**Text**{output_text}\n"

            # (Copy your prompt_template here unmodified…)
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

            prompt = prompt_template.replace("{alpaca}", combined_data_for_prompt)

            request = {
                "custom_id": f"{original_file_safe_name_for_id}_line_{global_idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4.1-2025-04-14",
                    "messages": [
                        { "role": "system", "content": "You are a translator that converts Urdu to Roman Urdu." },
                        { "role": "user",   "content": prompt }
                    ],
                    "max_tokens": 6805,
                    "temperature": 0.2
                }
            }

            fout.write(json.dumps(request, ensure_ascii=False) + "\n")

    print(f"✅ Prepared: {os.path.basename(output_file_path)}")

def process_file_in_batches(filename, batch_size=250):
    file_path = os.path.join(BATCH_DIR, filename)
    if not os.path.exists(file_path):
        print(f"Error: Source file '{file_path}' not found.")
        return

    base_safe = sanitize_custom_id(os.path.basename(file_path).replace(".jsonl", ""))
    with open(file_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    total = len(all_lines)
    num_batches = (total + batch_size - 1) // batch_size
    print(f"Dividing '{filename}' into {num_batches} batches of {batch_size} lines each")

    for i in range(num_batches):
        start = i * batch_size
        end   = min((i + 1) * batch_size, total)
        out_name = f"batch_{i+1}.jsonl"
        out_path = os.path.join(PREPARED_DIR, out_name)

        print(f"\n--- Batch {i+1}/{num_batches} (lines {start}–{end-1}) ---")
        wrap_for_batch_chunk(
            lines_chunk=all_lines[start:end],
            output_file_path=out_path,
            base_filename=filename,
            start_idx_global=start,
            original_file_safe_name_for_id=base_safe
        )

if __name__ == "__main__":
    source_file_name = r"E:\MCS_Project\BatchCreator\alpaca_data.jsonl" # Your input file name
    # MODIFICATION 2: Change the batch_size here to 500
    process_file_in_batches(source_file_name, batch_size=500)

    # — or adjust BATCH_DIR and pass just the filename,
    #   e.g.:
    # BATCH_DIR = r"E:\MCS_Project\BatchCreator"
    # process_file_in_batches("alpaca_data.jsonl", batch_size=500)
