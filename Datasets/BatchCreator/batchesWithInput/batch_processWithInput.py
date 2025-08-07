import os
import json

# === Configuration ===
BATCH_DIR = "."                              # where alpaca_data.jsonl lives
BASE_OUTPUT_DIR = "./batch_all_Output_claude"       # base directory for all batches
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

# Create separate directories for each field type
INSTRUCTION_DIR = os.path.join(BASE_OUTPUT_DIR, "instruction_batches")
INPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "input_batches")
OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "output_batches")

# Create all directories
for dir_path in [INSTRUCTION_DIR, INPUT_DIR, OUTPUT_DIR]:
    os.makedirs(dir_path, exist_ok=True)

def sanitize_custom_id(text):
    """Sanitizes text for use in custom_ids by replacing non-alphanumeric chars."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)

# Full prompt template
PROMPT_TEMPLATE = '''# Role and Objective

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
```json
{
  "text": "Education ki ahmiyat ke bare mein aik chhota essay likhiye."
}
```

## Example 2

### Input:
Explain how computers work.

### Output:
```json
{
  "text": "Computer kaise kaam karte hain wazahat kariye."
}
```

## Example 3

### Input:
List three benefits of exercise.

### Output:
```json
{
  "text": "Exercise ke teen fayde ki list banayiye."
}
```

## Example 4

### Input:
Describe the process of photosynthesis in plants.

### Output:
```json
{
  "text": "Plants mein photosynthesis ke process ki tafseel kariye."
}
```

## Example 5

### Input:
What are the main features of modern smartphones?

### Output:
```json
{
  "text": "Modern smartphones ke asli features kya hain?"
}
```

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
```json
{
  "text": "translated_text_here"
}
```

Do not include any explanations, comments, or additional text. Only return the JSON object with the translated text.

*Remember*: Your goal is to create clear, natural Roman Urdu translations that maintain the instructional value while being accessible to Roman Urdu speakers.

**Text to translate:**
'''

def create_batch_for_field(lines_chunk, field_name, batch_idx, original_safe_name, start_idx_global=0):
    """
    Creates a batch file for a specific field (instruction, input, or output).
    Processes all lines including empty ones.
    """
    # Determine output directory based on field
    if field_name == "instruction":
        output_dir = INSTRUCTION_DIR
    elif field_name == "input":
        output_dir = INPUT_DIR
    elif field_name == "output":
        output_dir = OUTPUT_DIR
    else:
        raise ValueError(f"Unknown field: {field_name}")
    
    output_path = os.path.join(output_dir, f"batch_{batch_idx}.jsonl")
    print(f"Processing {field_name} batch_{batch_idx} with {len(lines_chunk)} entries...")
    
    processed_count = 0
    with open(output_path, 'w', encoding='utf-8') as fout:
        for local_idx, line in enumerate(lines_chunk):
            global_idx = start_idx_global + local_idx
            raw = line.strip()
            
            # Handle all lines, including empty ones
            if not raw:
                # Handle blank lines - create empty data
                data = {}
                field_text = ""
            else:
                try:
                    data = json.loads(raw)
                    field_text = str(data.get(field_name, '')).strip()
                except json.JSONDecodeError as err:
                    print(f"❌ JSON decode error on line {global_idx}: {err}")
                    print("↳ Content:", repr(raw))
                    # Still process the line with empty content
                    field_text = ""

            # Combine prompt with the field text
            user_content = PROMPT_TEMPLATE + field_text

            # Claude API format
            params = {
                "model": "claude-sonnet-4-20250514",
                "system": "You are a translator that converts English to Roman Urdu.",
                "messages": [
                    {"role": "user", "content": user_content}
                ],
                "max_tokens": 6805,
                "temperature": 0.2
            }
            
            custom_id = f"{original_safe_name}_{field_name}_line_{global_idx}"
            record = {"custom_id": custom_id, "params": params}
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            processed_count += 1
    
    print(f"✅ Written: {field_name}/batch_{batch_idx}.jsonl ({processed_count} entries)")

def process_file(filename, batch_size=500):
    """Main orchestration function - creates batches for all three fields"""
    file_path = os.path.join(BATCH_DIR, filename)
    if not os.path.exists(file_path):
        print(f"Error: Source file '{file_path}' not found.")
        return

    base_safe = sanitize_custom_id(os.path.splitext(os.path.basename(file_path))[0])
    with open(file_path, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()

    total = len(all_lines)
    num_batches = (total + batch_size - 1) // batch_size
    print(f"Dividing '{filename}' into {num_batches} batches of {batch_size} lines each...")
    print(f"Creating separate batches for instruction, input, and output fields...")
    print(f"Processing ALL lines (including empty ones) - Total lines: {total}")

    # Process each batch
    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, total)
        chunk = all_lines[start:end]
        
        batch_num = i + 1
        print(f"\n--- Batch {batch_num}/{num_batches} (lines {start}–{end-1}) ---")
        
        # Create batches for each field
        for field_name in ["instruction", "input", "output"]:
            create_batch_for_field(
                lines_chunk=chunk,
                field_name=field_name,
                batch_idx=batch_num,
                original_safe_name=base_safe,
                start_idx_global=start
            )

    print(f"\n🎉 Processing complete!")
    print(f"📁 Created batches in:")
    print(f"   📂 Instructions: {INSTRUCTION_DIR}")
    print(f"   📂 Inputs: {INPUT_DIR}")
    print(f"   📂 Outputs: {OUTPUT_DIR}")

if __name__ == '__main__':
    # You can either set the full path here:
    source_file = 'alpaca_data_with_input.jsonl'  # or use full path
    
    # Or adjust BATCH_DIR and use just filename:
    # BATCH_DIR = r"E:\MCS_Project\BatchCreator"
    # source_file = 'alpaca_data.jsonl'
    
    process_file(source_file, batch_size=500)