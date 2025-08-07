import os
import json

# === Configuration ===
BATCH_DIR = "."                              # where alpaca_data.jsonl lives
PREPARED_DIR = "./batch_all_Output_claude"           # where to write your batches
os.makedirs(PREPARED_DIR, exist_ok=True)

# Lines you want to drop entirely (zero-based indices)
SKIP_LINE_INDICES = {3876, 4353, 6442, 12472, 14639}

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

def wrap_for_batch_chunk(lines_chunk, batch_idx, original_safe_name, start_idx_global=0):
    """
    Processes a chunk of lines and writes prepared API requests to batch file.
    Uses 'output' field instead of 'instruction'; skips the SKIP_LINE_INDICES.
    """
    output_path = os.path.join(PREPARED_DIR, f"batch_{batch_idx}.jsonl")
    print(f"Processing batch_{batch_idx} with {len(lines_chunk)} entries...")
    
    with open(output_path, 'w', encoding='utf-8') as fout:
        for local_idx, line in enumerate(lines_chunk):
            global_idx = start_idx_global + local_idx
            raw = line.strip()
            
            # 1) Skip only the explicitly listed bad lines:
            if global_idx in SKIP_LINE_INDICES:
                print(f"⚠️  Dropping line {global_idx} (in skip list)")
                continue

            if not raw:
                # Handle blank lines
                data = {}
            else:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError as err:
                    print(f"❌ JSON decode error on line {global_idx}: {err}")
                    print("↳ Content:", repr(raw))
                    continue

            # Extract the 'output' field only (changed from 'instruction'):
            output_text = str(data.get('output', '')).strip()

            # Combine prompt with the output text
            user_content = PROMPT_TEMPLATE + output_text

            # Claude API format (keeping your original structure)
            params = {
                "model": "claude-sonnet-4-20250514",
                "system": "You are a translator that converts English to Roman Urdu.",
                "messages": [
                    {"role": "user", "content": user_content}
                ],
                "max_tokens": 6805,
                "temperature": 0.2
            }
            
            custom_id = f"{original_safe_name}_line_{global_idx}"
            record = {"custom_id": custom_id, "params": params}
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
    
    print(f"✅ Written: batch_{batch_idx}.jsonl")

def process_file(filename, batch_size=500):
    """Main orchestration function"""
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
    print(f"Will skip lines: {sorted(SKIP_LINE_INDICES)}")

    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, total)
        chunk = all_lines[start:end]
        
        print(f"\n--- Batch {i+1}/{num_batches} (lines {start}–{end-1}) ---")
        wrap_for_batch_chunk(
            lines_chunk=chunk,
            batch_idx=i+1,
            original_safe_name=base_safe,
            start_idx_global=start
        )

if __name__ == '__main__':
    # You can either set the full path here:
    source_file = 'Final/alpaca_data.jsonl'  # or use full path like in your OpenAI version
    
    # Or adjust BATCH_DIR and use just filename:
    # BATCH_DIR = r"E:\MCS_Project\BatchCreator"
    # source_file = 'alpaca_data.jsonl'
    
    process_file(source_file, batch_size=500)