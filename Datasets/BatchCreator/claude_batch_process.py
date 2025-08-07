import os
import json

# Directories
BATCH_DIR = "."
PREPARED_DIR = "./batch_all"
os.makedirs(PREPARED_DIR, exist_ok=True)

# Helper to sanitize IDs
def sanitize_custom_id(text):
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)

# Full prompt template
PROMPT_TEMPLATE = ''' Role and Objective

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
Input: Write a short essay about the importance of education.
Output: {"text": "Education ki ahmiyat ke bare mein aik chhota essay likhiye."}
# ... (other examples omitted for brevity)
## Final Instructions
For each English text, output JSON: {"text": "translated_text_here"}
Only return the JSON.
'''

# Build a batch of Claude request payloads
def wrap_for_batch_chunk(lines, batch_idx, original_safe_name):
    output_path = os.path.join(PREPARED_DIR, f"batch_{batch_idx}.jsonl")
    with open(output_path, 'w', encoding='utf-8') as fout:
        print(f"Preparing batch_{batch_idx} with {len(lines)} entries...")
        for local_idx, line in enumerate(lines):
            idx_global = (batch_idx - 1) * BATCH_SIZE + local_idx
            raw = line.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # Only use the instruction field, renamed to text
            text = str(data.get('instruction', '')).strip()
            if not text:
                continue

            # Combine prompt
            user_content = PROMPT_TEMPLATE + text

            params = {
                "model": "claude-sonnet-4-20250514",
                "system": "You are a translator that converts Urdu to Roman Urdu.",
                "messages": [
                    {"role": "user", "content": user_content}
                ],
                "max_tokens": 6805,
                "temperature": 0.2
            }
            custom_id = f"{original_safe_name}_line_{idx_global}"
            record = {"custom_id": custom_id, "params": params}
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"✅ Written: batch_{batch_idx}.jsonl")

# Main orchestration
def process_file(filename, batch_size=250):
    global BATCH_SIZE
    BATCH_SIZE = batch_size
    path = os.path.join(BATCH_DIR, filename)
    if not os.path.exists(path):
        print(f"Source file not found: {path}")
        return

    base_safe = sanitize_custom_id(os.path.splitext(os.path.basename(path))[0])
    with open(path, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()

    total = len(all_lines)
    num_batches = (total + batch_size - 1) // batch_size
    print(f"Dividing '{filename}' into {num_batches} batches of {batch_size} lines each...")

    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, total)
        chunk = all_lines[start:end]
        wrap_for_batch_chunk(chunk, i+1, base_safe)

if __name__ == '__main__':
    process_file('alpaca_data.jsonl', batch_size=500)