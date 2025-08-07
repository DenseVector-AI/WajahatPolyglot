import os
import json
import sys

# === Configuration ===
# Base directory where the script resides
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Name of the source JSONL file (must remain unchanged)
TARGET_FILE = "alpaca_data.jsonl"
# Number of lines per batch
BATCH_SIZE = 500

# Input directory (adjust if you place the source file in a subfolder)
# If your JSONL file is in the same folder as this script, keep as BASE_DIR
INPUT_DIR = BASE_DIR

# Output directory for prepared batch files
BATCH_DIR = BASE_DIR
PREPARED_DIR = os.path.join(BATCH_DIR, "batch_all_openai")
# Ensure the output directory exists
os.makedirs(PREPARED_DIR, exist_ok=True)


def sanitize_custom_id(text: str) -> str:
    """
    Replace non-alphanumeric characters with underscores to create safe custom IDs.
    """
    return ''.join(c if c.isalnum() or c in '-_.' else '_' for c in text)


def wrap_for_batch_chunk(
    lines: list,
    output_path: str,
    base_name: str,
    start_index: int
) -> None:
    """
    Convert a chunk of JSONL lines into an OpenAI JSONL payload file using the 'input' field.
    """
    # Base prompt template for translation
    prompt_template = (
        """
        # Role and Objective

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
        """
    )


    with open(output_path, 'w', encoding='utf-8') as fout:
        count = 0
        for idx, raw in enumerate(lines):
            global_idx = start_index + idx
            line = raw.strip()
            if not line:
                full_prompt = prompt_template.format(CONTENT="")
            else:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                content = record.get('input', '')
                full_prompt = prompt_template.format(CONTENT=content)

            payload = {
                "custom_id": f"{base_name}_{global_idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4.1-2025-04-14",
                    "messages": [
                        {"role": "system", "content": "You are a translator from English to Roman Urdu."},
                        {"role": "user", "content": full_prompt}
                    ],
                    "max_tokens": 2048,
                    "temperature": 0.2
                }
            }
            fout.write(json.dumps(payload, ensure_ascii=False) + '\n')
            count += 1
    print(f"✅ Prepared {os.path.basename(output_path)} with {count} entries.")


def process_in_batches(filename: str):
    """
    Read the source JSONL, split into batches, and create OpenAI payload files.
    """
    source_path = os.path.join(INPUT_DIR, filename)
    if not os.path.isfile(source_path):
        print(f"Error: File not found at {source_path}")
        sys.exit(1)

    with open(source_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    total = len(lines)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    base_name = sanitize_custom_id(os.path.splitext(filename)[0])

    print(f"Total lines: {total}. Splitting into {batches} batches of up to {BATCH_SIZE} each.")
    for i in range(batches):
        start = i * BATCH_SIZE
        end = min(start + BATCH_SIZE, total)
        chunk = lines[start:end]
        out_name = f"{base_name}_batch_{i+1}_prepared.jsonl"
        out_path = os.path.join(PREPARED_DIR, out_name)
        print(f"Processing batch {i+1}/{batches}: lines {start}–{end-1}")
        wrap_for_batch_chunk(chunk, out_path, base_name, start)

    print(f"All batches written to {PREPARED_DIR}")


if __name__ == '__main__':
    print(f"Starting batch prep for {TARGET_FILE}...")
    process_in_batches(TARGET_FILE)
