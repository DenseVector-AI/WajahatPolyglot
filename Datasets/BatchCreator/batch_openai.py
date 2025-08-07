import os
import json
import sys

# === Configuration ===
# Adjust these to match your environment
# Use the directory where this script resides as the base
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
target_file = "alpaca_data.jsonl"  # source filename located in SCRIPT_DIR
batch_size = 500  # number of lines per batch

# Directories
BATCH_DIR = SCRIPT_DIR
PREPARED_DIR = os.path.join(BATCH_DIR, "batch_all_openai")
# Ensure output directory exists
os.makedirs(PREPARED_DIR, exist_ok=True)


def sanitize_custom_id(text: str) -> str:
    """
    Replace non-alphanumeric chars with underscores for safe IDs.
    """
    return ''.join(c if c.isalnum() or c in '-_.' else '_' for c in text)


def wrap_for_batch_chunk(
    lines_chunk: list,
    output_file_path: str,
    sanitized_source_name: str,
    start_idx: int = 0
) -> None:
    """
    Process a set of lines and write an OpenAI JSONL payload file.
    """
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

    with open(output_file_path, 'w', encoding='utf-8') as fout:
        written = 0
        for local_idx, raw_line in enumerate(lines_chunk):
            global_idx = start_idx + local_idx
            line = raw_line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            instr = record.get('instruction', '').strip()
            outp  = record.get('output', '').strip()
            if not instr or not outp:
                continue

            # Build the prompt
            content_block = f"**Text** {instr}\n"
            full_prompt = prompt_template.replace('{CONTENT}', content_block)

            # Build the OpenAI request payload
            request_record = {
                "custom_id": f"{sanitized_source_name}_line_{global_idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4.1-2025-04-14",
                    "messages": [
                        {"role": "system", "content": "You are a translator that converts Urdu to Roman Urdu."},
                        {"role": "user",   "content": full_prompt}
                    ],
                    "max_tokens": 6805,
                    "temperature": 0.2
                }
            }
            fout.write(json.dumps(request_record, ensure_ascii=False) + '\n')
            written += 1
    print(f"✅ Prepared: {os.path.basename(output_file_path)} ({written} records)")


def process_file_in_batches(input_filename: str, batch_size: int) -> None:
    """
    Read JSONL, split into batches, and write OpenAI JSONL files.
    """
    source_path = os.path.join(BATCH_DIR, input_filename)
    print(f"Source path: {source_path}")
    if not os.path.isfile(source_path):
        print(f"Error: Source file not found: {source_path}")
        sys.exit(1)

    base = os.path.splitext(os.path.basename(input_filename))[0]
    sanitized_name = sanitize_custom_id(base)

    with open(source_path, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()

    total_lines = len(all_lines)
    num_batches = (total_lines + batch_size - 1) // batch_size
    print(f"Total lines: {total_lines}. Dividing into {num_batches} batches of up to {batch_size} lines each.")

    for idx in range(num_batches):
        start = idx * batch_size
        end   = min(start + batch_size, total_lines)
        chunk = all_lines[start:end]

        output_fname = f"{sanitized_name}_batch_{idx+1}_prepared.jsonl"
        output_path = os.path.join(PREPARED_DIR, output_fname)

        print(f"Processing batch {idx+1}/{num_batches}: lines {start}–{end-1}" )
        wrap_for_batch_chunk(
            lines_chunk=chunk,
            output_file_path=output_path,
            sanitized_source_name=sanitized_name,
            start_idx=start
        )

    print(f"All done! Batches written to: {PREPARED_DIR}")


if __name__ == '__main__':
    print(f"Starting batch preparation for '{target_file}' in {BATCH_DIR} with batch size {batch_size}")
    process_file_in_batches(target_file, batch_size)