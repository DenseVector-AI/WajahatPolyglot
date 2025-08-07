import os
import json
import csv
import re
import random

BATCHES_DIR = 'batches'
OUTPUT_JSONL = 'merged_output.jsonl'
OUTPUT_CSV = 'merged_output.csv'
INSTRUCTION_SUFFIX = ' Is ka jawab Roman Urdu mein dijiye.'

# Helper to extract fields from various formats
def extract_fields(content):
    # Try to parse as JSON
    try:
        data = json.loads(content)
        if all(k in data for k in ['instruction', 'input', 'output']):
            return data['instruction'], data['input'], data['output']
    except Exception:
        pass
    # Try to extract from custom format
    instr = re.search(r'\*\*Instruction_Field:\*\* ?(.+?)(?:\\n|\n|$)', content)
    inp = re.search(r'\*\*Input_Field:\*\* ?(.+?)(?:\\n|\n|$)', content)
    out = re.search(r'\*\*Output_Field:\*\* ?(.+?)(?:\\n|\n|$)', content)
    if instr and out:
        return instr.group(1).strip(), inp.group(1).strip() if inp else '', out.group(1).strip()
    return None, None, None

def merge_batches():
    merged = []
    for fname in os.listdir(BATCHES_DIR):
        if fname.endswith('.jsonl'):
            with open(os.path.join(BATCHES_DIR, fname), 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        # Try OpenAI/Alpaca format
                        content = None
                        if 'response' in obj and 'body' in obj['response']:
                            choices = obj['response']['body'].get('choices', [])
                            if choices:
                                content = choices[0]['message']['content']
                        if not content:
                            continue
                        instruction, input_, output = extract_fields(content)
                        if instruction is not None and output is not None:
                            # Append the required sentence to instruction
                            instruction = instruction.strip() + INSTRUCTION_SUFFIX
                            merged.append({
                                'instruction': instruction,
                                'input': input_,
                                'output': output
                            })
                    except Exception:
                        continue
    # Shuffle the merged dataset
    random.shuffle(merged)
    # Write JSONL
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for item in merged:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    # Write CSV
    with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['instruction', 'input', 'output'])
        writer.writeheader()
        for item in merged:
            writer.writerow(item)
if __name__ == '__main__':
    merge_batches() 