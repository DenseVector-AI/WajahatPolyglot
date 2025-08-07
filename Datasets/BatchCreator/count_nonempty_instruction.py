import json

input_path = 'BatchCreator/alpaca_data.jsonl'
nonempty_count = 0
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f:
        try:
            record = json.loads(line)
            if 'instruction' in record and record['instruction'].strip() != '':
                nonempty_count += 1
        except Exception:
            continue
print(f"Non-empty 'instruction' field count: {nonempty_count}") 