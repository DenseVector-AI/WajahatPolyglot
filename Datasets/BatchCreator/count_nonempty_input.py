from asyncio.windows_events import NULL
import json

input_path = 'BatchCreator/alpaca_data.jsonl'
nonempty_count = 0
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f:
        try:
            record = json.loads(line)
            if 'instruction' in record and record['instruction'].strip() is not NULL:
                nonempty_count += 1
        except Exception:
            continue
print(f"Non-empty 'input' field count: {nonempty_count}") 