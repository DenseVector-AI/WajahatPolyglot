import os
import json

SRC_DIR = '../Batches250Size'
DST_DIR = '.'  # This script is in Batches250Size_translated

# Placeholder translation function (replace with actual translation logic or API)
def translate_to_roman_urdu(text):
    if not text:
        return text
    return f'[Roman Urdu] {text}'

def process_file(filename):
    src_path = os.path.join(SRC_DIR, filename)
    dst_filename = filename.replace('.jsonl', '_translated.jsonl')
    dst_path = os.path.join(DST_DIR, dst_filename)
    with open(src_path, 'r', encoding='utf-8') as src_file, open(dst_path, 'w', encoding='utf-8') as dst_file:
        for line in src_file:
            try:
                data = json.loads(line)
                body = data.get('body', {})
                messages = body.get('messages', [])
                for msg in messages:
                    if msg.get('role') == 'user':
                        content = msg.get('content', '')
                        # Simple field extraction and translation
                        for field in ['**Instruction_Field:**', '**Input_Field:**', '**Output_Field:**']:
                            if field in content:
                                parts = content.split(field)
                                before = parts[0]
                                after = parts[1]
                                # Find the next field or end
                                next_field = next((f for f in ['**Instruction_Field:**', '**Input_Field:**', '**Output_Field:**'] if f in after and f != field), None)
                                if next_field:
                                    value = after.split(next_field)[0].strip()
                                    rest = next_field + after.split(next_field, 1)[1]
                                else:
                                    value = after.strip()
                                    rest = ''
                                translated = translate_to_roman_urdu(value)
                                content = before + field + ' ' + translated + (' ' + rest if rest else '')
                        msg['content'] = content
                data['body']['messages'] = messages
                dst_file.write(json.dumps(data, ensure_ascii=False) + '\n')
            except Exception as e:
                print(f'Error processing line: {e}')
                dst_file.write(line)

def main():
    for filename in os.listdir(SRC_DIR):
        if filename.endswith('.jsonl'):
            print(f'Processing {filename}...')
            process_file(filename)
    print('Done.')

if __name__ == '__main__':
    main() 