import anthropic

client = anthropic.Anthropic(api_key="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")

try:
    message = client.messages.create(
        model="claude-3-sonnet-20240229",
        max_tokens=10,
        messages=[{"role": "user", "content": "Hello"}]
    )
    print("API key is valid!")
    print(message.content)
except anthropic.APIError as e:
    print(f"API Error: {e}")