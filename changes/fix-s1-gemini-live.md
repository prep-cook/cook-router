Fixed

- Gemini accepts an OpenAI assistant message with tool calls and `"content": null`, so multi-turn tool conversations from Chat clients are no longer refused.
- An Anthropic `tool_result` reaches Gemini as a `functionResponse` carrying its text; a tool result that contains an image is refused as unsupported.
- Anthropic and Chat thinking settings no longer leak their own fields into Gemini's `thinkingConfig`, which Gemini rejected with 400; a thinking budget maps to `thinkingBudget`, and disabled thinking maps to `thinkingBudget: 0`.
