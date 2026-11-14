Fixed

- Chat Completions responses with `"logprobs": null`, or `"content": null` beside tool calls, now reach Responses clients instead of failing. DeepSeek sends these on every response.
- Chat Completions responses with `"tool_calls": null`, which some OpenAI-compatible relays send, now translate for every client instead of failing.
- A Responses stream with extra blank lines after its final event no longer fails.
- Tool calls from a Chat Completions or Responses upstream now reach Gemini clients as `functionCall` parts that carry the upstream call id, instead of failing.
