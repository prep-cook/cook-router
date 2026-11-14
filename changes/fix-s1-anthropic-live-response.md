Fixed

- Streamed Anthropic responses that call a tool no longer fail translation; the API's empty first tool input fragment is accepted.
- Anthropic streaming clients keep the thinking signature: the stream sends no empty `signature_delta`, so a conversation with extended thinking can continue. The stream also sends no other empty deltas, and a `tool_use` block starts with `"input":{}`.
- Non-streaming Anthropic bodies built from a stream write `container` and `stop_details` once, with the final value.
- Chat responses translated from a non-streaming Anthropic response include `role: "assistant"`.
- Chat `usage` from an Anthropic, Responses or Gemini upstream uses Chat count names, including `prompt_tokens_details.cached_tokens` and `completion_tokens_details.reasoning_tokens`; Anthropic thinking tokens count as reasoning tokens.
- Gemini `usageMetadata` reports `candidatesTokenCount` when the upstream reports no reasoning count.
- Anthropic `usage.input_tokens` is present for upstreams that report no cache writes, such as OpenAI-compatible Chat APIs.
- A Responses reasoning summary reaches Anthropic clients as a `thinking` block instead of an undefined `opaque` block.
