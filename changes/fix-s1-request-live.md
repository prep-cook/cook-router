Fixed

- Responses requests with `instructions` or `max_output_tokens`, and Chat requests with `max_completion_tokens` or a `developer` message, reach Anthropic upstreams instead of being refused. The instructions become the system prompt and the limit becomes `max_tokens`.
- Gemini requests whose `generationConfig` holds only mapped controls, such as `temperature`, `maxOutputTokens` and `stopSequences`, reach Chat and Anthropic upstreams instead of being refused.
- Anthropic requests with `tool_result` blocks reach Chat upstreams: each result becomes a `tool` message with its text, ahead of the rest of the turn. Chat has no error flag, so `is_error` is dropped and the text is kept. A result containing an image is still refused.
- Anthropic requests with a `thinking` config no longer send an unknown `reasoning` object to Chat upstreams. The effort still becomes `reasoning_effort`.
- A Chat `reasoning` object's `effort` now sets Gemini's `thinkingLevel`, as `reasoning_effort` does, instead of sending an empty `thinkingConfig`.
