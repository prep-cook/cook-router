Changed

- Chat requests accept a `tool_choice` of type `allowed_tools` and forward it to Chat upstreams; Anthropic and Responses upstreams refuse it with 400.
- Anthropic requests with `output_config.effort` reach Chat upstreams as `reasoning_effort` instead of failing with 400.
- Chat requests without `max_tokens` reach Anthropic upstreams when the destination supplies a default: the model's configured value, else its output maximum. Without a default they still fail with 400; an explicit `max_tokens` is sent unchanged.
- Claude thinking history sent to a Chat upstream loses its signatures and redacted blocks, and its visible thinking travels as `reasoning_content`, instead of failing with 400. Anthropic upstreams still receive the history unchanged.
