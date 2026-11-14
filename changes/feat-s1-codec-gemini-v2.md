Added

- Add the Gemini generateContent and streamGenerateContent boundary codec: requests and responses in JSON, SSE and JSON-array framings map to and from the internal stream, keeping part order, source role spelling, signatures, provided call IDs, native controls, SSE fields, events with unknown names and unknown fields at every depth.
- Map Gemini media to mapped media records with exact decoded bytes, MIME types and video offsets as media timing, and publish unfinished request strings and media before the body ends.
- Map Gemini finish reasons and `finishMessage` in both directions, report output and per-modality usage counts, take the creation time from `createTime`, and carry a response identity that arrives after the first chunk, even in a body without candidates.
- Send streamed Gemini responses as Gemini does: every chunk carries content, and the last one carries the finish and usage, so SDK chat history keeps the turn.
- Send Chat tool results to Gemini as function responses linked to their calls, map `max_completion_tokens`, JSON mode and a single `stop` string, and give Gemini calls without an ID a Chat call ID.
- Refuse another protocol's opaque history or signatures, a `system` role inside `contents`, a tool result that answers no preceding call, an unknown `response_format` type and a Chat `allowed_tools` tool choice at the Gemini request fence, and keep other protocols' unknown fields out of Gemini requests.
- Fail a Gemini response part that carries media by URI (`fileData`) at Chat, Anthropic and Responses clients, which cannot represent it, and fail Chat output when a provider call ID equals the ID given to a Gemini call without one.
