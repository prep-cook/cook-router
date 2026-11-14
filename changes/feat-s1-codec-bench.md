Internal

- `bench/codec/build-new.sh` builds the new boundary-codec harness for `bench/codec-gate.sh`. It covers Chat, Anthropic, Responses and Gemini, classifies each fixture case's outcome (ok, invalid input, unsupported, other error), and times only successful cases with `partial.py --ok-only`. `ck-stream` gains a `bench-hooks` feature that exposes per-stream metadata bytes to the harness.
