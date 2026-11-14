//! Regressions from real upstream captures (2026-11-15 live tests). Each fixture under
//! `fixtures/live/` is a recorded response body; the Responses stream drops one trailing blank line.

// Test-only inclusion until the framework owner declares the production modules.
#[path = "../src/anthropic/mod.rs"]
pub mod anthropic;
#[allow(dead_code)]
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[allow(dead_code)]
#[path = "../src/gemini/boundary.rs"]
pub mod gemini;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[allow(dead_code)]
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Operation, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};
use std::task::Poll;

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("{}/tests/fixtures/live/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

/// Upstream protocol of a fixture.
#[derive(Clone, Copy)]
enum Source {
    Anthropic,
    Chat,
    Responses,
}

/// Client egress under test.
#[derive(Clone, Copy)]
enum Client {
    Anthropic,
    Chat,
    Gemini,
}

/// Feeds an ingress one fragment, or finishes it on `None`.
type Feed = Box<dyn FnMut(&mut ck_stream::Writer, Option<&[u8]>)>;

/// Translates `input` from `source` to `client` in 13-byte fragments and returns the client bytes.
fn run(source: Source, upstream: BodyMode, input: &[u8], client: Client, mode: BodyMode) -> Vec<u8> {
    let mut m = Mapped::new();
    let (op, limit) = (Operation::Generate, 1 << 20);
    let mut ingress: Feed = match source {
        Source::Anthropic => {
            let mut i = anthropic::response::Ingress::new(&mut m.writer, identity(), op, upstream, limit).unwrap();
            Box::new(move |w, chunk| match chunk {
                Some(chunk) => i.advance(w, chunk).unwrap(),
                None => i.finish(w).unwrap(),
            })
        }
        Source::Chat => {
            let mut i =
                codec::chat::response::Ingress::new(&mut m.writer, &m.store, identity(), op, upstream, limit).unwrap();
            Box::new(move |w, chunk| match chunk {
                Some(chunk) => i.advance(w, chunk).unwrap(),
                None => i.finish(w).unwrap(),
            })
        }
        Source::Responses => {
            let mut i = responses::response::Ingress::new(&mut m.writer, identity(), op, upstream, limit).unwrap();
            Box::new(move |w, chunk| match chunk {
                Some(chunk) => i.advance(w, chunk).unwrap(),
                None => i.finish(w).unwrap(),
            })
        }
    };
    let mut egress: Box<dyn Egress> = match client {
        Client::Anthropic => Box::new(anthropic::egress::Egress::new(&mut m.writer, identity(), op, mode, |_| {}).unwrap()),
        Client::Chat => {
            Box::new(codec::chat::egress::Egress::new(&mut m.writer, identity(), op, mode, true, |_| {}).unwrap())
        }
        Client::Gemini => Box::new(gemini::egress::Egress::new(&mut m.writer, identity(), op, mode, |_| {}).unwrap()),
    };
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut output = Vec::new();
    for chunk in input.chunks(13).map(Some).chain([None]) {
        ingress(&mut m.writer, chunk);
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress.advance(&mut m.writer, lease, &mut output);
        }
    }
    egress.finish();
    output
}

/// Object-safe view of a response egress that appends its output to a buffer.
trait Egress {
    fn advance(&mut self, writer: &mut ck_stream::Writer, lease: ck_stream::Lease, output: &mut Vec<u8>);
    fn finish(&mut self);
}
impl<E: ResponseEgress> Egress for E {
    fn advance(&mut self, writer: &mut ck_stream::Writer, lease: ck_stream::Lease, output: &mut Vec<u8>) {
        ResponseEgress::advance(self, writer, lease, |part| {
            let start = output.len();
            while output.len() - start < part.len() {
                output.extend_from_slice(part.chunk(output.len() - start).unwrap());
            }
            Ok(())
        })
        .unwrap();
    }
    fn finish(&mut self) {
        ResponseEgress::finish(self).unwrap();
    }
}

/// The JSON payloads of an SSE body.
fn events(raw: &[u8]) -> Vec<Value> {
    std::str::from_utf8(raw)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: ").or_else(|| line.strip_prefix("data:")))
        .filter_map(|line| serde_json::from_str(line.trim()).ok())
        .collect()
}

/// The message an official SDK accumulates from a Messages stream: deltas append text, thinking
/// and tool JSON, and a `signature_delta` replaces the signature.
fn accumulate(raw: &[u8]) -> Value {
    let mut message = Value::Null;
    let mut json = Vec::new();
    for event in events(raw) {
        match event["type"].as_str().unwrap() {
            "message_start" => message = event["message"].clone(),
            "content_block_start" => {
                message["content"].as_array_mut().unwrap().push(event["content_block"].clone());
                json.push(String::new());
            }
            "content_block_delta" => {
                let index = event["index"].as_u64().unwrap() as usize;
                let block = &mut message["content"][index];
                let delta = &event["delta"];
                match delta["type"].as_str().unwrap() {
                    "signature_delta" => block["signature"] = delta["signature"].clone(),
                    "input_json_delta" => json[index].push_str(delta["partial_json"].as_str().unwrap()),
                    kind => {
                        let field = if kind == "text_delta" { "text" } else { "thinking" };
                        let text = block[field].as_str().unwrap_or_default().to_owned();
                        block[field] = json!(text + delta[field].as_str().unwrap());
                    }
                }
            }
            "content_block_stop" => {
                let index = event["index"].as_u64().unwrap() as usize;
                if !json[index].is_empty() {
                    message["content"][index]["input"] = serde_json::from_str(&json[index]).unwrap();
                }
            }
            _ => {}
        }
    }
    message
}

#[test]
fn streamed_tool_use_with_an_empty_first_fragment_translates() {
    for name in ["a2_tools_t1_stream.sse", "b3_think_stream.sse"] {
        let input = fixture(name);
        let buffered = run(Source::Anthropic, BodyMode::Streaming, &input, Client::Anthropic, BodyMode::Buffered);
        let text = std::str::from_utf8(&buffered).unwrap();
        assert_eq!(text.matches("\"input\"").count(), 1, "{name}: {text}");
        let buffered: Value = serde_json::from_slice(&buffered).unwrap();
        let streamed = accumulate(&input);
        assert_eq!(buffered["content"], streamed["content"], "{name}");
        let tool = streamed["content"].as_array().unwrap().last().unwrap();
        assert_eq!(tool["type"], "tool_use");
        assert!(tool["input"].as_object().is_some_and(|input| !input.is_empty()));
    }
}

#[test]
fn streamed_replay_keeps_the_signature_and_sends_no_empty_delta() {
    for name in ["a2_tools_t1_stream.sse", "b3_think_stream.sse"] {
        let input = fixture(name);
        let output = run(Source::Anthropic, BodyMode::Streaming, &input, Client::Anthropic, BodyMode::Streaming);
        assert_eq!(accumulate(&output)["content"], accumulate(&input)["content"], "{name}");
        for event in events(&output) {
            for field in ["text", "thinking", "signature", "partial_json"] {
                assert_ne!(event["delta"][field], "", "{name}: {event}");
            }
            if event["content_block"]["type"] == "tool_use" {
                assert_eq!(event["content_block"]["input"], json!({}), "{name}");
            }
        }
    }
}

#[test]
fn buffered_body_from_a_stream_writes_finish_members_once() {
    let output = run(
        Source::Anthropic,
        BodyMode::Streaming,
        &fixture("a1b_chat_stream.sse"),
        Client::Anthropic,
        BodyMode::Buffered,
    );
    let text = std::str::from_utf8(&output).unwrap();
    for key in ["\"container\"", "\"stop_details\""] {
        assert_eq!(text.matches(key).count(), 1, "{text}");
    }
    // The message_delta value replaces the message_start one, also after a leading ping.
    let rows = [
        json!({"type":"ping"}),
        json!({"type":"message_start","message":{"id":"m","model":"c","role":"assistant","content":[],"stop_details":null,"usage":{"input_tokens":1,"output_tokens":0}}}),
        json!({"type":"message_delta","delta":{"stop_reason":"refusal","stop_details":{"type":"refusal"}},"usage":{"output_tokens":0}}),
        json!({"type":"message_stop"}),
    ];
    let input: String = rows.iter().map(|r| format!("event: {}\ndata: {r}\n\n", r["type"].as_str().unwrap())).collect();
    let output = run(Source::Anthropic, BodyMode::Streaming, input.as_bytes(), Client::Anthropic, BodyMode::Buffered);
    let text = std::str::from_utf8(&output).unwrap();
    assert_eq!(text.matches("\"stop_details\"").count(), 1, "{text}");
    assert_eq!(serde_json::from_str::<Value>(text).unwrap()["stop_details"], json!({"type":"refusal"}));
}

#[test]
fn chat_message_from_a_buffered_anthropic_body_has_a_role() {
    let input = fixture("a1b_chat_nonstream.json");
    let buffered = run(Source::Anthropic, BodyMode::Buffered, &input, Client::Chat, BodyMode::Buffered);
    let buffered: Value = serde_json::from_slice(&buffered).unwrap();
    assert_eq!(buffered["choices"][0]["message"]["role"], "assistant", "{buffered}");
    let streamed = run(Source::Anthropic, BodyMode::Buffered, &input, Client::Chat, BodyMode::Streaming);
    let first = &events(&streamed)[0];
    assert_eq!(first["choices"][0]["delta"]["role"], "assistant", "{first}");
}

