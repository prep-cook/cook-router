//! Upstream responses captured live from DeepSeek, an OpenAI-compatible relay and OpenAI,
//! decoded offline through every client egress they once failed on.

// Test-only inclusion until the framework owner declares the production module.
#[path = "../src/anthropic/mod.rs"]
pub mod anthropic;
// The codecs leave some shared helpers unused.
#[allow(dead_code)]
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "../src/gemini/boundary.rs"]
pub mod gemini;
#[path = "../src/responses/mod.rs"]
pub mod responses;
// This target reads client bytes directly, not `Mapped::output`.
#[allow(dead_code)]
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Operation, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use gemini::response::Framing;
use semantic::Mapped;
use serde_json::Value;
use std::task::Poll;

const DS_TEXT: &[u8] = include_bytes!("fixtures/openai-live/ds-chat-text.raw");
const DS_TOOLS: &[u8] = include_bytes!("fixtures/openai-live/ds-chat-tools1.raw");
const DS_THINKING: &[u8] = include_bytes!("fixtures/openai-live/ds-anth-thinking.raw");
const RELAY_CHAT_TOOLS: &[u8] = include_bytes!("fixtures/openai-live/relay-chat-tools2.raw");
const RELAY_RESPONSES_TOOL: &[u8] = include_bytes!("fixtures/openai-live/relay-resp-func1.raw");
const OPENAI_ERROR: &[u8] = include_bytes!("fixtures/openai-live/oai-resp-func1.raw");

#[derive(Clone, Copy)]
enum Upstream {
    Chat,
    Responses,
}

#[derive(Clone, Copy)]
enum Client {
    Chat(BodyMode),
    Anthropic(BodyMode),
    Responses(BodyMode),
    Gemini(Framing),
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Client::Chat(m) => write!(f, "Chat({m:?})"),
            Client::Anthropic(m) => write!(f, "Anthropic({m:?})"),
            Client::Responses(m) => write!(f, "Responses({m:?})"),
            Client::Gemini(Framing::Sse) => f.write_str("Gemini(Sse)"),
            Client::Gemini(_) => f.write_str("Gemini(Json)"),
        }
    }
}

const CLIENTS: [Client; 8] = [
    Client::Chat(BodyMode::Buffered),
    Client::Chat(BodyMode::Streaming),
    Client::Anthropic(BodyMode::Buffered),
    Client::Anthropic(BodyMode::Streaming),
    Client::Responses(BodyMode::Buffered),
    Client::Responses(BodyMode::Streaming),
    Client::Gemini(Framing::Json),
    Client::Gemini(Framing::Sse),
];

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// Feeds `input` in `step`-byte advances through the upstream ingress and `client`'s egress,
/// draining the canonical records after every advance. Returns the client bytes and the first
/// ingress error; the records published before that error still reach the client.
fn decode(upstream: Upstream, input: &[u8], mode: BodyMode, step: usize, client: Client) -> (Vec<u8>, Option<String>) {
    match client {
        Client::Chat(m) => run(upstream, input, mode, step, |w| {
            codec::chat::egress::Egress::new(w, identity(), Operation::Generate, m, true, |_| {})
        }),
        Client::Anthropic(m) => run(upstream, input, mode, step, |w| {
            anthropic::egress::Egress::new(w, identity(), Operation::Generate, m, |_| {})
        }),
        Client::Responses(m) => run(upstream, input, mode, step, |w| {
            responses::egress::Egress::new(w, Operation::Generate, m, |_, _| {})
        }),
        Client::Gemini(f) => run(upstream, input, mode, step, |w| {
            gemini::egress::Egress::with_framing(w, Operation::Generate, f, |_| {})
        }),
    }
}

fn run<E: ResponseEgress>(
    upstream: Upstream,
    input: &[u8],
    mode: BodyMode,
    step: usize,
    make: impl FnOnce(&mut ck_stream::Writer) -> codec::Result<E>,
) -> (Vec<u8>, Option<String>) {
    let mut m = Mapped::new();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut ingress: Box<dyn ResponseIngress> = match upstream {
        Upstream::Chat => Box::new(
            codec::chat::response::Ingress::new(
                &mut m.writer,
                &m.store,
                identity(),
                Operation::Generate,
                mode,
                1 << 22,
            )
            .unwrap(),
        ),
        Upstream::Responses => Box::new(
            responses::response::Ingress::new(&mut m.writer, identity(), Operation::Generate, mode, 1 << 22).unwrap(),
        ),
    };
    let mut egress = make(&mut m.writer).unwrap();
    let mut out = Vec::new();
    let mut failure = None;
    let pieces = input.chunks(step).map(Some).chain([None]);
    for piece in pieces {
        let result = match piece {
            Some(piece) => ingress.advance(&mut m.writer, piece),
            None => ingress.finish(&mut m.writer),
        };
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut m.writer, lease, |part| {
                    out.extend_from_slice(&bytes(&part));
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("{} egress: {e:?}", std::any::type_name::<E>()));
        }
        if let Err(error) = result {
            failure = Some(format!("{error:?}"));
            break;
        }
    }
    egress.finish().unwrap();
    (out, failure)
}

/// Client SSE `data:` payloads as JSON.
fn events(out: &[u8]) -> Vec<Value> {
    std::str::from_utf8(out)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The buffered Responses body, or the response of the stream's terminal event.
fn final_response(out: &[u8], downstream: BodyMode) -> Value {
    match downstream {
        BodyMode::Buffered => serde_json::from_slice(out).unwrap(),
        _ => events(out).last().unwrap()["response"].clone(),
    }
}

/// Every function call part of a Gemini client body, buffered or streamed.
fn gemini_calls(out: &[u8], framing: Framing) -> Vec<Value> {
    let records = match framing {
        Framing::Sse => events(out),
        _ => vec![serde_json::from_slice(out).unwrap()],
    };
    let mut calls = Vec::new();
    for record in records {
        for part in record["candidates"][0]["content"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if !part["functionCall"].is_null() {
                calls.push(part["functionCall"].clone());
            }
        }
    }
    calls
}

#[test]
fn chat_null_logprobs_reach_responses_clients() {
    let source: Value = serde_json::from_slice(DS_TEXT).unwrap();
    assert!(source["choices"][0]["logprobs"].is_null());
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let (out, failure) = decode(
            Upstream::Chat,
            DS_TEXT,
            BodyMode::Buffered,
            61,
            Client::Responses(downstream),
        );
        assert_eq!(failure, None);
        let response = final_response(&out, downstream);
        assert_eq!(response["status"], "completed");
        let message = response["output"].as_array().unwrap().last().unwrap();
        assert_eq!(
            message["content"][0]["text"],
            source["choices"][0]["message"]["content"]
        );
    }
}

#[test]
fn chat_null_content_beside_tool_calls_reaches_responses_clients() {
    // The streamed capture opens with `"content":null`; the buffered one is the capture with
    // the standard OpenAI null content in place of DeepSeek's empty string.
    let buffered = String::from_utf8(DS_THINKING.to_vec())
        .unwrap()
        .replacen("\"content\":\"\"", "\"content\":null", 1);
    assert!(std::str::from_utf8(DS_TOOLS).unwrap().contains("\"content\":null"));
    for (input, mode) in [
        (DS_TOOLS, BodyMode::Streaming),
        (buffered.as_bytes(), BodyMode::Buffered),
    ] {
        for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
            let (out, failure) = decode(Upstream::Chat, input, mode, 61, Client::Responses(downstream));
            assert_eq!(failure, None);
            let response = final_response(&out, downstream);
            let output = response["output"].as_array().unwrap();
            let calls: Vec<_> = output.iter().filter(|item| item["type"] == "function_call").collect();
            assert_eq!(calls.len(), if mode == BodyMode::Streaming { 2 } else { 1 });
            assert!(calls.iter().all(|call| call["name"] == "get_weather"));
            let arguments: Value = serde_json::from_str(calls[0]["arguments"].as_str().unwrap()).unwrap();
            assert_eq!(arguments["city"], "Paris");
        }
    }
}

#[test]
fn chat_and_responses_tool_calls_reach_gemini_clients() {
    for (upstream, input, mode, count) in [
        (Upstream::Chat, DS_TOOLS, BodyMode::Streaming, 2),
        (Upstream::Chat, DS_THINKING, BodyMode::Buffered, 1),
        (Upstream::Responses, RELAY_RESPONSES_TOOL, BodyMode::Streaming, 1),
    ] {
        for framing in [Framing::Json, Framing::Sse] {
            let (out, failure) = decode(upstream, input, mode, 61, Client::Gemini(framing));
            assert_eq!(failure, None);
            let calls = gemini_calls(&out, framing);
            assert_eq!(calls.len(), count, "{}", String::from_utf8_lossy(&out));
            for call in calls {
                assert_eq!(call["name"], "get_weather");
                assert!(call["args"]["city"].is_string());
            }
        }
    }
}

#[test]
fn responses_error_event_ends_every_client_with_an_upstream_failure() {
    assert!(std::str::from_utf8(OPENAI_ERROR).unwrap().contains("event: error"));
    for client in CLIENTS {
        let (out, failure) = decode(Upstream::Responses, OPENAI_ERROR, BodyMode::Streaming, 61, client);
        assert!(failure.is_some(), "{client:?}");
        let text = String::from_utf8_lossy(&out);
        assert!(!text.contains("credit_balance_exhausted"), "{client:?}: {text}");
        if let Client::Responses(downstream) = client {
            assert_eq!(final_response(&out, downstream)["status"], "failed");
        }
    }
}

#[test]
fn blank_lines_after_the_responses_terminal_event_are_ignored() {
    assert!(RELAY_RESPONSES_TOOL.ends_with(b"}\n\n\n"));
    for client in CLIENTS {
        for step in [61, RELAY_RESPONSES_TOOL.len() - 1] {
            let (_, failure) = decode(
                Upstream::Responses,
                RELAY_RESPONSES_TOOL,
                BodyMode::Streaming,
                step,
                client,
            );
            assert_eq!(failure, None, "{client:?} step {step}");
        }
    }
}

#[test]
fn chat_null_tool_calls_decode_like_absent_ones() {
    let source: Value = serde_json::from_slice(RELAY_CHAT_TOOLS).unwrap();
    assert!(source["choices"][0]["message"]["tool_calls"].is_null());
    for client in CLIENTS {
        let (out, failure) = decode(Upstream::Chat, RELAY_CHAT_TOOLS, BodyMode::Buffered, 61, client);
        assert_eq!(failure, None, "{client:?}");
        assert!(String::from_utf8_lossy(&out).contains("Tokyo"), "{client:?}");
    }
}
