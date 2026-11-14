//! Request policy between Chat and Anthropic: owner rulings of 2026-11-15 and the B1/B4 fixes.
use crate::anthropic::request::AnthropicCapability;
use crate::codec::event::Direction;
use crate::{Mapped, Operation, RequestEgress, RequestIngress, Value, anthropic, bytes, codec, identity, json};

/// Encodes a Chat request for a Chat upstream, or for an Anthropic one with `anthropic`'s
/// capability; returns the fence error.
fn from_chat(input: &Value, anthropic: Option<AnthropicCapability>) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, &serde_json::to_vec(input).unwrap())
        .unwrap();
    let output = match anthropic {
        None => {
            let ready = ingress.finish(&mut mapped.writer, &codec::chat::request::ChatCapability)?;
            codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&ready, &mut mapped.writer)
        }
        Some(capability) => {
            let ready = ingress.finish(&mut mapped.writer, &capability)?;
            anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
                default_max_tokens: capability.default_max_tokens,
            }
            .encode(&ready, &mut mapped.writer)
        }
    }
    .unwrap();
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}

/// Encodes an Anthropic request for a Chat upstream, or returns the fence error.
fn anthropic_to_chat(input: &Value) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    for chunk in serde_json::to_vec(input).unwrap().chunks(5) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let ready = ingress.finish(&mut mapped.writer, &codec::chat::request::ChatCapability)?;
    let output = codec::chat::request::Egress {
        destination: "chat",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}

fn allowed_tools(name: &str) -> Value {
    let tool = |name: &str| json!({"type":"function","function":{"name":name,"parameters":{"type":"object"}}});
    json!({
        "model":"m",
        "max_tokens":10,
        "messages":[{"role":"user","content":"q"}],
        "tools":[tool("f"),tool("g")],
        "tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":name}}]}},
    })
}

#[test]
fn allowed_tools_choice_reaches_chat_and_is_refused_where_inexpressible() {
    let input = allowed_tools("f");
    let output = from_chat(&input, None).unwrap();
    assert_eq!(output["tool_choice"], input["tool_choice"]);
    assert!(matches!(
        from_chat(&input, Some(AnthropicCapability::default())),
        Err(codec::Error::Unsupported)
    ));
    assert!(matches!(
        from_chat(&allowed_tools("undeclared"), None),
        Err(codec::Error::Schema)
    ));
}

#[test]
fn output_config_effort_reaches_chat_as_reasoning_effort() {
    let input = json!({"model":"m","max_tokens":10,"output_config":{"effort":"high"},"messages":[{"role":"user","content":"q"}]});
    let output = anthropic_to_chat(&input).unwrap();
    assert_eq!(output["reasoning_effort"], "high");
    assert!(output.get("output_config").is_none());
}

#[test]
fn signed_thinking_history_reaches_chat_as_visible_reasoning_only() {
    let input = json!({"model":"m","max_tokens":10,"system":"s","tools":[{"name":"f","input_schema":{"type":"object"}}],"messages":[
        {"role":"user","content":"q"},
        {"role":"assistant","content":[
            {"type":"thinking","thinking":"first","signature":"sig-1"},
            {"type":"tool_use","id":"c","name":"f","input":{}},
            {"type":"redacted_thinking","data":"opaque-1"},
        ]},
        {"role":"user","content":"continue"},
        {"role":"assistant","content":[
            {"type":"thinking","thinking":"second","signature":"sig-2"},
            {"type":"redacted_thinking","data":"opaque-2"},
            {"type":"thinking","thinking":"third","signature":"sig-3"},
            {"type":"text","text":"answer"},
        ]},
        {"role":"user","content":"next"},
    ]});
    let output = anthropic_to_chat(&input).unwrap();
    assert_eq!(output["messages"][2]["reasoning_content"], "first");
    assert_eq!(output["messages"][2]["tool_calls"][0]["id"], "c");
    // Chat requires a nonempty content array; thinking plus tool_use leaves none.
    assert_eq!(output["messages"][2]["content"], "");
    assert_eq!(
        output["messages"][4],
        json!({"role":"assistant","content":[{"type":"text","text":"answer"}],"reasoning_content":"second\nthird"})
    );
    let raw = output.to_string();
    for dropped in ["sig-", "opaque-", "signature", "redacted"] {
        assert!(!raw.contains(dropped), "{dropped} reached Chat: {raw}");
    }
}

#[test]
fn chat_max_tokens_reaches_anthropic_from_the_request_or_the_capability_default() {
    let input = |max_tokens: Option<u64>| {
        let mut body = json!({"model":"m","messages":[{"role":"user","content":"q"}]});
        if let Some(max_tokens) = max_tokens {
            body["max_tokens"] = json!(max_tokens);
        }
        body
    };
    let default = |default_max_tokens| Some(AnthropicCapability { default_max_tokens });
    assert_eq!(
        from_chat(&input(None), default(Some(4096))).unwrap()["max_tokens"],
        4096
    );
    assert!(matches!(
        from_chat(&input(None), default(None)),
        Err(codec::Error::Schema)
    ));
    assert!(matches!(
        from_chat(&input(None), default(Some(0))),
        Err(codec::Error::Schema)
    ));
    assert_eq!(
        from_chat(&input(Some(7)), default(Some(4096))).unwrap()["max_tokens"],
        7
    );
    assert_eq!(from_chat(&input(Some(7)), default(None)).unwrap()["max_tokens"], 7);
}

#[test]
fn claude_history_left_without_parts_reaches_chat_with_empty_text() {
    let turn = |blocks: Value| {
        json!({"model":"m","max_tokens":10,"tools":[{"name":"f","input_schema":{"type":"object"}}],"messages":[
            {"role":"user","content":"q"},
            {"role":"assistant","content":blocks},
            {"role":"user","content":"next"},
        ]})
    };
    let redacted = anthropic_to_chat(&turn(json!([{"type":"redacted_thinking","data":"opaque"}]))).unwrap();
    assert_eq!(redacted["messages"][1], json!({"role":"assistant","content":""}));
    let tool = anthropic_to_chat(&turn(json!([{"type":"tool_use","id":"c","name":"f","input":{}}]))).unwrap();
    assert_eq!(tool["messages"][1]["content"], "");
    assert_eq!(tool["messages"][1]["tool_calls"][0]["id"], "c");
}
