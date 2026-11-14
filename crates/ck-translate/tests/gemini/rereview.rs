//! Regressions from the second review of the Gemini codec, one input per finding.
use super::*;
use gemini::response::Framing;

/// Candidate metadata that repeats without a new part travels with the held part, so the
/// last streamed record still carries content (the SDK keeps the turn in chat history).
#[test]
fn repeated_candidate_metadata_keeps_the_last_part() {
    let rows = [
        json!({"candidates":[{"content":{"parts":[{"text":"hi"}]},"safetyRatings":[]}]}),
        json!({"candidates":[{"safetyRatings":[{"category":"x"}],"finishReason":"STOP"}]}),
    ];
    let (out, _) = response(&sse(&rows), Framing::Sse, Framing::Sse, Operation::Generate);
    let rows = events(&out);
    assert_eq!(
        rows,
        vec![
            json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hi"}]},"safetyRatings":[{"category":"x"}],"finishReason":"STOP"}]})
        ]
    );
}

/// A long Chat tool result that ingress keeps as mapped UTF-8 media reaches Gemini as the
/// function response text.
#[test]
fn long_chat_tool_result_reaches_gemini() {
    let text = "x".repeat(40_000);
    let input = json!({"model":"m","messages":[
        {"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"f","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"a","content":text}
    ]});
    let out = super::regressions::chat_to_gemini(&serde_json::to_vec(&input).unwrap());
    assert_eq!(
        out["contents"][1]["parts"][0]["functionResponse"],
        json!({"name":"f","id":"a","response":{"result":text}})
    );
}

/// A native call id equal to the canonical id of an id-less call in the same candidate fails
/// Chat egress visibly rather than giving two calls one id.
#[test]
fn native_and_canonical_call_ids_never_collide_in_chat() {
    use super::final_review::gemini_to_chat;
    let body = |parts: Value| json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]});
    let alone = gemini_to_chat(&body(json!([{"text":"x"},{"functionCall":{"name":"g","args":{}}}]))).unwrap();
    let canonical = alone["choices"][0]["message"]["tool_calls"][0]["id"].clone();
    for parts in [
        json!([{"functionCall":{"id":canonical,"name":"f","args":{}}},{"functionCall":{"name":"g","args":{}}}]),
        json!([{"text":"x"},{"functionCall":{"name":"g","args":{}}},{"functionCall":{"id":canonical,"name":"f","args":{}}}]),
    ] {
        let result = gemini_to_chat(&body(parts));
        assert!(matches!(result, Err(codec::Error::Unsupported)), "{result:?}");
    }
    let distinct = json!([{"functionCall":{"id":"a","name":"f","args":{}}},{"functionCall":{"name":"g","args":{}}}]);
    let out = gemini_to_chat(&body(distinct)).unwrap();
    let calls = &out["choices"][0]["message"]["tool_calls"];
    assert_eq!(calls[0]["id"], "a");
    assert_ne!(calls[1]["id"], "a");
}

/// Held parts are written before an event with an unknown name, and SSE fields open the
/// event of the data they came with, so streamed output keeps the source order.
#[test]
fn held_parts_keep_their_place_around_native_events() {
    let text = |t: &str| json!({"candidates":[{"content":{"parts":[{"text":t}]}}]});
    let mut wire = sse(&[text("before")]);
    wire.extend(b"event: future_event\ndata: marker\n\n");
    wire.extend(sse(&[
        json!({"candidates":[{"content":{"parts":[{"text":"after"}]},"finishReason":"STOP"}]}),
    ]));
    let (out, _) = response(&wire, Framing::Sse, Framing::Sse, Operation::Generate);
    let raw = String::from_utf8(out).unwrap();
    let at = |needle: &str| raw.find(needle).unwrap_or_else(|| panic!("{needle} missing: {raw}"));
    assert!(at("\"before\"") < at("event: future_event\ndata: marker\n\n"), "{raw}");
    assert!(at("data: marker") < at("\"after\""), "{raw}");

    let body = json!({"candidates":[{"content":{"parts":[{"text":"a"},{"text":"b"}]},"finishReason":"STOP"}]});
    let wire = format!("id: ev1\nretry: 1500\nevent: message\ndata: {body}\n\n");
    let (out, _) = response(wire.as_bytes(), Framing::Sse, Framing::Sse, Operation::Generate);
    let raw = String::from_utf8(out.clone()).unwrap();
    assert!(
        raw.starts_with("id: ev1\nretry: 1500\nevent: message\ndata: {"),
        "{raw}"
    );
    assert!(raw.ends_with("\n\n") && raw.matches("id: ev1").count() == 1, "{raw}");
    let rows = events(&out);
    assert_eq!(rows[0]["candidates"][0]["content"]["parts"][0]["text"], "a", "{raw}");
    let (again, _) = response(&out, Framing::Sse, Framing::Json, Operation::Generate);
    assert_eq!(
        serde_json::from_slice::<Value>(&again).unwrap()["candidates"][0]["content"]["parts"],
        json!([{"text":"a"},{"text":"b"}])
    );
}

/// A null stop control stays unset; only a single string becomes a one-element list.
#[test]
fn null_stop_stays_unset() {
    let input = json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"stopSequences":null}});
    assert_eq!(request(&input, Operation::Generate).0, input);
    let out =
        super::regressions::chat_to_gemini(br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stop":null}"#);
    let stop = &out["generationConfig"]["stopSequences"];
    assert!(stop.is_null(), "{out}");
}

/// Call part members named like the canonical call members return to Gemini unchanged.
#[test]
fn call_part_members_named_like_canonical_ones_survive() {
    for extra in [json!({"arguments_format":17}), json!({"function":{"future":18}})] {
        let mut part = json!({"functionCall":{"name":"f","args":{}}});
        part.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let input = json!({"contents":[{"role":"model","parts":[part]}]});
        assert_eq!(request(&input, Operation::Generate).0, input);
    }
}

/// Streamed content extras from several chunks merge, as they do in buffered output.
#[test]
fn streamed_content_extras_merge() {
    let rows = [
        json!({"candidates":[{"content":{"parts":[{"text":"hi"}],"futureA":17}}]}),
        json!({"candidates":[{"content":{"futureB":18},"finishReason":"STOP"}]}),
    ];
    for downstream in [Framing::Sse, Framing::Json] {
        let (out, _) = response(&sse(&rows), Framing::Sse, downstream, Operation::Generate);
        let last = match downstream {
            Framing::Sse => events(&out).pop().unwrap(),
            _ => serde_json::from_slice(&out).unwrap(),
        };
        let content = &last["candidates"][0]["content"];
        assert_eq!(
            (&content["futureA"], &content["futureB"]),
            (&json!(17), &json!(18)),
            "{last}"
        );
    }
}

/// A later createTime reaches the client when its spelling changes, even within the same
/// second or when it first appears at the Unix epoch.
#[test]
fn create_time_updates_below_one_second_reach_egress() {
    for (initial, late) in [
        (Some("2026-11-15T00:00:00.123Z"), "2026-11-15T00:00:00.999Z"),
        (None, "1970-01-01T00:00:00Z"),
    ] {
        let mut first = json!({"candidates":[{"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}]});
        if let Some(time) = initial {
            first["createTime"] = json!(time);
        }
        let rows = [first, json!({"createTime":late,"usageMetadata":{"promptTokenCount":1}})];
        for downstream in [Framing::Json, Framing::Sse] {
            let (out, _) = response(&sse(&rows), Framing::Sse, downstream, Operation::Generate);
            let last = match downstream {
                Framing::Json => serde_json::from_slice::<Value>(&out).unwrap(),
                _ => events(&out).pop().unwrap(),
            };
            assert_eq!(last["createTime"], late, "{last}");
        }
    }
    // An unchanged createTime publishes no identity update.
    let time = "2026-11-15T00:00:00.123Z";
    let rows = [
        json!({"createTime":time,"candidates":[{"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}]}),
        json!({"createTime":time,"usageMetadata":{"promptTokenCount":1}}),
    ];
    let (_, canonical) = response(&sse(&rows), Framing::Sse, Framing::Json, Operation::Generate);
    assert!(!canonical.iter().any(|v| v["choices"] == json!([])), "{canonical:?}");
}

/// Gemini ingress of `body` into one client egress; the egress result of the first failing record.
fn gemini_to<E: ResponseEgress>(
    body: &Value,
    egress: impl FnOnce(&mut ck_stream::Writer) -> E,
) -> codec::Result<Vec<u8>> {
    let mut mapped = Mapped::new();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut ingress = gemini::response::Ingress::with_framing(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        Framing::Json,
        1 << 20,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, &serde_json::to_vec(body).unwrap())
        .unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let mut egress = egress(&mut mapped.writer);
    let mut out = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress.advance(&mut mapped.writer, lease, |part| {
            out.extend(bytes(&part));
            Ok(())
        })?;
    }
    egress.finish().map(|_| out)
}

/// A response part with media by URI fails at clients that cannot carry it, before any
/// success; Gemini clients get it back unchanged.
#[test]
fn response_file_data_fails_where_it_cannot_be_written() {
    let body = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[
        {"fileData":{"mimeType":"image/png","fileUri":"https://example.invalid/a.png"}}
    ]},"finishReason":"STOP"}]});
    let (out, _) = response(
        &serde_json::to_vec(&body).unwrap(),
        Framing::Json,
        Framing::Json,
        Operation::Generate,
    );
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap(), body);
    let response = identity(Direction::Response);
    let unsupported = |result: codec::Result<Vec<u8>>| matches!(result, Err(codec::Error::Unsupported));
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        assert!(unsupported(gemini_to(&body, |w| {
            codec::chat::egress::Egress::new(w, response, Operation::Generate, mode, true, |_| {}).unwrap()
        })));
        assert!(unsupported(gemini_to(&body, |w| {
            anthropic::egress::Egress::new(w, response, Operation::Generate, mode, |_| {}).unwrap()
        })));
        assert!(unsupported(gemini_to(&body, |w| {
            responses::egress::Egress::new(w, Operation::Generate, mode, |_, _| {}).unwrap()
        })));
    }
}

/// Chat's `allowed_tools` choice has no Gemini mapping yet and is refused at the fence.
#[test]
fn chat_allowed_tools_choice_is_refused_at_the_gemini_fence() {
    let input = json!({"model":"m","messages":[{"role":"user","content":"q"}],
        "tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}],
        "tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"function","function":{"name":"f"}}]}}});
    let mut m = Mapped::new();
    let mut ingress = codec::chat::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut m.writer, &serde_json::to_vec(&input).unwrap())
        .unwrap();
    let result = ingress.finish(&mut m.writer, &gemini::request::GeminiCapability);
    assert!(matches!(result, Err(codec::Error::Unsupported)));
}
