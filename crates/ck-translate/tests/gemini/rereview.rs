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
