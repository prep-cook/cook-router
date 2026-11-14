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
