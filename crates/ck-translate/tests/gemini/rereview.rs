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
