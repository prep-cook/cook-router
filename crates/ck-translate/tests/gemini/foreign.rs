//! Gemini and Responses requests bound for Chat or Anthropic, and Chat reasoning bound for
//! Gemini, from the 2026-11-15 live tests.
use super::*;
use crate::anthropic::request::AnthropicCapability;

enum To {
    Chat,
    Anthropic,
}

/// Runs `ingress` over `input` and encodes for `to`; a fence refusal is the error.
fn encode(mapped: &mut Mapped, mut ingress: impl RequestIngress, input: &Value, to: To) -> codec::Result<Value> {
    for chunk in serde_json::to_vec(input).unwrap().chunks(7) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let output = match to {
        To::Chat => {
            let ready = ingress.finish(&mut mapped.writer, &codec::chat::request::ChatCapability)?;
            codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&ready, &mut mapped.writer)
        }
        To::Anthropic => {
            let capability = AnthropicCapability {
                default_max_tokens: Some(4096),
            };
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
fn from_gemini(input: &Value, to: To) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "gemini-test",
        1024 * 1024,
    )
    .unwrap();
    encode(&mut mapped, ingress, input, to)
}
fn from_responses(input: &Value) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let ingress = responses::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    encode(&mut mapped, ingress, input, To::Anthropic)
}

/// A5r: `instructions` becomes the system prompt and `max_output_tokens` becomes `max_tokens`.
#[test]
fn responses_instructions_and_output_limit_reach_anthropic() {
    let input = json!({"model":"claude-haiku-5-5","instructions":"Be brief.","input":[{"role":"user","content":[{"type":"input_text","text":"Name one ocean in one word."}]}],"max_output_tokens":64});
    let out = from_responses(&input).unwrap();
    assert_eq!(out["system"], "Be brief.");
    assert_eq!(out["max_tokens"], 64);
    assert_eq!(
        out["messages"],
        json!([{"role":"user","content":[{"type":"text","text":"Name one ocean in one word."}]}])
    );
    assert!(out.get("max_completion_tokens").is_none(), "{out}");
}

/// A5g: a plain Gemini request carries no provider parameters, so Chat and Anthropic accept it;
/// a Gemini-only member is still refused there.
#[test]
fn plain_gemini_request_reaches_chat_and_anthropic() {
    let input = json!({"systemInstruction":{"parts":[{"text":"Be brief."}]},"contents":[{"role":"user","parts":[{"text":"Name one planet in one word."}]}],"generationConfig":{"maxOutputTokens":64,"stopSequences":["ZZZ"],"temperature":1}});
    let chat = from_gemini(&input, To::Chat).unwrap();
    assert_eq!(chat["messages"][0]["role"], "system");
    assert_eq!(chat["max_tokens"], 64);
    assert_eq!(chat["stop"], json!(["ZZZ"]));
    assert_eq!(chat["temperature"], 1);
    let anthropic = from_gemini(&input, To::Anthropic).unwrap();
    assert_eq!(anthropic["max_tokens"], 64);
    assert_eq!(anthropic["stop_sequences"], json!(["ZZZ"]));
    assert_eq!(anthropic["temperature"], 1);
    let mut native = input.clone();
    native["generationConfig"]["presencePenalty"] = json!(0.5);
    assert!(matches!(from_gemini(&native, To::Chat), Err(codec::Error::Unsupported)));
    assert!(matches!(
        from_gemini(&native, To::Anthropic),
        Err(codec::Error::Unsupported)
    ));
}

/// The Chat `reasoning` object's effort becomes the same `thinkingLevel` as `reasoning_effort`.
#[test]
fn chat_reasoning_effort_object_reaches_gemini_thinking_level() {
    use super::regressions::chat_to_gemini;
    let out =
        chat_to_gemini(br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"reasoning":{"effort":"low"}}"#);
    assert_eq!(
        out,
        json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}})
    );
}
