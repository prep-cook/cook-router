//! Boundary-codec registry for the new harness: one arm per protocol and role.
//!
//! Extension point: when a protocol's codecs merge, add a `Protocol` variant, map its
//! fixture names in `parse`, and add its arm to each constructor and `validate_encode`.
//! Fixture names follow the old pair formats: `openai` is Chat Completions, `claude` is
//! Anthropic Messages, `openai-response` and `codex` are Responses, `gemini` is Gemini;
//! `interactions` and `antigravity` have no codec, and a workload naming one fails the run.
use crate::{anthropic, gemini, responses};
use crate::codec::{
    self, BodyMode, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress, chat,
    event::{Direction, Identity, LocalId},
};
use ck_stream::{Lease, ScratchLease, Store, Writer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

impl Protocol {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "openai" => Some(Self::Chat),
            "claude" => Some(Self::Anthropic),
            "openai-response" | "codex" => Some(Self::Responses),
            "gemini" => Some(Self::Gemini),
            _ => None,
        }
    }
    pub const SUPPORTED: &[&str] = &["openai", "claude", "openai-response", "codex", "gemini"];
}

/// The boundary stage that refused an operation, and the codec's content-free error.
#[derive(Debug)]
pub struct Failure {
    pub stage: &'static str,
    pub error: codec::Error,
}

pub fn at(stage: &'static str) -> impl FnOnce(codec::Error) -> Failure {
    move |error| Failure { stage, error }
}

pub fn identity(direction: Direction) -> codec::Result<Identity> {
    Ok(Identity {
        request_id: LocalId::new("request")?,
        stream_id: LocalId::new("stream")?,
        attempt: 1,
        direction,
    })
}

/// Client decode: the client protocol's request ingress.
pub enum RequestIn {
    Chat(chat::request::Ingress),
    Anthropic(anthropic::request::Ingress),
    Responses(responses::request::Ingress),
    Gemini(gemini::request::Ingress),
}

impl RequestIn {
    /// `model` is the routed model; only Gemini takes it from outside the body (its URL).
    pub fn new(client: Protocol, owner: &mut Writer, store: &Store, model: &str, limit: u64) -> codec::Result<Self> {
        let id = identity(Direction::Request)?;
        let op = Operation::Generate;
        Ok(match client {
            Protocol::Chat => Self::Chat(chat::request::Ingress::new(owner, store, id, op, limit)?),
            Protocol::Anthropic => Self::Anthropic(anthropic::request::Ingress::new(owner, store, id, op, limit)?),
            Protocol::Responses => Self::Responses(responses::request::Ingress::new(owner, store, id, op, limit)?),
            Protocol::Gemini => Self::Gemini(gemini::request::Ingress::new(owner, store, id, op, model, limit)?),
        })
    }
    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.advance(owner, bytes),
            Self::Anthropic(ingress) => ingress.advance(owner, bytes),
            Self::Responses(ingress) => ingress.advance(owner, bytes),
            Self::Gemini(ingress) => ingress.advance(owner, bytes),
        }
    }
    /// Upstream encode: the fence validates for the upstream destination, whose egress encodes.
    pub fn encode(&mut self, owner: &mut Writer, upstream: Protocol, stream: bool) -> Result<ScratchLease, Failure> {
        match self {
            Self::Chat(ingress) => validate_encode(ingress, owner, upstream, stream),
            Self::Anthropic(ingress) => validate_encode(ingress, owner, upstream, stream),
            Self::Responses(ingress) => validate_encode(ingress, owner, upstream, stream),
            Self::Gemini(ingress) => validate_encode(ingress, owner, upstream, stream),
        }
    }
}

/// Stands in for the per-model `max_tokens` default production resolves (owner ruling
/// 2026-11-15); 32000 is the old pair translators' fallback for an unregistered model.
const DEFAULT_MAX_TOKENS: Option<u64> = Some(32000);

fn validate_encode<I: RequestIngress>(
    ingress: &mut I,
    owner: &mut Writer,
    upstream: Protocol,
    upstream_stream: bool,
) -> Result<ScratchLease, Failure> {
    // `finish` completes the client document and runs the destination fence; the harness
    // tells the two apart afterwards with `ops::passes_own_fence`.
    let (fence, egress) = (at("request_finish"), at("upstream_encode"));
    match upstream {
        Protocol::Chat => {
            let ready = ingress.finish(owner, &chat::request::ChatCapability).map_err(fence)?;
            chat::request::Egress { destination: "chat", upstream_stream }.encode(&ready, owner).map_err(egress)
        }
        Protocol::Anthropic => {
            let capability = anthropic::request::AnthropicCapability { default_max_tokens: DEFAULT_MAX_TOKENS };
            let ready = ingress.finish(owner, &capability).map_err(fence)?;
            anthropic::request::Egress { destination: "anthropic", upstream_stream, default_max_tokens: DEFAULT_MAX_TOKENS }
                .encode(&ready, owner)
                .map_err(egress)
        }
        Protocol::Responses => {
            let ready = ingress.finish(owner, &responses::request::ResponsesCapability).map_err(fence)?;
            responses::request::Egress { destination: "responses", upstream_stream }.encode(&ready, owner).map_err(egress)
        }
        Protocol::Gemini => {
            let ready = ingress.finish(owner, &gemini::request::GeminiCapability).map_err(fence)?;
            gemini::request::Egress { destination: "gemini", upstream_stream }.encode(&ready, owner).map_err(egress)
        }
    }
}

/// Upstream decode: the upstream protocol's response ingress.
pub enum ResponseIn {
    Chat(chat::response::Ingress),
    Anthropic(anthropic::response::Ingress),
    Responses(responses::response::Ingress),
    Gemini(gemini::response::Ingress),
}

impl ResponseIn {
    pub fn new(
        upstream: Protocol,
        owner: &mut Writer,
        store: &Store,
        operation: Operation,
        mode: BodyMode,
        limit: u64,
    ) -> codec::Result<Self> {
        let id = identity(Direction::Response)?;
        Ok(match upstream {
            Protocol::Chat => Self::Chat(chat::response::Ingress::new(owner, store, id, operation, mode, limit)?),
            Protocol::Anthropic => {
                Self::Anthropic(anthropic::response::Ingress::new(owner, id, operation, mode, limit)?)
            }
            Protocol::Responses => {
                Self::Responses(responses::response::Ingress::new(owner, id, operation, mode, limit)?)
            }
            Protocol::Gemini => Self::Gemini(gemini::response::Ingress::new(owner, id, operation, mode, limit)?),
        })
    }
    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.advance(owner, bytes),
            Self::Anthropic(ingress) => ingress.advance(owner, bytes),
            Self::Responses(ingress) => ingress.advance(owner, bytes),
            Self::Gemini(ingress) => ingress.advance(owner, bytes),
        }
    }
    pub fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.finish(owner),
            Self::Anthropic(ingress) => ingress.finish(owner),
            Self::Responses(ingress) => ingress.finish(owner),
            Self::Gemini(ingress) => ingress.finish(owner),
        }
    }
}

/// Client encode: the client protocol's response egress.
pub enum ResponseOut {
    Chat(chat::egress::Egress),
    Anthropic(anthropic::egress::Egress),
    Responses(responses::egress::Egress<fn(&'static str, u64)>),
    Gemini(gemini::egress::Egress),
}

impl ResponseOut {
    pub fn new(
        client: Protocol,
        owner: &mut Writer,
        operation: Operation,
        mode: BodyMode,
        include_usage: bool,
    ) -> codec::Result<Self> {
        let id = identity(Direction::Response)?;
        Ok(match client {
            Protocol::Chat => {
                Self::Chat(chat::egress::Egress::new(owner, id, operation, mode, include_usage, |_| {})?)
            }
            Protocol::Anthropic => {
                Self::Anthropic(anthropic::egress::Egress::new(owner, id, operation, mode, |_| {})?)
            }
            Protocol::Responses => {
                let quiet: fn(&'static str, u64) = |_, _| {};
                Self::Responses(responses::egress::Egress::new(owner, operation, mode, quiet)?)
            }
            Protocol::Gemini => Self::Gemini(gemini::egress::Egress::new(owner, id, operation, mode, |_| {})?),
        })
    }
    pub fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        output: impl FnMut(ScratchLease) -> codec::Result<()>,
    ) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.advance(owner, input, output),
            Self::Anthropic(egress) => egress.advance(owner, input, output),
            Self::Responses(egress) => egress.advance(owner, input, output),
            Self::Gemini(egress) => egress.advance(owner, input, output),
        }
    }
    pub fn finish(&mut self) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.finish(),
            Self::Anthropic(egress) => egress.finish(),
            Self::Responses(egress) => egress.finish(),
            Self::Gemini(egress) => egress.finish(),
        }
    }
}
