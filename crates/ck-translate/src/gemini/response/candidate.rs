//! One native response body: identity, counts, prompt feedback, candidates and extras.

use super::{Choice, State};
use crate::codec::{
    Error, Operation, Result,
    document::Document,
    event::{Kind, Scope},
};
use ck_stream::Writer;

/// Candidate members with their own canonical record; the rest travel as `gemini.candidate`.
const CANDIDATE_KNOWN: &[&str] = &[
    "index",
    "content",
    "finishReason",
    "groundingMetadata",
    "citationMetadata",
    "safetyRatings",
    "urlContextMetadata",
    "logprobsResult",
];
/// A finished candidate's `finishMessage` travels on its finish chunk.
const FINISHED_KNOWN: &[&str] = &[
    "index",
    "content",
    "finishReason",
    "finishMessage",
    "groundingMetadata",
    "citationMetadata",
    "safetyRatings",
    "urlContextMetadata",
    "logprobsResult",
];
/// Response members with their own canonical record; the rest travel as `gemini.response`.
const RESPONSE_KNOWN: &[&str] = &[
    "candidates",
    "usageMetadata",
    "promptFeedback",
    "responseId",
    "modelVersion",
    "createTime",
];

impl State {
    pub(super) fn body(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        super::super::object(doc, root)?;
        if doc.member(root, "error")?.is_some() {
            return Err(Error::Schema);
        }
        if self.base.is_none() {
            self.start(owner, doc)?;
        } else {
            self.refresh(owner, doc)?;
        }
        if self.operation == Operation::CountTokens {
            return self.count(owner, doc);
        }
        if let Some(feedback) = doc.member(root, "promptFeedback")? {
            if let Some(block) = doc.member(feedback, "blockReason")?
                && !doc.string_is(block, "BLOCK_REASON_UNSPECIFIED")?
            {
                self.blocked = true;
            }
            self.named(
                owner,
                doc,
                feedback,
                Kind::Annotation,
                Scope::default(),
                "promptFeedback",
            )?;
        }
        if let Some(candidates) = doc.member(root, "candidates")? {
            super::super::array(doc, candidates)?;
            for (ordinal, candidate) in doc.children(candidates)?.enumerate() {
                self.candidate(owner, doc, candidate, ordinal as u64)?;
            }
        }
        if let Some(usage) = doc.member(root, "usageMetadata")? {
            self.usage(owner, doc, usage)?;
        }
        self.extras(owner, doc, root, None, RESPONSE_KNOWN)
    }
    pub(super) fn start(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        let base = self.base_fields(owner, doc)?;
        let synthetic = self.synthetic;
        let created = match doc.member(root, "createTime")? {
            Some(n) => unix_seconds(doc, n)?.is_some(),
            None => false,
        };
        self.emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| super::super::members(&base, base.root(), &[], e),
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"ids\":{")?;
                if let Some(n) = doc.member(root, "responseId")? {
                    e.bytes(b"\"responseId\":")?;
                    doc.write(n, e)?;
                }
                e.bytes(b"}")?;
                if let Some(n) = doc.member(root, "createTime")? {
                    e.bytes(b",\"timestamp\":")?;
                    doc.write(n, e)?;
                }
                e.bytes(b"}")?;
                super::synthetic(synthetic, !created, e)
            },
        )?;
        self.base = Some(base);
        self.keep_time(owner, doc)
    }
    fn keep_time(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        if let Some(n) = doc.member(doc.root(), "createTime")? {
            self.time = Some(super::super::mapped(owner, |e| doc.write(n, e))?);
        }
        Ok(())
    }
    /// The base chunk fields: the body's `responseId` and `modelVersion`, else the published
    /// ones, else the stream id and an empty model, which stay marked synthetic.
    fn base_fields(&mut self, owner: &mut Writer, doc: &Document) -> Result<Document> {
        let root = doc.root();
        let supplied = [doc.member(root, "responseId")?, doc.member(root, "modelVersion")?];
        let time = match doc.member(root, "createTime")? {
            Some(n) => unix_seconds(doc, n)?,
            None => None,
        };
        let previous = self.base.as_ref();
        let base = super::super::mapped(owner, |e| {
            for ((canonical, node), fallback) in ["id", "model"]
                .into_iter()
                .zip(supplied)
                .zip([self.identity.stream_id.as_str(), ""])
            {
                e.bytes(if canonical == "id" { b"{" } else { b"," })?;
                e.string(canonical)?;
                e.bytes(b":")?;
                match (node, previous) {
                    (Some(n), _) => {
                        super::super::string(doc, n)?;
                        doc.write(n, e)?;
                    }
                    (None, Some(base)) => base.write(base.required(base.root(), canonical)?, e)?,
                    (None, None) => e.string(fallback)?,
                }
            }
            e.bytes(b",\"created\":")?;
            match (time, previous) {
                (Some(time), _) => e.unsigned(time)?,
                (None, Some(base)) => base.write(base.required(base.root(), "created")?, e)?,
                (None, None) => e.bytes(b"0")?,
            }
            e.bytes(b"}")
        })?;
        for (synthetic, node) in self.synthetic.iter_mut().zip(supplied) {
            *synthetic &= node.is_none();
        }
        Ok(base)
    }
    /// Rebuilds the base when a later body supplies an identity that differs from the
    /// published one, and publishes it in a chunk without choices, so the change reaches
    /// consumers even when the body has no candidate.
    fn refresh(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        let root = doc.root();
        let mut changed = false;
        for ((native, canonical), synthetic) in [("responseId", "id"), ("modelVersion", "model")]
            .into_iter()
            .zip(self.synthetic)
        {
            if let Some(n) = doc.member(root, native)? {
                super::super::string(doc, n)?;
                changed |= synthetic || !super::super::same(base, base.required(base.root(), canonical)?, doc, n)?;
            }
        }
        let time = doc.member(root, "createTime")?;
        if let Some(n) = time {
            changed |= match &self.time {
                Some(old) => !super::super::same(old, old.root(), doc, n)?,
                None => true,
            };
        }
        if !changed {
            return Ok(());
        }
        let base = self.base_fields(owner, doc)?;
        let synthetic = self.synthetic;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            Scope::default(),
            |e| {
                super::super::members(&base, base.root(), &[], e)?;
                e.bytes(b",\"object\":\"chat.completion.chunk\",\"choices\":[]")
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\"")?;
                if let Some(n) = time {
                    e.bytes(b",\"timestamp\":")?;
                    doc.write(n, e)?;
                }
                e.bytes(b"}")?;
                super::synthetic(synthetic, false, e)
            },
        )?;
        self.base = Some(base);
        self.keep_time(owner, doc)
    }
    fn count(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        let total = doc.required(root, "totalTokens")?;
        doc.unsigned(total)?;
        self.emitter.emit(
            owner,
            Kind::CountTokens,
            Scope::default(),
            |e| {
                e.bytes(b",\"total_tokens\":")?;
                doc.write(total, e)?;
                e.bytes(b",\"details\":")?;
                doc.write(root, e)
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
        )?;
        Ok(())
    }
    fn candidate(&mut self, owner: &mut Writer, doc: &Document, candidate: usize, ordinal: u64) -> Result<()> {
        let index = match doc.member(candidate, "index")? {
            Some(n) => doc.unsigned(n)?,
            None => ordinal,
        };
        let slot = match self.choices.iter().position(|c| c.index == index) {
            Some(n) => n,
            None => self.choices.push(Choice {
                index,
                finished: false,
                next_part: 0,
                next_tool: 0,
            })?,
        };
        if self.choices[slot].finished {
            return Err(Error::Schema);
        }
        if let Some(content) = doc.member(candidate, "content")? {
            if let Some(role) = doc.member(content, "role")?
                && !doc.string_is(role, "model")?
            {
                return Err(Error::Schema);
            }
            if let Some(parts) = doc.member(content, "parts")? {
                super::super::array(doc, parts)?;
                for part in doc.children(parts)? {
                    self.part(owner, doc, part, slot)?;
                }
            }
        }
        let scope = Scope {
            choice_index: Some(index),
            ..Scope::default()
        };
        for (field, kind) in [
            ("groundingMetadata", Kind::Grounding),
            ("citationMetadata", Kind::Annotation),
            ("safetyRatings", Kind::Annotation),
            ("urlContextMetadata", Kind::UrlContext),
        ] {
            if let Some(n) = doc.member(candidate, field)? {
                self.named(owner, doc, n, kind, scope, field)?;
            }
        }
        if let Some(logprobs) = doc.member(candidate, "logprobsResult")? {
            self.chunk(
                owner,
                scope,
                |e| e.bytes(b"{}"),
                None,
                Some((doc, logprobs)),
                |_| Ok(()),
            )?;
        }
        let finish = doc.member(candidate, "finishReason")?;
        let known = if finish.is_some() {
            FINISHED_KNOWN
        } else {
            CANDIDATE_KNOWN
        };
        self.extras(owner, doc, candidate, Some(index), known)?;
        if let Some(finish) = finish {
            self.finish(owner, doc, candidate, finish, slot)?;
        }
        Ok(())
    }
    /// Publishes the finish chunk: the Chat reason, the exact native reason and the native
    /// `finishMessage`, which belongs to the finish rather than to the candidate's extras.
    fn finish(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        candidate: usize,
        finish: usize,
        slot: usize,
    ) -> Result<()> {
        super::super::string(doc, finish)?;
        let reason = reason(doc, finish, self.choices[slot].next_tool != 0)?;
        let message = doc.member(candidate, "finishMessage")?;
        let scope = Scope {
            choice_index: Some(self.choices[slot].index),
            ..Scope::default()
        };
        self.chunk(
            owner,
            scope,
            |e| e.bytes(b"{}"),
            Some(reason),
            None,
            |e| {
                e.bytes(b",\"native_finish_reason\":")?;
                doc.write(finish, e)?;
                if let Some(message) = message {
                    e.bytes(b",\"original\":[{\"protocol\":\"gemini\",\"pointer\":\"/finishMessage\",\"value\":")?;
                    doc.write(message, e)?;
                    e.bytes(b"}]")?;
                }
                Ok(())
            },
        )?;
        self.choices[slot].finished = true;
        Ok(())
    }
    /// Publishes the members of a response or candidate (`choice`) that have no canonical
    /// record, when there are any, as one opaque record; a candidate's unknown `content`
    /// members travel as its `content` member.
    fn extras(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        parent: usize,
        choice: Option<u64>,
        known: &[&str],
    ) -> Result<()> {
        let content = match choice {
            Some(_) => doc.member(parent, "content")?,
            None => None,
        };
        let content = match content {
            Some(n) if unknown(doc, n, &["role", "parts"])? => Some(n),
            _ => None,
        };
        if content.is_none() && !unknown(doc, parent, known)? {
            return Ok(());
        }
        let scope = Scope {
            choice_index: choice,
            ..Scope::default()
        };
        self.emitter.emit(
            owner,
            Kind::Opaque,
            scope,
            |e| {
                e.bytes(b",\"format\":\"json\",\"name\":")?;
                match choice {
                    Some(index) => {
                        e.bytes(b"\"gemini.candidate\",\"value\":{\"index\":")?;
                        e.unsigned(index)?;
                    }
                    None => e.bytes(b"\"gemini.response\",\"value\":{\"candidates\":[]")?,
                }
                super::super::members(doc, parent, known, e)?;
                if let Some(content) = content {
                    e.bytes(b",\"content\":{")?;
                    let mut first = true;
                    for n in doc.children(content)? {
                        let name = doc.node(n)?.name.ok_or(Error::Schema)?;
                        if !doc.name_is(name, "role")? && !doc.name_is(name, "parts")? {
                            e.bytes(if std::mem::replace(&mut first, false) {
                                b""
                            } else {
                                b","
                            })?;
                            doc.write_name(name, e)?;
                            e.bytes(b":")?;
                            doc.write(n, e)?;
                        }
                    }
                    e.bytes(b"}")?;
                }
                e.bytes(b"}")
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
        )?;
        Ok(())
    }
    pub(super) fn named(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        node: usize,
        kind: Kind,
        scope: Scope,
        name: &'static str,
    ) -> Result<()> {
        self.emitter.emit(
            owner,
            kind,
            scope,
            |e| {
                if kind == Kind::Annotation {
                    e.bytes(b",\"kind\":")?;
                    e.string(name)?;
                }
                e.bytes(b",\"value\":")?;
                doc.write(node, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"event\":")?;
                e.string(name)?;
                e.bytes(b"}")
            },
        )?;
        Ok(())
    }
}

/// Whether `parent` has a member not named in `known`.
fn unknown(doc: &Document, parent: usize, known: &[&str]) -> Result<bool> {
    for node in doc.children(parent)? {
        let name = doc.node(node)?.name.ok_or(Error::Schema)?;
        if !known
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
        {
            return Ok(true);
        }
    }
    Ok(false)
}
/// Unix seconds of a protobuf JSON `Timestamp`: `YYYY-MM-DDThh:mm:ss[.frac]` then `Z` or
/// `±hh:mm`. Another spelling or a time before 1970 has none.
/// ponytail: repeats crate::common::parse_rfc3339_unix without its day-of-month check, because
/// the staged codec build cannot reach crate::common; call that once the codec joins the crate.
fn unix_seconds(doc: &Document, node: usize) -> Result<Option<u64>> {
    let (mut text, mut len, mut chars) = ([0u8; 40], 0, doc.chars(node)?);
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || len == text.len() {
            return Ok(None);
        }
        text[len] = c as u8;
        len += 1;
    }
    let s = &text[..len];
    let digits = |at: usize, n: usize| -> Option<i64> {
        s.get(at..at + n)?
            .iter()
            .try_fold(0, |v, &b| b.is_ascii_digit().then(|| v * 10 + i64::from(b - b'0')))
    };
    let separators = [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')];
    let fields = [(0, 4), (5, 2), (8, 2), (11, 2), (14, 2), (17, 2)].map(|(at, n)| digits(at, n));
    let [Some(year), Some(month), Some(day), Some(hour), Some(min), Some(sec)] = fields else {
        return Ok(None);
    };
    if separators.iter().any(|&(at, c)| s[at] != c) {
        return Ok(None);
    }
    let mut at = 19;
    if s.get(at) == Some(&b'.') {
        at += 1;
        while s.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
    }
    let offset = match (s.get(at), digits(at + 1, 2), digits(at + 4, 2)) {
        (Some(b'Z'), ..) if at + 1 == len => 0,
        (Some(&sign @ (b'+' | b'-')), Some(h), Some(m)) if at + 6 == len && s[at + 3] == b':' => {
            if sign == b'-' {
                -(h * 60 + m) * 60
            } else {
                (h * 60 + m) * 60
            }
        }
        _ => return Ok(None),
    };
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 59 {
        return Ok(None);
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let (era, yoe) = (y.div_euclid(400), y.rem_euclid(400));
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    Ok(u64::try_from(days * 86_400 + hour * 3600 + min * 60 + sec - offset).ok())
}
/// The Chat finish reason for a native one. Chat has no value for the other native reasons
/// (`OTHER`, `MALFORMED_FUNCTION_CALL`, `FINISH_REASON_UNSPECIFIED`, future ones), so they
/// finish as `stop`; the exact native reason rides along as `native_finish_reason`.
fn reason(doc: &Document, finish: usize, called: bool) -> Result<&'static str> {
    const FILTERED: &[&str] = &[
        "SAFETY",
        "RECITATION",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "IMAGE_SAFETY",
        "IMAGE_PROHIBITED_CONTENT",
        "IMAGE_RECITATION",
    ];
    Ok(if doc.string_is(finish, "MAX_TOKENS")? {
        "length"
    } else if FILTERED.iter().try_fold(false, |found, name| {
        Ok::<_, Error>(found || doc.string_is(finish, name)?)
    })? {
        "content_filter"
    } else if called && doc.string_is(finish, "STOP")? {
        "tool_calls"
    } else {
        "stop"
    })
}
