//! Native parts: one open document per canonical block, written out when the block ends.

use super::{Block, Diagnostic, Egress, local, mapped, native};
use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    encode,
    event::{Kind, LocalId},
};
use ck_stream::{ScratchLease, Writer};

impl Egress {
    /// The open block a record's `item_id` and `block_index` name.
    fn block(&self, event: &Document) -> Result<usize> {
        let (item, index) = key(event)?;
        self.blocks
            .iter()
            .position(|b| b.item == item && b.index == index)
            .ok_or(Error::Schema)
    }
    pub(super) fn block_start(&mut self, owner: &mut Writer, event: Document) -> Result<()> {
        let root = event.root();
        let x = event.required(root, "x_ck")?;
        let choice = event.unsigned(event.required(x, "choice_index")?)?;
        self.open_choice(choice)?;
        let item = local(&event, event.required(x, "item_id")?)?;
        let index = event.unsigned(event.required(x, "block_index")?)?;
        if self.blocks.iter().any(|b| b.item == item && b.index == index) {
            return Err(Error::Schema);
        }
        let thought =
            super::super::is(&event, root, "kind", "thinking")? || super::super::is(&event, root, "kind", "reasoning")?;
        let mut document = empty_part(owner, thought)?;
        restore_originals(&mut document, event)?;
        self.blocks.push(Block {
            item,
            index,
            choice,
            document,
            arguments: None,
            media: None,
            thought,
            held: false,
        })?;
        Ok(())
    }
    /// Appends a text or reasoning delta. A streamed part first writes the text it holds, so
    /// a part keeps its signature while long text still streams.
    pub(super) fn text(
        &mut self,
        owner: &mut Writer,
        event: Document,
        text: usize,
        reasoning: bool,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let block = self.block(&event)?;
        if self.blocks[block].held {
            self.write_part(owner, block, output)?;
            let thought = self.blocks[block].thought;
            self.blocks[block].document = empty_part(owner, thought)?;
        }
        let entry = &mut self.blocks[block];
        entry.thought |= reasoning;
        entry.held = self.body.is_none();
        let doc = &mut entry.document;
        let root = doc.root();
        if reasoning {
            doc.set(root, "thought", Value::Bool(true))?;
        }
        let target = match doc.member(root, "text")? {
            Some(node) => node,
            None => doc.set(root, "text", Value::Fragments)?,
        };
        move_absorbed(doc, event, text, target)
    }
    pub(super) fn arguments(&mut self, event: Document, fragment: usize) -> Result<()> {
        let block = self.block(&event)?;
        let entry = &mut self.blocks[block];
        let target = entry.arguments.ok_or(Error::Schema)?;
        move_absorbed(&mut entry.document, event, fragment, target)
    }
    pub(super) fn signature(&mut self, event: Document) -> Result<()> {
        let root = event.root();
        if !super::super::is(&event, root, "kind", "gemini")? {
            (self.diagnostic)(Diagnostic::UnsupportedEvent(Kind::ReasoningSignature));
            return Ok(());
        }
        let fragment = event.required(root, "fragment")?;
        let block = self.block(&event)?;
        let doc = &mut self.blocks[block].document;
        let part = doc.root();
        let target = match doc.member(part, "thoughtSignature")? {
            Some(node) => node,
            None => doc.set(part, "thoughtSignature", Value::Fragments)?,
        };
        move_absorbed(doc, event, fragment, target)
    }
    pub(super) fn tool_start(&mut self, owner: &mut Writer, event: Document) -> Result<()> {
        let root = event.root();
        let x = event.required(root, "x_ck")?;
        if event.member(x, "block_index")?.is_none() {
            let choice = event.unsigned(event.required(x, "choice_index")?)?;
            self.open_choice(choice)?;
            let (item, index) = key(&event)?;
            if self.blocks.iter().any(|b| b.item == item && b.index == index) {
                return Err(Error::Schema);
            }
            self.blocks.push(Block {
                item,
                index,
                choice,
                document: empty_part(owner, false)?,
                arguments: None,
                media: None,
                thought: false,
                held: false,
            })?;
        }
        let name = event.required(root, "name")?;
        let id = match event
            .member(x, "source")?
            .map(|s| event.member(s, "ids"))
            .transpose()?
            .flatten()
        {
            Some(ids) => event.member(ids, "id")?,
            None => None,
        };
        let block = self.block(&event)?;
        let entry = &mut self.blocks[block];
        let doc = &mut entry.document;
        let call = container(doc, "functionCall")?;
        let offset = absorb(doc, event)?;
        take(doc, offset, name, call, "name")?;
        if let Some(id) = id {
            take(doc, offset, id, call, "id")?;
        }
        entry.arguments = Some(doc.add(None, Value::Fragments)?);
        Ok(())
    }
    /// Parses the collected argument fragments into the call's `args` object, and ends the
    /// part of a call outside any block.
    pub(super) fn tool_end(
        &mut self,
        owner: &mut Writer,
        event: &Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        self.tool_args(owner, event)?;
        let x = event.required(event.root(), "x_ck")?;
        if event.member(x, "block_index")?.is_none() {
            self.block_end(owner, event, output)?;
        }
        Ok(())
    }
    fn tool_args(&mut self, owner: &mut Writer, event: &Document) -> Result<()> {
        let block = self.block(event)?;
        let entry = &mut self.blocks[block];
        let arguments = entry.arguments.take().ok_or(Error::Schema)?;
        let doc = &mut entry.document;
        if doc.node(arguments)?.first.is_none() {
            return Ok(());
        }
        let raw = encode::scratch(owner, |e| {
            let mut chars = doc.chars(arguments)?;
            while let Some(c) = chars.next_scalar()? {
                e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
            }
            Ok(())
        })?;
        let value = doc.import_json(owner, raw)?;
        super::super::object(doc, value)?;
        let call = doc.required(doc.root(), "functionCall")?;
        doc.rename(value, "args")?;
        doc.append(call, value)
    }
    /// Sets a native part member from a record's payload: a function response, executable
    /// code or its result, or an audio transcription.
    pub(super) fn part_value(&mut self, kind: Kind, event: Document) -> Result<()> {
        let root = event.root();
        let x = event.required(root, "x_ck")?;
        let (name, value) = match kind {
            Kind::ToolResult => ("functionResponse", event.required(root, "value")?),
            Kind::CodeExecution if super::super::is(&event, root, "phase", "code")? => {
                ("executableCode", event.required(root, "value")?)
            }
            Kind::CodeExecution => ("codeExecutionResult", event.required(root, "value")?),
            _ => ("audioTranscription", event.required(root, "text")?),
        };
        if kind != Kind::MediaTranscript && !native(&event, x)? {
            (self.diagnostic)(Diagnostic::UnsupportedEvent(kind));
            return Ok(());
        }
        let block = self.block(&event)?;
        let doc = &mut self.blocks[block].document;
        let part = doc.root();
        doc.remove(part, name)?;
        let offset = absorb(doc, event)?;
        take(doc, offset, value, part, name)
    }
    pub(super) fn media_start(&mut self, event: Document) -> Result<()> {
        let root = event.root();
        let mime = event.member(root, "mime")?;
        let block = self.block(&event)?;
        let entry = &mut self.blocks[block];
        let doc = &mut entry.document;
        let inline = container(doc, "inlineData")?;
        if let Some(mime) = mime {
            let offset = absorb(doc, event)?;
            take(doc, offset, mime, inline, "mimeType")?;
        }
        entry.media = Some(doc.set(inline, "data", Value::Base64Fragments)?);
        Ok(())
    }
    pub(super) fn data(&mut self, event: Document) -> Result<()> {
        let bytes = event.required(event.root(), "bytes")?;
        let block = self.block(&event)?;
        let entry = &mut self.blocks[block];
        let target = entry.media.ok_or(Error::Schema)?;
        move_absorbed(&mut entry.document, event, bytes, target)
    }
    pub(super) fn block_end(
        &mut self,
        owner: &mut Writer,
        event: &Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let block = self.block(event)?;
        if self.blocks[block].arguments.is_some() {
            return Err(Error::Schema);
        }
        let doc = &self.blocks[block].document;
        if self.body.is_none() {
            if doc.node(doc.root())?.first.is_some() {
                self.write_part(owner, block, output)?;
            }
            self.blocks.swap_remove(block);
            return Ok(());
        }
        let Block { choice, document, .. } = self.blocks.swap_remove(block);
        let slot = self.choice(choice)?;
        let candidate = self.choices[slot].node.ok_or(Error::Schema)?;
        let body = self.body.as_mut().ok_or(Error::Schema)?;
        let parts = body.required(body.required(candidate, "content")?, "parts")?;
        let part = body.absorb(document)?;
        body.append(parts, part)
    }
    /// Holds a streamed part as its choice's next record, first writing the part held before
    /// it. A finished choice keeps its members for the last record.
    fn write_part(
        &mut self,
        owner: &mut Writer,
        block: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let slot = self.choice(self.blocks[block].choice)?;
        let document = std::mem::replace(&mut self.blocks[block].document, empty_part(owner, false)?);
        self.blocks[block].held = false;
        if self.choices[slot].part.is_some() {
            let fields = !self.choices[slot].finished;
            self.flush(owner, slot, fields, output)?;
        }
        self.choices[slot].part = Some(document);
        Ok(())
    }
}
/// A record's block: its `item_id` and `block_index`, or for a call outside any block (Chat
/// and Responses sources) its `tool_call_id` with an index no block uses.
fn key(event: &Document) -> Result<(LocalId, u64)> {
    let x = event.required(event.root(), "x_ck")?;
    match event.member(x, "block_index")? {
        Some(index) => Ok((local(event, event.required(x, "item_id")?)?, event.unsigned(index)?)),
        None => Ok((local(event, event.required(x, "tool_call_id")?)?, u64::MAX)),
    }
}
/// The part's object member `name`, added when absent.
fn container(doc: &mut Document, name: &'static str) -> Result<usize> {
    let part = doc.root();
    match doc.member(part, name)? {
        Some(node) => Ok(node),
        None => doc.set(part, name, Value::Object),
    }
}
fn empty_part(owner: &mut Writer, thought: bool) -> Result<Document> {
    mapped(owner, |e| e.bytes(if thought { b"{\"thought\":true}" } else { b"{}" }))
}
/// Absorbs a record into `doc` and returns the offset of its node indexes there. Records stay
/// whole: a Chat chunk may hold restored provenance in further sources.
fn absorb(doc: &mut Document, event: Document) -> Result<usize> {
    let root = event.root();
    doc.absorb(event)?.checked_sub(root).ok_or(Error::Position)
}
/// Moves absorbed record node `value` under `target`, named `name`.
fn take(doc: &mut Document, offset: usize, value: usize, target: usize, name: &'static str) -> Result<()> {
    let node = value.checked_add(offset).ok_or(Error::Limit)?;
    doc.detach(doc.node(node)?.parent.ok_or(Error::Schema)?, node)?;
    doc.rename(node, name)?;
    doc.append(target, node)
}
/// Moves record node `value` into `doc` as the last child of `target`.
fn move_absorbed(doc: &mut Document, event: Document, value: usize, target: usize) -> Result<()> {
    let node = value.checked_add(absorb(doc, event)?).ok_or(Error::Limit)?;
    doc.detach(doc.node(node)?.parent.ok_or(Error::Schema)?, node)?;
    doc.append(target, node)
}
/// Restores the part members a Gemini source kept as provenance, unless the part already
/// has a member of that name.
fn restore_originals(doc: &mut Document, event: Document) -> Result<()> {
    let x = event.required(event.root(), "x_ck")?;
    let Some(original) = event.member(x, "original")? else {
        return Ok(());
    };
    let offset = absorb(doc, event)?;
    let original = original.checked_add(offset).ok_or(Error::Limit)?;
    let part = doc.root();
    let mut next = doc.node(original)?.first;
    while let Some(entry) = next {
        next = doc.node(entry)?.next;
        if !super::super::is(doc, entry, "protocol", "gemini")? {
            continue;
        }
        let Value::String(text) = doc.node(doc.required(entry, "pointer")?)?.value else {
            return Err(Error::Schema);
        };
        let name = Name::Pointer(text);
        // A nested pointer names a member of the part's call or inline data.
        let mut parent = part;
        for (prefix, member) in [("/functionCall/", "functionCall"), ("/inlineData/", "inlineData")] {
            let mut chars = doc.chars(doc.required(entry, "pointer")?)?;
            if prefix
                .chars()
                .try_fold(true, |yes, p| Ok::<_, Error>(yes && chars.next_scalar()? == Some(p)))?
            {
                parent = container(doc, member)?;
            }
        }
        let mut present = false;
        for member in doc.children(parent)? {
            present |= doc.names_equal(name, doc.node(member)?.name.ok_or(Error::Schema)?)?;
        }
        if !present {
            let node = doc.add(Some(name), Value::Null)?;
            doc.transplant(node, doc.required(entry, "value")?)?;
            doc.append(parent, node)?;
        }
    }
    Ok(())
}
