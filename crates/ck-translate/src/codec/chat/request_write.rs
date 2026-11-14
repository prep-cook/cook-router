use super::super::request_content::write_part;
use super::*;

pub(super) fn write_request(view: RequestView<'_>, streaming: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    let x = doc.required(root, "x_ck")?;
    e.bytes(b"{")?;
    let mut first = true;
    for child in doc.children(root)? {
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "x_ck")? || doc.name_is(name, "stream")? {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        if doc.name_is(name, "tools")? && doc.member(x, "chat_legacy_functions")?.is_some() {
            e.bytes(b"\"functions\":[")?;
            for (i, tool) in doc.children(child)?.enumerate() {
                if i != 0 {
                    e.bytes(b",")?;
                }
                super::super::request_controls::write(doc, doc.required(tool, "function")?, e)?;
            }
            e.bytes(b"]")?;
        } else if doc.name_is(name, "tool_choice")? && doc.member(x, "chat_legacy_choice")?.is_some() {
            e.bytes(b"\"function_call\":")?;
            if matches!(doc.node(child)?.value, Value::Object) {
                super::super::request_controls::write(doc, doc.required(child, "function")?, e)?;
            } else {
                super::super::request_controls::write(doc, child, e)?;
            }
        } else {
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            if doc.name_is(name, "messages")? {
                e.bytes(b"[")?;
                for (i, message) in doc.children(child)?.enumerate() {
                    if i != 0 {
                        e.bytes(b",")?;
                    }
                    write_message(view, message, i as u64, e)?;
                }
                e.bytes(b"]")?;
            } else {
                super::super::request_controls::write(doc, child, e)?;
            }
        }
    }
    if !first {
        e.bytes(b",")?;
    }
    e.bytes(b"\"stream\":")?;
    e.bytes(if streaming { b"true" } else { b"false" })?;
    if let Some(thinking) = doc.member(x, "thinking")? {
        for (from, to) in [("effort", "reasoning_effort"), ("native", "reasoning")] {
            if let Some(value) = doc.member(thinking, from)? {
                e.bytes(b",")?;
                e.string(to)?;
                e.bytes(b":")?;
                doc.write(value, e)?;
            }
        }
    }
    super::super::provenance::request_members(doc, root, None, &[], &["x_ck"], e)?;
    e.bytes(b"}")
}

fn write_message(view: RequestView<'_>, message: usize, _message_index: u64, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let claude = super::super::request_controls::from_claude(doc)?;
    let mut reasoning = None;
    e.bytes(b"{")?;
    for (i, field) in doc.children(message)?.enumerate() {
        if i != 0 {
            e.bytes(b",")?;
        }
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "tool_calls")? {
            let x = doc.required(doc.root(), "x_ck")?;
            let mut legacy = false;
            if let Some(calls) = doc.member(x, "chat_legacy_calls")? {
                for call in doc.children(calls)? {
                    legacy |= doc.origin(call)?.is_some_and(|(parent, _)| parent == message);
                }
            }
            if legacy {
                e.bytes(b"\"function_call\":")?;
                write_function(
                    doc,
                    doc.required(doc.node(field)?.first.ok_or(Error::Schema)?, "function")?,
                    false,
                    e,
                )?;
            } else {
                e.bytes(b"\"tool_calls\":[")?;
                for (j, call) in doc.children(field)?.enumerate() {
                    if j != 0 {
                        e.bytes(b",")?;
                    }
                    let custom = doc.string_is(doc.required(call, "arguments_format")?, "text")?;
                    e.bytes(b"{")?;
                    let mut first = true;
                    for member in doc.children(call)? {
                        let name = doc.node(member)?.name.ok_or(Error::Schema)?;
                        if doc.name_is(name, "arguments_format")? {
                            continue;
                        }
                        if !first {
                            e.bytes(b",")?;
                        }
                        first = false;
                        if doc.name_is(name, "function")? {
                            e.string(if custom { "custom" } else { "function" })?;
                            e.bytes(b":")?;
                            write_function(doc, member, custom, e)?;
                        } else {
                            doc.write_name(name, e)?;
                            e.bytes(b":")?;
                            super::super::request_controls::write(doc, member, e)?;
                        }
                    }
                    super::super::request_controls::members(doc, call, &mut first, e)?;
                    super::super::provenance::request_members(doc, call, None, &[], &[], e)?;
                    e.bytes(b"}")?;
                }
                e.bytes(b"]")?;
            }
        } else {
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            if doc.name_is(name, "content")? && matches!(doc.node(field)?.value, Value::Array) {
                write_content(view, field, claude, e)?;
                reasoning = claude.then_some(field);
            } else {
                super::super::request_controls::write(doc, field, e)?;
            }
        }
    }
    if let Some(content) = reasoning {
        write_reasoning(doc, content, e)?;
    }
    super::super::request_controls::members(doc, message, &mut false, e)?;
    super::super::provenance::request_members(doc, message, None, &[], &[], e)?;
    e.bytes(b"}")
}
/// Writes a content array; for Claude history it leaves out thinking, which
/// [`write_reasoning`] carries, and redacted blocks, which Chat cannot replay. Chat requires at
/// least one part in a content array, so Claude history left with none becomes `""`.
fn write_content(view: RequestView<'_>, content: usize, claude: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let mut first = true;
    for part in doc.children(content)? {
        let kind = doc.required(part, "type")?;
        if claude && (doc.string_is(kind, "reasoning")? || doc.string_is(kind, "redacted")?) {
            continue;
        }
        e.bytes(if first { b"[" } else { b"," })?;
        first = false;
        write_part(view, part, e)?;
    }
    e.bytes(match (first, claude) {
        (false, _) => b"]".as_slice(),
        (true, true) => b"\"\"",
        (true, false) => b"[]",
    })
}
/// Writes the visible thinking of Claude history as `reasoning_content`, blocks joined by a
/// newline; signatures stay behind (owner ruling 2026-11-15).
fn write_reasoning(doc: &Document, content: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = true;
    for part in doc.children(content)? {
        if !doc.string_is(doc.required(part, "type")?, "reasoning")? {
            continue;
        }
        e.bytes(if first {
            b",\"reasoning_content\":\"".as_slice()
        } else {
            b"\\n"
        })?;
        first = false;
        doc.write_contents(doc.required(part, "text")?, e)?;
    }
    if first { Ok(()) } else { e.bytes(b"\"") }
}
fn write_function(doc: &Document, function: usize, custom: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    for (i, field) in doc.children(function)?.enumerate() {
        if i != 0 {
            e.bytes(b",")?;
        }
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "arguments")? {
            e.string(if custom { "input" } else { "arguments" })?;
            e.bytes(b":")?;
            if custom {
                doc.write(field, e)?;
            } else {
                e.quoted_json(|e| doc.write(field, e))?;
            }
        } else {
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            super::super::request_controls::write(doc, field, e)?;
        }
    }
    super::super::request_controls::members(doc, function, &mut false, e)?;
    super::super::provenance::request_members(doc, function, None, &[], &[], e)?;
    e.bytes(b"}")
}
