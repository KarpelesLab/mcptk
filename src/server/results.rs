//! Conversions from handler return values to protocol results.

use crate::types::{CallToolResult, Content, GetPromptResult, PromptMessage, ReadResourceResult, ResourceContents};
use serde::Serialize;

/// Something a tool handler can return.
pub trait IntoToolResult {
    fn into_tool_result(self) -> CallToolResult;
}

impl IntoToolResult for CallToolResult {
    fn into_tool_result(self) -> CallToolResult {
        self
    }
}

impl IntoToolResult for String {
    fn into_tool_result(self) -> CallToolResult {
        CallToolResult::text(self)
    }
}

impl IntoToolResult for &'static str {
    fn into_tool_result(self) -> CallToolResult {
        CallToolResult::text(self)
    }
}

impl IntoToolResult for Content {
    fn into_tool_result(self) -> CallToolResult {
        CallToolResult::new(vec![self])
    }
}

impl IntoToolResult for Vec<Content> {
    fn into_tool_result(self) -> CallToolResult {
        CallToolResult::new(self)
    }
}

impl IntoToolResult for () {
    fn into_tool_result(self) -> CallToolResult {
        CallToolResult::new(Vec::new())
    }
}

/// Return a value as JSON: serialized as text content, and as
/// `structuredContent` (pair it with
/// [`Tool::output_schema_for`](crate::types::Tool::output_schema_for)).
///
/// Protocol 2026-07-28 allows any JSON value as `structuredContent`; older
/// revisions only objects, so for those clients other values are sent as
/// text only.
#[derive(Clone, Debug)]
pub struct Json<T>(pub T);

impl<T: Serialize> IntoToolResult for Json<T> {
    fn into_tool_result(self) -> CallToolResult {
        match serde_json::to_value(&self.0) {
            Ok(value) => CallToolResult::text(value.to_string()).structured(value),
            Err(e) => CallToolResult::error(format!("failed to serialize result: {e}")),
        }
    }
}

/// Something a resource handler can return. Plain text gets the URI and MIME
/// type of the resource read.
pub trait IntoReadResult {
    fn into_read_result(self, uri: &str, mime_type: Option<&str>) -> ReadResourceResult;
}

impl IntoReadResult for ReadResourceResult {
    fn into_read_result(self, _: &str, _: Option<&str>) -> ReadResourceResult {
        self
    }
}

impl IntoReadResult for ResourceContents {
    fn into_read_result(self, _: &str, _: Option<&str>) -> ReadResourceResult {
        ReadResourceResult { contents: vec![self], meta: None }
    }
}

impl IntoReadResult for Vec<ResourceContents> {
    fn into_read_result(self, _: &str, _: Option<&str>) -> ReadResourceResult {
        ReadResourceResult { contents: self, meta: None }
    }
}

impl IntoReadResult for String {
    fn into_read_result(self, uri: &str, mime_type: Option<&str>) -> ReadResourceResult {
        ResourceContents::text(uri, mime_type, self).into_read_result(uri, mime_type)
    }
}

impl IntoReadResult for &'static str {
    fn into_read_result(self, uri: &str, mime_type: Option<&str>) -> ReadResourceResult {
        self.to_string().into_read_result(uri, mime_type)
    }
}

/// Something a prompt handler can return.
pub trait IntoPromptResult {
    fn into_prompt_result(self) -> GetPromptResult;
}

impl IntoPromptResult for GetPromptResult {
    fn into_prompt_result(self) -> GetPromptResult {
        self
    }
}

impl IntoPromptResult for Vec<PromptMessage> {
    fn into_prompt_result(self) -> GetPromptResult {
        GetPromptResult { description: None, messages: self, meta: None }
    }
}

impl IntoPromptResult for PromptMessage {
    fn into_prompt_result(self) -> GetPromptResult {
        vec![self].into_prompt_result()
    }
}

/// A single user message.
impl IntoPromptResult for String {
    fn into_prompt_result(self) -> GetPromptResult {
        PromptMessage::user(self).into_prompt_result()
    }
}
