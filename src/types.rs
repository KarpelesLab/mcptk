//! MCP protocol types.
//!
//! These follow the MCP schema (up to revision 2025-11-25). Fields that are
//! rarely used or still moving are kept as raw JSON so newer peers don't break
//! us.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// A JSON object.
pub type JsonObject = Map<String, Value>;

/// The newest protocol revision this crate speaks.
pub const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";

/// Every protocol revision this crate accepts, newest first.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Pick the revision to use for a client that asked for `requested`: the same
/// one if we support it, else our latest (the client then decides whether to
/// go on).
pub fn negotiate_protocol_version(requested: &str) -> &'static str {
    SUPPORTED_PROTOCOL_VERSIONS.iter().find(|v| **v == requested).copied().unwrap_or(LATEST_PROTOCOL_VERSION)
}

/// An icon for a server, tool, resource or prompt.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Icon {
    pub src: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sizes: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub theme: Option<String>,
}

/// Name and version of an MCP client or server.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Implementation {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub website_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Icon>>,
}

impl Implementation {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Implementation { name: name.into(), version: version.into(), ..Default::default() }
    }
}

/// `{ "listChanged": bool }`, shared by several capabilities.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListChangedCapability {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesCapability {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
}

/// What a server offers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental: Option<JsonObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<JsonObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completions: Option<JsonObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompts: Option<ListChangedCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<ListChangedCapability>,
    /// Optional protocol extensions (2026-07-28+), keyed by extension id such
    /// as `io.modelcontextprotocol/tasks`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<JsonObject>,
}

/// What a client offers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental: Option<JsonObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roots: Option<ListChangedCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elicitation: Option<Value>,
    /// Optional protocol extensions (2026-07-28+), keyed by extension id such
    /// as `io.modelcontextprotocol/tasks`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<JsonObject>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub protocol_version: String,
    #[serde(default)]
    pub capabilities: ClientCapabilities,
    #[serde(default)]
    pub client_info: Implementation,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    pub server_info: Implementation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

/// Hints for clients about how to use a piece of content.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Annotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<Vec<Role>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TextContent {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

/// Image or audio data, base64-encoded.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaContent {
    pub data: String,
    pub mime_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

/// A resource embedded in a message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddedResource {
    pub resource: ResourceContents,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

/// A piece of content in a tool result, prompt or sampling message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text(TextContent),
    Image(MediaContent),
    Audio(MediaContent),
    /// A link to a resource the client may read (2025-06-18+).
    ResourceLink(Resource),
    Resource(EmbeddedResource),
}

impl Content {
    pub fn text(text: impl Into<String>) -> Self {
        Content::Text(TextContent { text: text.into(), ..Default::default() })
    }

    pub fn image(base64_data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Content::Image(MediaContent { data: base64_data.into(), mime_type: mime_type.into(), ..Default::default() })
    }

    pub fn audio(base64_data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Content::Audio(MediaContent { data: base64_data.into(), mime_type: mime_type.into(), ..Default::default() })
    }

    pub fn resource_link(resource: Resource) -> Self {
        Content::ResourceLink(resource)
    }

    pub fn embedded(resource: ResourceContents) -> Self {
        Content::Resource(EmbeddedResource { resource, annotations: None, meta: None })
    }

    /// The text, if this is text content.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Content::Text(t) => Some(&t.text),
            _ => None,
        }
    }
}

/// Hints about a tool's behavior. Clients must not trust these from servers
/// they don't trust.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolAnnotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destructive_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_world_hint: Option<bool>,
}

/// A tool definition, as listed by `tools/list`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<ToolAnnotations>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Icon>>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl Tool {
    /// A tool taking no arguments, until [`Tool::input_schema`] says otherwise.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Tool {
            name: name.into(),
            title: None,
            description: Some(description.into()),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: None,
            annotations: None,
            icons: None,
            meta: None,
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the JSON Schema of the arguments; it must be an object schema.
    pub fn input_schema(mut self, schema: Value) -> Self {
        self.input_schema = schema;
        self
    }

    /// Set the JSON Schema of `structuredContent` in results.
    pub fn output_schema(mut self, schema: Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Derive the output schema from `T`.
    #[cfg(feature = "schemars")]
    pub fn output_schema_for<T: schemars::JsonSchema>(self) -> Self {
        self.output_schema(crate::server::schema_for::<T>())
    }

    pub fn annotations(mut self, annotations: ToolAnnotations) -> Self {
        self.annotations = Some(annotations);
        self
    }

    fn annotations_mut(&mut self) -> &mut ToolAnnotations {
        self.annotations.get_or_insert_with(Default::default)
    }

    /// Hint that the tool doesn't modify its environment.
    pub fn read_only(mut self) -> Self {
        self.annotations_mut().read_only_hint = Some(true);
        self
    }

    /// Hint that the tool may perform destructive updates.
    pub fn destructive(mut self) -> Self {
        self.annotations_mut().destructive_hint = Some(true);
        self
    }

    /// Hint that repeated calls with the same arguments have no extra effect.
    pub fn idempotent(mut self) -> Self {
        self.annotations_mut().idempotent_hint = Some(true);
        self
    }

    /// Hint whether the tool interacts with an open world of external entities.
    pub fn open_world(mut self, open: bool) -> Self {
        self.annotations_mut().open_world_hint = Some(open);
        self
    }

    pub fn icon(mut self, icon: Icon) -> Self {
        self.icons.get_or_insert_with(Vec::new).push(icon);
        self
    }

    /// Set a `_meta` entry, for vendor keys such as `anthropic/alwaysLoad`.
    pub fn meta(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.meta.get_or_insert_with(JsonObject::new).insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<JsonObject>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

/// The result of a tool call. Failures of the tool itself are reported here
/// with `is_error`, so the model can see them; protocol failures are JSON-RPC
/// errors instead.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    pub content: Vec<Content>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl CallToolResult {
    pub fn new(content: Vec<Content>) -> Self {
        CallToolResult { content, ..Default::default() }
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self::new(vec![Content::text(text)])
    }

    /// A failed call, with a message the model can act on.
    pub fn error(message: impl Into<String>) -> Self {
        CallToolResult { is_error: Some(true), ..Self::text(message) }
    }

    pub fn structured(mut self, value: Value) -> Self {
        self.structured_content = Some(value);
        self
    }

    pub fn is_error(&self) -> bool {
        self.is_error == Some(true)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListToolsResult {
    pub tools: Vec<Tool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// A resource the server can read.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub uri: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Icon>>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl Resource {
    pub fn new(uri: impl Into<String>, name: impl Into<String>) -> Self {
        Resource { uri: uri.into(), name: name.into(), ..Default::default() }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }

    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Set a `_meta` entry.
    pub fn meta(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.meta.get_or_insert_with(JsonObject::new).insert(key.into(), value.into());
        self
    }
}

/// A family of resources, addressed by an RFC 6570 URI template.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTemplate {
    pub uri_template: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Icon>>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl ResourceTemplate {
    pub fn new(uri_template: impl Into<String>, name: impl Into<String>) -> Self {
        ResourceTemplate { uri_template: uri_template.into(), name: name.into(), ..Default::default() }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }
}

/// The contents of a resource: text, or base64 binary data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResourceContents {
    #[serde(rename_all = "camelCase")]
    Text {
        uri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        text: String,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        meta: Option<JsonObject>,
    },
    #[serde(rename_all = "camelCase")]
    Blob {
        uri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        blob: String,
        #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
        meta: Option<JsonObject>,
    },
}

impl ResourceContents {
    pub fn text(uri: impl Into<String>, mime_type: Option<&str>, text: impl Into<String>) -> Self {
        ResourceContents::Text {
            uri: uri.into(),
            mime_type: mime_type.map(str::to_string),
            text: text.into(),
            meta: None,
        }
    }

    pub fn blob(uri: impl Into<String>, mime_type: Option<&str>, base64_data: impl Into<String>) -> Self {
        ResourceContents::Blob {
            uri: uri.into(),
            mime_type: mime_type.map(str::to_string),
            blob: base64_data.into(),
            meta: None,
        }
    }

    pub fn uri(&self) -> &str {
        match self {
            ResourceContents::Text { uri, .. } | ResourceContents::Blob { uri, .. } => uri,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResourcesResult {
    pub resources: Vec<Resource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResourceTemplatesResult {
    pub resource_templates: Vec<ResourceTemplate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadResourceParams {
    pub uri: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadResourceResult {
    pub contents: Vec<ResourceContents>,
}

/// An argument a prompt accepts.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptArgument {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}

/// A prompt template the server offers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Prompt {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<PromptArgument>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Icon>>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl Prompt {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Prompt { name: name.into(), description: Some(description.into()), ..Default::default() }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn argument(mut self, name: impl Into<String>, description: impl Into<String>, required: bool) -> Self {
        self.arguments.get_or_insert_with(Vec::new).push(PromptArgument {
            name: name.into(),
            description: Some(description.into()),
            required: Some(required),
            title: None,
        });
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPromptParams {
    pub name: String,
    #[serde(default)]
    pub arguments: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromptMessage {
    pub role: Role,
    pub content: Content,
}

impl PromptMessage {
    pub fn user(text: impl Into<String>) -> Self {
        PromptMessage { role: Role::User, content: Content::text(text) }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        PromptMessage { role: Role::Assistant, content: Content::text(text) }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPromptResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub messages: Vec<PromptMessage>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPromptsResult {
    pub prompts: Vec<Prompt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Log severity, as in syslog (RFC 5424).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggingLevel {
    Debug,
    Info,
    Notice,
    Warning,
    Error,
    Critical,
    Alert,
    Emergency,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetLevelParams {
    pub level: LoggingLevel,
}

/// What a completion request is about.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CompletionRef {
    #[serde(rename = "ref/prompt")]
    Prompt { name: String },
    #[serde(rename = "ref/resource")]
    Resource { uri: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompletionArgument {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompletionContext {
    /// Arguments already resolved.
    #[serde(default)]
    pub arguments: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteParams {
    #[serde(rename = "ref")]
    pub reference: CompletionRef,
    pub argument: CompletionArgument,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<CompletionContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Completion {
    pub values: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_more: Option<bool>,
}

impl Completion {
    pub fn new(values: Vec<String>) -> Self {
        Completion { values, ..Default::default() }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompleteResult {
    pub completion: Completion,
}

/// A message for, or from, the client's LLM (sampling).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SamplingMessage {
    pub role: Role,
    pub content: Content,
}

/// `sampling/createMessage` parameters: ask the client's LLM for a completion.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageParams {
    pub messages: Vec<SamplingMessage>,
    pub max_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_preferences: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_context: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageResult {
    pub role: Role,
    pub content: Content,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
}

/// `elicitation/create` parameters: ask the user for structured input.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElicitParams {
    pub message: String,
    /// A flat object schema of primitive properties.
    pub requested_schema: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ElicitAction {
    Accept,
    Decline,
    Cancel,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ElicitResult {
    pub action: ElicitAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<JsonObject>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Root {
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ListRootsResult {
    pub roots: Vec<Root>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn content_is_tagged() {
        assert_eq!(serde_json::to_value(Content::text("hi")).unwrap(), json!({"type":"text","text":"hi"}));
        let link = Content::resource_link(Resource::new("file:///a", "a").mime_type("text/plain"));
        assert_eq!(
            serde_json::to_value(link).unwrap(),
            json!({"type":"resource_link","uri":"file:///a","name":"a","mimeType":"text/plain"})
        );
        let c: Content = serde_json::from_value(json!({"type":"image","data":"AA==","mimeType":"image/png"})).unwrap();
        assert!(matches!(c, Content::Image(m) if m.mime_type == "image/png"));
    }

    #[test]
    fn resource_contents_untagged() {
        let t: ResourceContents = serde_json::from_value(json!({"uri":"x","text":"y"})).unwrap();
        assert!(matches!(t, ResourceContents::Text { .. }));
        let b: ResourceContents = serde_json::from_value(json!({"uri":"x","blob":"AA=="})).unwrap();
        assert!(matches!(b, ResourceContents::Blob { .. }));
    }

    #[test]
    fn negotiates() {
        assert_eq!(negotiate_protocol_version("2025-03-26"), "2025-03-26");
        assert_eq!(negotiate_protocol_version("1999-01-01"), LATEST_PROTOCOL_VERSION);
    }
}
