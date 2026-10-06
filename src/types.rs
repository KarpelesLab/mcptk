//! MCP protocol types.
//!
//! These follow the MCP schema (up to revision 2026-07-28). Fields that are
//! rarely used or still moving are kept as raw JSON so newer peers don't break
//! us.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// A JSON object.
pub type JsonObject = Map<String, Value>;

/// The newest protocol revision this crate speaks (a stateless revision:
/// see [`STATELESS_PROTOCOL_VERSIONS`]).
pub const LATEST_PROTOCOL_VERSION: &str = "2026-07-28";

/// The newest revision that uses the `initialize` handshake: what
/// `initialize` falls back to for clients asking for one we don't know.
pub const LATEST_HANDSHAKE_PROTOCOL_VERSION: &str = "2025-11-25";

/// Every protocol revision this crate accepts, newest first.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Revisions without a handshake (2026-07-28 and later): every request
/// carries its protocol version and client capabilities in `_meta`.
pub const STATELESS_PROTOCOL_VERSIONS: &[&str] = &["2026-07-28"];

/// Revisions that open a session with the `initialize` handshake, newest
/// first.
pub const HANDSHAKE_PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Pick the revision to use for a client that asked for `requested` in
/// `initialize`: the same one if it is a handshake revision we support, else
/// our latest handshake revision (the client then decides whether to go on).
/// Never a stateless revision: those have no handshake.
pub fn negotiate_protocol_version(requested: &str) -> &'static str {
    HANDSHAKE_PROTOCOL_VERSIONS.iter().find(|v| **v == requested).copied().unwrap_or(LATEST_HANDSHAKE_PROTOCOL_VERSION)
}

/// Whether `version` is a stateless revision this crate supports.
pub fn is_stateless_protocol_version(version: &str) -> bool {
    STATELESS_PROTOCOL_VERSIONS.contains(&version)
}

/// `_meta` key (requests, 2026-07-28+): the protocol revision of the request.
pub const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
/// `_meta` key (requests, 2026-07-28+): the client's [`Implementation`].
pub const META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
/// `_meta` key (requests, 2026-07-28+): the client's [`ClientCapabilities`].
pub const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
/// `_meta` key (requests, 2026-07-28+): the lowest [`LoggingLevel`] to send
/// `notifications/message` for; none are sent without it.
pub const META_LOG_LEVEL: &str = "io.modelcontextprotocol/logLevel";
/// `_meta` key (results, 2026-07-28+): the server's [`Implementation`].
pub const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";
/// `_meta` key (notifications, 2026-07-28+): the id of the
/// `subscriptions/listen` request a notification belongs to.
pub const META_SUBSCRIPTION_ID: &str = "io.modelcontextprotocol/subscriptionId";

/// Who may cache a result (2026-07-28+), like HTTP `Cache-Control`
/// `public`/`private`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheScope {
    /// Holds no user-specific data: shared caches may serve it to anyone.
    #[default]
    Public,
    /// Only reusable within the same authorization context.
    Private,
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

impl ClientCapabilities {
    fn has_sub(capability: &Option<Value>, name: &str) -> bool {
        capability.as_ref().and_then(|c| c.get(name)).is_some_and(|v| !v.is_null())
    }

    /// Whether the client accepts form mode elicitation: `elicitation: {}`
    /// (form only, as before 2025-11-25) or `elicitation: {form: {}}`.
    pub fn supports_elicitation_form(&self) -> bool {
        self.elicitation.is_some()
            && (Self::has_sub(&self.elicitation, "form") || !Self::has_sub(&self.elicitation, "url"))
    }

    /// Whether the client accepts URL mode elicitation:
    /// `elicitation: {url: {}}` (2025-11-25+).
    pub fn supports_elicitation_url(&self) -> bool {
        Self::has_sub(&self.elicitation, "url")
    }

    /// Whether the client accepts tools in sampling requests:
    /// `sampling: {tools: {}}` (2025-11-25+).
    pub fn supports_sampling_tools(&self) -> bool {
        Self::has_sub(&self.sampling, "tools")
    }
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

/// The result of `server/discover` (2026-07-28+). `resultType`, `ttlMs`,
/// `cacheScope` and `_meta` are added when it is sent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverResult {
    pub supported_versions: Vec<String>,
    pub capabilities: ServerCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

/// Which notifications a `subscriptions/listen` stream carries
/// (2026-07-28+).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools_list_changed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompts_list_changed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources_list_changed: Option<bool>,
    /// URIs to receive `notifications/resources/updated` for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_subscriptions: Option<Vec<String>>,
    /// Tasks to receive `notifications/tasks` status updates for (tasks
    /// extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_ids: Option<Vec<String>>,
}

/// `subscriptions/listen` parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ListenParams {
    #[serde(default)]
    pub notifications: SubscriptionFilter,
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

    /// Claude Code: let this tool's text results reach `chars` characters
    /// (capped at [`anthropic::MAX_RESULT_SIZE_CHARS_LIMIT`]) before they are
    /// saved to a file instead of shown inline.
    pub fn max_result_size_chars(self, chars: u32) -> Self {
        self.meta(anthropic::MAX_RESULT_SIZE_CHARS, chars.min(anthropic::MAX_RESULT_SIZE_CHARS_LIMIT))
    }

    /// Claude Code: ask the user for permission on every call of this tool,
    /// even in bypass modes and despite allow rules.
    pub fn requires_user_interaction(self) -> Self {
        self.meta(anthropic::REQUIRES_USER_INTERACTION, true)
    }

    /// Claude Code: load this tool upfront instead of deferring it behind
    /// tool search.
    pub fn always_load(self) -> Self {
        self.meta(anthropic::ALWAYS_LOAD, true)
    }
}

/// Claude Code's `_meta` keys for tools, see
/// <https://code.claude.com/docs/en/mcp>.
pub mod anthropic {
    /// A number of characters: how large the tool's text results may be before
    /// Claude Code saves them to a file (default 50,000).
    pub const MAX_RESULT_SIZE_CHARS: &str = "anthropic/maxResultSizeChars";
    /// Claude Code's ceiling for [`MAX_RESULT_SIZE_CHARS`].
    pub const MAX_RESULT_SIZE_CHARS_LIMIT: u32 = 500_000;
    /// `true`: Claude Code asks for permission on every call of the tool.
    pub const REQUIRES_USER_INTERACTION: &str = "anthropic/requiresUserInteraction";
    /// `true`: Claude Code loads the tool upfront rather than through tool
    /// search.
    pub const ALWAYS_LOAD: &str = "anthropic/alwaysLoad";
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
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
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
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
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

/// The model asking to call a tool (sampling with tools, 2025-11-25+).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolUseContent {
    /// Identifies this tool use; the matching result's `toolUseId`.
    pub id: String,
    pub name: String,
    pub input: JsonObject,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

/// The result of a tool use, sent back to the model (sampling with tools,
/// 2025-11-25+).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultContent {
    /// The `id` of the [`ToolUseContent`] this answers.
    pub tool_use_id: String,
    pub content: Vec<Content>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

/// A piece of content in a sampling message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SamplingContent {
    Text(TextContent),
    Image(MediaContent),
    Audio(MediaContent),
    /// In assistant messages: the model wants a tool called.
    ToolUse(ToolUseContent),
    /// In user messages: the result of a tool use. A message holding tool
    /// results must hold nothing else.
    ToolResult(ToolResultContent),
}

impl SamplingContent {
    pub fn text(text: impl Into<String>) -> Self {
        SamplingContent::Text(TextContent { text: text.into(), ..Default::default() })
    }

    pub fn image(base64_data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        SamplingContent::Image(MediaContent {
            data: base64_data.into(),
            mime_type: mime_type.into(),
            ..Default::default()
        })
    }

    pub fn audio(base64_data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        SamplingContent::Audio(MediaContent {
            data: base64_data.into(),
            mime_type: mime_type.into(),
            ..Default::default()
        })
    }

    pub fn tool_use(id: impl Into<String>, name: impl Into<String>, input: JsonObject) -> Self {
        SamplingContent::ToolUse(ToolUseContent { id: id.into(), name: name.into(), input, meta: None })
    }

    pub fn tool_result(tool_use_id: impl Into<String>, content: Vec<Content>) -> Self {
        SamplingContent::ToolResult(ToolResultContent {
            tool_use_id: tool_use_id.into(),
            content,
            ..Default::default()
        })
    }

    /// The text, if this is text content.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            SamplingContent::Text(t) => Some(&t.text),
            _ => None,
        }
    }

    fn is_tool_related(&self) -> bool {
        matches!(self, SamplingContent::ToolUse(_) | SamplingContent::ToolResult(_))
    }
}

/// Sampling content is a single block or an array of them on the wire; a
/// single block is sent as an object, so that older clients understand it.
mod one_or_many {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T: Serialize, S: Serializer>(items: &[T], s: S) -> Result<S::Ok, S::Error> {
        match items {
            [one] => one.serialize(s),
            many => many.serialize(s),
        }
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(d: D) -> Result<Vec<T>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany<T> {
            Many(Vec<T>),
            One(T),
        }
        Ok(match OneOrMany::deserialize(d)? {
            OneOrMany::Many(v) => v,
            OneOrMany::One(t) => vec![t],
        })
    }
}

/// A message for, or from, the client's LLM (sampling).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SamplingMessage {
    pub role: Role,
    /// One or more blocks (several are sent as an array, 2025-11-25+).
    #[serde(with = "one_or_many")]
    pub content: Vec<SamplingContent>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl SamplingMessage {
    pub fn new(role: Role, content: Vec<SamplingContent>) -> Self {
        SamplingMessage { role, content, meta: None }
    }

    /// A user message with some text.
    pub fn user(text: impl Into<String>) -> Self {
        Self::new(Role::User, vec![SamplingContent::text(text)])
    }

    /// An assistant message with some text.
    pub fn assistant(text: impl Into<String>) -> Self {
        Self::new(Role::Assistant, vec![SamplingContent::text(text)])
    }

    /// A user message answering tool uses (it must hold only tool results).
    pub fn tool_results(results: Vec<ToolResultContent>) -> Self {
        Self::new(Role::User, results.into_iter().map(SamplingContent::ToolResult).collect())
    }
}

/// How the model may use the tools of a sampling request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolChoiceMode {
    /// The model decides (the default).
    Auto,
    /// The model must use at least one tool.
    Required,
    /// The model must not use tools.
    None,
}

/// `toolChoice` in a sampling request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolChoice {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ToolChoiceMode>,
}

impl ToolChoice {
    pub fn auto() -> Self {
        ToolChoice { mode: Some(ToolChoiceMode::Auto) }
    }

    pub fn required() -> Self {
        ToolChoice { mode: Some(ToolChoiceMode::Required) }
    }

    pub fn none() -> Self {
        ToolChoice { mode: Some(ToolChoiceMode::None) }
    }
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
    /// Tools the model may call (2025-11-25+; needs the client's
    /// `sampling.tools` capability).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
}

impl CreateMessageParams {
    /// Whether this is a tool-enabled request: it offers tools, sets a tool
    /// choice, or carries tool uses or results.
    pub fn uses_tools(&self) -> bool {
        self.tools.is_some()
            || self.tool_choice.is_some()
            || self.messages.iter().flat_map(|m| &m.content).any(SamplingContent::is_tool_related)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageResult {
    pub role: Role,
    /// One or more blocks; several tool uses may come at once.
    #[serde(with = "one_or_many")]
    pub content: Vec<SamplingContent>,
    pub model: String,
    /// `endTurn`, `stopSequence`, `maxTokens`, `toolUse`, or a
    /// provider-specific reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
}

impl CreateMessageResult {
    /// The first text block, if any.
    pub fn text(&self) -> Option<&str> {
        self.content.iter().find_map(SamplingContent::as_text)
    }

    /// The tool uses the model asks for.
    pub fn tool_uses(&self) -> impl Iterator<Item = &ToolUseContent> {
        self.content.iter().filter_map(|c| match c {
            SamplingContent::ToolUse(t) => Some(t),
            _ => None,
        })
    }
}

/// Form mode elicitation: ask the user for structured data, through the
/// client. Not for secrets (use URL mode).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElicitFormParams {
    pub message: String,
    /// A flat object schema of primitive properties.
    pub requested_schema: Value,
}

/// URL mode elicitation (2025-11-25+): send the user to a URL for an
/// interaction the client must not see, such as entering credentials or a
/// third-party OAuth flow. Serialized with `"mode": "url"`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "mode", rename = "url")]
pub struct ElicitUrlParams {
    pub message: String,
    /// Identifies the elicitation, in `notifications/elicitation/complete`
    /// (2025-11-25). Not sent to 2026-07-28 clients, which learn the outcome
    /// by retrying the request: keep what you need in the request state.
    pub elicitation_id: String,
    pub url: String,
}

/// `elicitation/create` parameters: ask the user for input, in form or URL
/// mode.
#[derive(Clone, Debug, PartialEq)]
pub enum ElicitParams {
    Form(ElicitFormParams),
    Url(ElicitUrlParams),
}

impl ElicitParams {
    /// Ask for data matching `requested_schema`, a flat object schema of
    /// primitive properties.
    pub fn form(message: impl Into<String>, requested_schema: Value) -> Self {
        ElicitParams::Form(ElicitFormParams { message: message.into(), requested_schema })
    }

    /// Send the user to `url`. Use a fresh, unique `elicitation_id`, and bind
    /// it to the user, not just the session.
    pub fn url(message: impl Into<String>, url: impl Into<String>, elicitation_id: impl Into<String>) -> Self {
        ElicitParams::Url(ElicitUrlParams {
            message: message.into(),
            elicitation_id: elicitation_id.into(),
            url: url.into(),
        })
    }

    pub fn message(&self) -> &str {
        match self {
            ElicitParams::Form(p) => &p.message,
            ElicitParams::Url(p) => &p.message,
        }
    }
}

impl From<ElicitFormParams> for ElicitParams {
    fn from(p: ElicitFormParams) -> Self {
        ElicitParams::Form(p)
    }
}

impl From<ElicitUrlParams> for ElicitParams {
    fn from(p: ElicitUrlParams) -> Self {
        ElicitParams::Url(p)
    }
}

// Form mode goes without `mode`, which every revision understands.
impl Serialize for ElicitParams {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ElicitParams::Form(p) => p.serialize(s),
            ElicitParams::Url(p) => p.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for ElicitParams {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let v = Value::deserialize(d)?;
        match v.get("mode").and_then(Value::as_str) {
            None | Some("form") => serde_json::from_value(v).map(ElicitParams::Form),
            Some("url") => serde_json::from_value(v).map(ElicitParams::Url),
            Some(other) => return Err(D::Error::custom(format!("unknown elicitation mode: {other}"))),
        }
        .map_err(D::Error::custom)
    }
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
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonObject>,
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
    fn anthropic_tool_meta() {
        let tool = Tool::new("t", "d").max_result_size_chars(200_000).requires_user_interaction().always_load();
        assert_eq!(
            serde_json::to_value(&tool).unwrap()["_meta"],
            json!({
                "anthropic/maxResultSizeChars": 200000,
                "anthropic/requiresUserInteraction": true,
                "anthropic/alwaysLoad": true
            })
        );
        let capped = Tool::new("t", "d").max_result_size_chars(u32::MAX);
        assert_eq!(capped.meta.unwrap()[anthropic::MAX_RESULT_SIZE_CHARS], 500_000);
    }

    #[test]
    fn elicit_params_modes() {
        let form = ElicitParams::form("Name?", json!({"type":"object"}));
        assert_eq!(
            serde_json::to_value(&form).unwrap(),
            json!({"message":"Name?","requestedSchema":{"type":"object"}})
        );
        let url = ElicitParams::url("Set your key", "https://example.com/key", "e1");
        let wire = json!({"mode":"url","message":"Set your key","elicitationId":"e1","url":"https://example.com/key"});
        assert_eq!(serde_json::to_value(&url).unwrap(), wire);

        assert_eq!(serde_json::from_value::<ElicitParams>(wire).unwrap(), url);
        let explicit_form = json!({"mode":"form","message":"Name?","requestedSchema":{"type":"object"}});
        assert_eq!(serde_json::from_value::<ElicitParams>(explicit_form).unwrap(), form);
        assert!(serde_json::from_value::<ElicitParams>(json!({"mode":"carrier-pigeon","message":"x"})).is_err());
        assert!(serde_json::from_value::<ElicitParams>(json!({"mode":"url","message":"x"})).is_err());
    }

    #[test]
    fn url_elicitation_required_error() {
        let ElicitParams::Url(p) = ElicitParams::url("Authorize", "https://example.com/connect", "e2") else {
            unreachable!()
        };
        let err = crate::jsonrpc::ErrorObject::url_elicitation_required("Needs authorization", vec![p]);
        assert_eq!(
            serde_json::to_value(err).unwrap(),
            json!({"code":-32042,"message":"Needs authorization","data":{"elicitations":[
                {"mode":"url","message":"Authorize","elicitationId":"e2","url":"https://example.com/connect"}
            ]}})
        );
    }

    #[test]
    fn client_capability_helpers() {
        let caps = |v: Value| serde_json::from_value::<ClientCapabilities>(v).unwrap();
        let none = caps(json!({}));
        assert!(!none.supports_elicitation_form() && !none.supports_elicitation_url());
        let legacy = caps(json!({"elicitation": {}}));
        assert!(legacy.supports_elicitation_form() && !legacy.supports_elicitation_url());
        let both = caps(json!({"elicitation": {"form": {}, "url": {}}}));
        assert!(both.supports_elicitation_form() && both.supports_elicitation_url());
        let url_only = caps(json!({"elicitation": {"url": {}}}));
        assert!(!url_only.supports_elicitation_form() && url_only.supports_elicitation_url());
        assert!(!caps(json!({"sampling": {}})).supports_sampling_tools());
        assert!(caps(json!({"sampling": {"tools": {}}})).supports_sampling_tools());
    }

    #[test]
    fn sampling_with_tools() {
        let params = CreateMessageParams {
            messages: vec![
                SamplingMessage::user("Weather in Paris?"),
                SamplingMessage::new(
                    Role::Assistant,
                    vec![SamplingContent::tool_use(
                        "call_1",
                        "get_weather",
                        json!({"city":"Paris"}).as_object().unwrap().clone(),
                    )],
                ),
                SamplingMessage::tool_results(vec![ToolResultContent {
                    tool_use_id: "call_1".into(),
                    content: vec![Content::text("18°C")],
                    ..Default::default()
                }]),
            ],
            max_tokens: 100,
            tools: Some(vec![Tool::new("get_weather", "Get the weather")]),
            tool_choice: Some(ToolChoice::auto()),
            ..Default::default()
        };
        assert!(params.uses_tools());
        let v = serde_json::to_value(&params).unwrap();
        // A single block stays an object, as older clients expect.
        assert_eq!(v["messages"][0]["content"], json!({"type":"text","text":"Weather in Paris?"}));
        assert_eq!(
            v["messages"][1]["content"],
            json!({"type":"tool_use","id":"call_1","name":"get_weather","input":{"city":"Paris"}})
        );
        assert_eq!(
            v["messages"][2]["content"],
            json!({"type":"tool_result","toolUseId":"call_1","content":[{"type":"text","text":"18°C"}]})
        );
        assert_eq!(v["tools"][0]["name"], "get_weather");
        assert_eq!(v["toolChoice"], json!({"mode":"auto"}));
        assert_eq!(serde_json::from_value::<CreateMessageParams>(v).unwrap(), params);

        assert!(
            !CreateMessageParams { messages: vec![SamplingMessage::user("hi")], ..Default::default() }.uses_tools()
        );

        let res: CreateMessageResult = serde_json::from_value(json!({
            "role": "assistant",
            "content": [
                {"type":"tool_use","id":"a","name":"get_weather","input":{"city":"Paris"}},
                {"type":"tool_use","id":"b","name":"get_weather","input":{"city":"London"}}
            ],
            "model": "m",
            "stopReason": "toolUse"
        }))
        .unwrap();
        assert_eq!(res.tool_uses().map(|t| t.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(res.text(), None);
        let v = serde_json::to_value(&res).unwrap();
        assert!(v["content"].is_array());
    }

    #[test]
    fn negotiates() {
        assert_eq!(negotiate_protocol_version("2025-03-26"), "2025-03-26");
        assert_eq!(negotiate_protocol_version("1999-01-01"), LATEST_HANDSHAKE_PROTOCOL_VERSION);
        assert_eq!(negotiate_protocol_version("2026-07-28"), LATEST_HANDSHAKE_PROTOCOL_VERSION);
    }
}
