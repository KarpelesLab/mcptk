//! MCP Apps: interactive HTML user interfaces served by an MCP server.
//!
//! [MCP Apps] is the official `io.modelcontextprotocol/ui` extension
//! (SEP-1865, stable revision 2026-01-26). It standardizes what the OpenAI
//! Apps SDK (`openai/outputTemplate`) and MCP-UI did before it:
//!
//! 1. The server exposes an HTML page as a resource with a `ui://` URI and
//!    the MIME type [`MIME_TYPE`] (`text/html;profile=mcp-app`). Build it
//!    with [`UiResource`], which also carries the page's sandbox settings
//!    (CSP domains, browser permissions, border preference) in `_meta.ui`.
//! 2. A tool points at that page with `_meta.ui.resourceUri`
//!    ([`ToolUiExt::ui_resource`]). When a host that supports the extension
//!    calls the tool, it reads the resource, renders it in a sandboxed iframe,
//!    and hands the iframe the tool's arguments and result.
//! 3. The page talks to the host with JSON-RPC over `postMessage` (`ui/*`
//!    methods): it can call this server's tools through the host, send chat
//!    messages, update the model's context, open links... That protocol is
//!    between the host and the page; the server only sees ordinary
//!    `tools/call` and `resources/read` requests.
//!
//! ```no_run
//! use mcptk::apps::{AppsBuilderExt, ToolUiExt, UiResource};
//! use mcptk::{Server, Tool, ToolError};
//!
//! let page = UiResource::new("ui://clock/view", "Clock", "<!doctype html><html>...</html>")
//!     .resource_domain("https://cdn.jsdelivr.net")
//!     .prefers_border(true);
//!
//! let server = Server::builder("clock", "1.0")
//!     .ui_resource(page) // registers the resource and declares the extension
//!     .tool(Tool::new("now", "Show the time").ui_resource("ui://clock/view"), |_ctx, _args| async move {
//!         // Always return meaningful text: hosts without MCP Apps only see this.
//!         Ok::<_, ToolError>("It is 12:00.")
//!     })
//!     .build();
//! ```
//!
//! ## What a server author needs to know
//!
//! - **Tool results stay normal.** Return `content` the model (and text-only
//!   hosts) can use, and put the data the page renders in
//!   `structuredContent` (e.g. with [`Json`](crate::Json)). The host forwards
//!   the whole result to the page (`ui/notifications/tool-result`), and the
//!   arguments too (`ui/notifications/tool-input`).
//! - **Visibility.** By default a linked tool is visible to the model and
//!   callable by the page. Mark helper tools the page calls (refresh, paging,
//!   form submission) with [`ToolUiExt::app_only`]: hosts then hide them from
//!   the model. Hosts that don't support the extension still list them; use
//!   [`hide_app_only_tools_without_ui`] as a tool filter to hide them there.
//! - **Calls from the page are ordinary `tools/call` requests** proxied by the
//!   host. Validate them like any other input.
//! - **CSP.** By default the page may load nothing from the network beyond
//!   inline scripts and styles and `data:` images. Declare every origin it
//!   fetches from ([`UiResource::connect_domain`]) or loads scripts, styles,
//!   fonts, images and media from ([`UiResource::resource_domain`]). Hosts
//!   never loosen the policy beyond what you declare.
//! - **Fallback.** Check [`UiSupport::supports_ui`] when a tool should behave
//!   differently for hosts that can't render the page.
//! - **Claude Code** (a terminal host) doesn't render apps: it hides `ui://`
//!   and `text/html;profile=mcp-app` resources from `@` suggestions and its
//!   resource list tool, but a read by URI still works.
//!
//! [MCP Apps]: https://github.com/modelcontextprotocol/ext-apps

use crate::ServerBuilder;
use crate::server::{RequestContext, Session};
use crate::types::{ClientCapabilities, JsonObject, ReadResourceResult, Resource, ResourceContents, Tool};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The extension identifier, the key in `capabilities.extensions`.
pub const EXTENSION_ID: &str = "io.modelcontextprotocol/ui";

/// The MIME type of an MCP App HTML resource.
pub const MIME_TYPE: &str = "text/html;profile=mcp-app";

/// The URI scheme UI resources must use.
pub const URI_SCHEME: &str = "ui://";

/// Deprecated flat tool `_meta` key for the UI resource URI, superseded by
/// `_meta.ui.resourceUri`. [`ToolUiExt::ui_resource`] sets both, like the
/// official SDK, for hosts that only know the old form.
pub const LEGACY_RESOURCE_URI_META_KEY: &str = "ui/resourceUri";

/// The `_meta` key of the per-request client capabilities (protocol
/// revision 2026-07-28 and later, where there is no `initialize`).
const CLIENT_CAPABILITIES_META_KEY: &str = "io.modelcontextprotocol/clientCapabilities";

/// Who may call a tool linked to a UI.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// The model sees the tool and can call it.
    Model,
    /// The UI page can call the tool (through the host, on this server only).
    App,
}

/// Content Security Policy origins a UI page needs. Empty lists are omitted,
/// which means "none" (or `'self'` for `baseUriDomains`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Csp {
    /// Origins for fetch, XHR and WebSocket (`connect-src`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connect_domains: Vec<String>,
    /// Origins for scripts, styles, images, fonts and media (`script-src`,
    /// `style-src`, `img-src`, `font-src`, `media-src`). Wildcard subdomains
    /// such as `https://*.example.com` are allowed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_domains: Vec<String>,
    /// Origins for nested iframes (`frame-src`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frame_domains: Vec<String>,
    /// Allowed document base URIs (`base-uri`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub base_uri_domains: Vec<String>,
}

impl Csp {
    fn is_empty(&self) -> bool {
        self.connect_domains.is_empty()
            && self.resource_domains.is_empty()
            && self.frame_domains.is_empty()
            && self.base_uri_domains.is_empty()
    }
}

/// Browser features a UI page asks for (Permission Policy). Hosts may refuse
/// them; pages should feature-detect.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Permissions {
    pub camera: bool,
    pub microphone: bool,
    pub geolocation: bool,
    pub clipboard_write: bool,
}

impl Permissions {
    fn is_empty(&self) -> bool {
        *self == Permissions::default()
    }
}

/// Serialized as `{"camera": {}, ...}` with only the requested features.
impl Serialize for Permissions {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = JsonObject::new();
        for (on, key) in [
            (self.camera, "camera"),
            (self.microphone, "microphone"),
            (self.geolocation, "geolocation"),
            (self.clipboard_write, "clipboardWrite"),
        ] {
            if on {
                map.insert(key.to_string(), json!({}));
            }
        }
        map.serialize(serializer)
    }
}

/// The `_meta.ui` object of a UI resource: how the host should sandbox and
/// frame the page.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UiResourceMeta {
    #[serde(skip_serializing_if = "Csp::is_empty")]
    pub csp: Csp,
    #[serde(skip_serializing_if = "Permissions::is_empty")]
    pub permissions: Permissions,
    /// A dedicated sandbox origin; its format is host-specific.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    /// Whether the host should draw a border and background around the page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefers_border: Option<bool>,
}

impl UiResourceMeta {
    /// The `_meta.ui` value.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| json!({}))
    }

    /// The same settings under the OpenAI Apps SDK compatibility keys
    /// (`openai/widgetCSP`, `openai/widgetDomain`, `openai/widgetPrefersBorder`).
    fn openai_keys(&self) -> JsonObject {
        let mut meta = JsonObject::new();
        if !self.csp.is_empty() {
            let mut csp = JsonObject::new();
            for (list, key) in [
                (&self.csp.connect_domains, "connect_domains"),
                (&self.csp.resource_domains, "resource_domains"),
                (&self.csp.frame_domains, "frame_domains"),
            ] {
                if !list.is_empty() {
                    csp.insert(key.to_string(), json!(list));
                }
            }
            meta.insert("openai/widgetCSP".to_string(), Value::Object(csp));
        }
        if let Some(domain) = &self.domain {
            meta.insert("openai/widgetDomain".to_string(), json!(domain));
        }
        if let Some(border) = self.prefers_border {
            meta.insert("openai/widgetPrefersBorder".to_string(), json!(border));
        }
        meta
    }
}

/// An MCP App page: a `ui://` resource holding an HTML document, with its
/// `_meta.ui` sandbox settings.
///
/// Register it with [`AppsBuilderExt::ui_resource`], or build the parts
/// yourself: [`UiResource::resource`] for the listing and
/// [`UiResource::contents`] / [`UiResource::contents_for`] for reads.
#[derive(Clone, Debug)]
pub struct UiResource {
    uri: String,
    name: String,
    title: Option<String>,
    description: Option<String>,
    html: String,
    ui: UiResourceMeta,
    extra_meta: JsonObject,
    meta_in_listing: bool,
    openai: bool,
}

impl UiResource {
    /// A page at `uri`, which should start with `ui://` (a warning is logged
    /// otherwise), holding a complete HTML5 document.
    pub fn new(uri: impl Into<String>, name: impl Into<String>, html: impl Into<String>) -> Self {
        let uri = uri.into();
        if !uri.starts_with(URI_SCHEME) {
            tracing::warn!(%uri, "MCP Apps UI resource URIs must use the ui:// scheme");
        }
        UiResource {
            uri,
            name: name.into(),
            title: None,
            description: None,
            html: html.into(),
            ui: UiResourceMeta::default(),
            extra_meta: JsonObject::new(),
            meta_in_listing: true,
            openai: false,
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn html(&self) -> &str {
        &self.html
    }

    /// The `_meta.ui` settings.
    pub fn ui_meta(&self) -> &UiResourceMeta {
        &self.ui
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Replace the whole CSP declaration.
    pub fn csp(mut self, csp: Csp) -> Self {
        self.ui.csp = csp;
        self
    }

    /// Allow fetch/XHR/WebSocket to `origin` (e.g. `https://api.example.com`).
    pub fn connect_domain(mut self, origin: impl Into<String>) -> Self {
        self.ui.csp.connect_domains.push(origin.into());
        self
    }

    /// Allow scripts, styles, images, fonts and media from `origin`.
    pub fn resource_domain(mut self, origin: impl Into<String>) -> Self {
        self.ui.csp.resource_domains.push(origin.into());
        self
    }

    /// Allow nested iframes from `origin`.
    pub fn frame_domain(mut self, origin: impl Into<String>) -> Self {
        self.ui.csp.frame_domains.push(origin.into());
        self
    }

    /// Allow `origin` as the document's base URI.
    pub fn base_uri_domain(mut self, origin: impl Into<String>) -> Self {
        self.ui.csp.base_uri_domains.push(origin.into());
        self
    }

    /// Replace the requested browser permissions.
    pub fn permissions(mut self, permissions: Permissions) -> Self {
        self.ui.permissions = permissions;
        self
    }

    pub fn camera(mut self) -> Self {
        self.ui.permissions.camera = true;
        self
    }

    pub fn microphone(mut self) -> Self {
        self.ui.permissions.microphone = true;
        self
    }

    pub fn geolocation(mut self) -> Self {
        self.ui.permissions.geolocation = true;
        self
    }

    pub fn clipboard_write(mut self) -> Self {
        self.ui.permissions.clipboard_write = true;
        self
    }

    /// Ask for a dedicated sandbox origin. The format is host-specific (e.g.
    /// `{hash}.claudemcpcontent.com`, `www-example-com.oaiusercontent.com`).
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.ui.domain = Some(domain.into());
        self
    }

    /// Ask the host for (`true`) or against (`false`) a visible border and
    /// background. Set it explicitly: host defaults vary.
    pub fn prefers_border(mut self, border: bool) -> Self {
        self.ui.prefers_border = Some(border);
        self
    }

    /// Set another `_meta` entry, on both the listing and the contents.
    pub fn meta(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.extra_meta.insert(key.into(), value.into());
        self
    }

    /// Whether `_meta.ui` also goes on the `resources/list` entry, so hosts
    /// can review the sandbox settings without reading the page (default
    /// `true`). It is always on the `resources/read` contents, which take
    /// precedence.
    pub fn meta_in_listing(mut self, enabled: bool) -> Self {
        self.meta_in_listing = enabled;
        self
    }

    /// Also emit the OpenAI Apps SDK compatibility keys (`openai/widgetCSP`,
    /// `openai/widgetDomain`, `openai/widgetPrefersBorder`). ChatGPT reads the
    /// standard `_meta.ui` fields, so this is only for older hosts.
    pub fn openai_compat(mut self) -> Self {
        self.openai = true;
        self
    }

    fn meta_object(&self) -> JsonObject {
        let mut meta = JsonObject::new();
        let ui = self.ui.to_value();
        if ui.as_object().is_some_and(|o| !o.is_empty()) {
            meta.insert("ui".to_string(), ui);
        }
        if self.openai {
            meta.extend(self.ui.openai_keys());
        }
        meta.extend(self.extra_meta.clone());
        meta
    }

    /// The `resources/list` entry.
    pub fn resource(&self) -> Resource {
        let mut meta = self.meta_object();
        if !self.meta_in_listing {
            meta.remove("ui");
        }
        Resource {
            uri: self.uri.clone(),
            name: self.name.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            mime_type: Some(MIME_TYPE.to_string()),
            meta: (!meta.is_empty()).then_some(meta),
            ..Default::default()
        }
    }

    /// The `resources/read` contents, with the page's HTML.
    pub fn contents(&self) -> ResourceContents {
        self.contents_for(self.html.clone())
    }

    /// `resources/read` contents with other HTML (e.g. rendered per request)
    /// and this resource's URI, MIME type and `_meta`.
    pub fn contents_for(&self, html: impl Into<String>) -> ResourceContents {
        let meta = self.meta_object();
        ResourceContents::Text {
            uri: self.uri.clone(),
            mime_type: Some(MIME_TYPE.to_string()),
            text: html.into(),
            meta: (!meta.is_empty()).then_some(meta),
        }
    }

    /// The whole `resources/read` result.
    pub fn read_result(&self) -> ReadResourceResult {
        ReadResourceResult { contents: vec![self.contents()], meta: None }
    }
}

/// MCP Apps helpers for [`Tool`].
pub trait ToolUiExt: Sized {
    /// Render this tool's results with the UI resource at `uri`: sets
    /// `_meta.ui.resourceUri`, and the deprecated flat `_meta["ui/resourceUri"]`
    /// for older hosts.
    fn ui_resource(self, uri: impl Into<String>) -> Self;

    /// Set who may call the tool (`_meta.ui.visibility`). The default, when
    /// unset, is model and app.
    fn ui_visibility(self, visibility: &[Visibility]) -> Self;

    /// Only the UI page may call this tool; hosts hide it from the model.
    fn app_only(self) -> Self {
        self.ui_visibility(&[Visibility::App])
    }

    /// Only the model may call this tool; hosts reject calls from the page.
    fn model_only(self) -> Self {
        self.ui_visibility(&[Visibility::Model])
    }

    /// Mirror the MCP Apps settings into the OpenAI Apps SDK compatibility
    /// keys: `openai/outputTemplate` (the resource URI),
    /// `openai/widgetAccessible` (callable by the page) and
    /// `openai/visibility` (`public`/`private` to the model). Call it after
    /// [`ui_resource`](Self::ui_resource) and
    /// [`ui_visibility`](Self::ui_visibility). ChatGPT reads the standard
    /// keys, so this is only for older hosts.
    fn openai_compat(self) -> Self;

    /// The UI resource URI, from `_meta.ui.resourceUri` or the legacy key.
    fn ui_resource_uri(&self) -> Option<&str>;

    /// Who may call the tool (model and app when unset).
    fn visibility(&self) -> Vec<Visibility>;
}

impl ToolUiExt for Tool {
    fn ui_resource(mut self, uri: impl Into<String>) -> Self {
        let uri = uri.into();
        ui_meta_mut(&mut self).insert("resourceUri".to_string(), json!(uri));
        self.meta(LEGACY_RESOURCE_URI_META_KEY, uri)
    }

    fn ui_visibility(mut self, visibility: &[Visibility]) -> Self {
        ui_meta_mut(&mut self).insert("visibility".to_string(), json!(visibility));
        self
    }

    fn openai_compat(mut self) -> Self {
        if let Some(uri) = self.ui_resource_uri().map(str::to_string) {
            self = self.meta("openai/outputTemplate", uri);
        }
        let visibility = self.visibility();
        let model = if visibility.contains(&Visibility::Model) { "public" } else { "private" };
        self.meta("openai/widgetAccessible", visibility.contains(&Visibility::App)).meta("openai/visibility", model)
    }

    fn ui_resource_uri(&self) -> Option<&str> {
        let meta = self.meta.as_ref()?;
        meta.get("ui")
            .and_then(|ui| ui.get("resourceUri"))
            .or_else(|| meta.get(LEGACY_RESOURCE_URI_META_KEY))
            .and_then(Value::as_str)
    }

    fn visibility(&self) -> Vec<Visibility> {
        self.meta
            .as_ref()
            .and_then(|m| m.get("ui"))
            .and_then(|ui| ui.get("visibility"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_else(|| vec![Visibility::Model, Visibility::App])
    }
}

/// The `_meta.ui` object of a tool, created (or replaced, if it isn't an
/// object) as needed.
fn ui_meta_mut(tool: &mut Tool) -> &mut JsonObject {
    let ui = tool.meta.get_or_insert_with(JsonObject::new).entry("ui").or_insert_with(|| json!({}));
    if !ui.is_object() {
        *ui = json!({});
    }
    ui.as_object_mut().expect("just made an object")
}

/// MCP Apps helpers for [`ServerBuilder`].
pub trait AppsBuilderExt: Sized {
    /// Declare the extension in `capabilities.extensions` (with empty
    /// settings). [`ui_resource`](Self::ui_resource) does it too.
    fn ui_apps(self) -> Self;

    /// Register a static UI page and declare the extension. For HTML built per
    /// request, register [`UiResource::resource`] with
    /// [`ServerBuilder::resource`] and return [`UiResource::contents_for`].
    fn ui_resource(self, resource: UiResource) -> Self;
}

impl AppsBuilderExt for ServerBuilder {
    fn ui_apps(self) -> Self {
        self.extension(EXTENSION_ID, json!({}))
    }

    fn ui_resource(self, page: UiResource) -> Self {
        let contents = page.contents();
        self.ui_apps().resource(page.resource(), move |_ctx, _uri| {
            let contents = contents.clone();
            async move { Ok(contents) }
        })
    }
}

/// The client's `io.modelcontextprotocol/ui` extension settings.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiClientCapability {
    /// The UI content types the host can render, e.g. [`MIME_TYPE`].
    #[serde(default)]
    pub mime_types: Vec<String>,
    /// Other settings, for future versions of the extension.
    #[serde(flatten)]
    pub other: JsonObject,
}

impl UiClientCapability {
    /// Whether the host renders `text/html;profile=mcp-app` pages.
    pub fn supports_html(&self) -> bool {
        self.mime_types.iter().any(|m| m == MIME_TYPE)
    }
}

/// The client's MCP Apps settings, if it declared the extension.
pub fn ui_capability(capabilities: &ClientCapabilities) -> Option<UiClientCapability> {
    let value = capabilities.extensions.as_ref()?.get(EXTENSION_ID)?;
    serde_json::from_value(value.clone()).ok()
}

/// Whether the client declared the extension with [`MIME_TYPE`] among its
/// `mimeTypes`.
pub fn client_supports_ui(capabilities: &ClientCapabilities) -> bool {
    ui_capability(capabilities).is_some_and(|c| c.supports_html())
}

/// Whether the client on the other end can render MCP Apps, so tools can fall
/// back to plain text.
pub trait UiSupport {
    /// The client's MCP Apps settings, if it declared the extension.
    fn ui_capability(&self) -> Option<UiClientCapability>;

    /// Whether the client renders `text/html;profile=mcp-app` pages.
    fn supports_ui(&self) -> bool {
        self.ui_capability().is_some_and(|c| c.supports_html())
    }
}

/// From the capabilities the client sent at initialization.
impl UiSupport for Session {
    fn ui_capability(&self) -> Option<UiClientCapability> {
        ui_capability(self.client_capabilities()?)
    }
}

/// From the request's `_meta["io.modelcontextprotocol/clientCapabilities"]`
/// (protocol 2026-07-28, where each request carries them) if present, else
/// from the session.
impl UiSupport for RequestContext {
    fn ui_capability(&self) -> Option<UiClientCapability> {
        let per_request = self.meta().and_then(|m| m.get(CLIENT_CAPABILITIES_META_KEY));
        match per_request {
            Some(caps) => {
                serde_json::from_value::<ClientCapabilities>(caps.clone()).ok().and_then(|c| ui_capability(&c))
            }
            None => self.session().ui_capability(),
        }
    }
}

/// A [`ServerBuilder::tool_filter`] that hides app-only tools (visibility
/// without `model`) from clients that can't render MCP Apps, which would
/// otherwise show them to the model.
///
/// ```no_run
/// # use mcptk::Server;
/// let builder = Server::builder("app", "1.0").tool_filter(mcptk::apps::hide_app_only_tools_without_ui);
/// ```
pub fn hide_app_only_tools_without_ui(session: &Session, tool: &Tool) -> bool {
    tool.visibility().contains(&Visibility::Model) || session.supports_ui()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_meta() {
        let tool = Tool::new("show", "Show it").meta("anthropic/alwaysLoad", true).ui_resource("ui://x/view");
        assert_eq!(
            serde_json::to_value(&tool).unwrap()["_meta"],
            json!({
                "anthropic/alwaysLoad": true,
                "ui": {"resourceUri": "ui://x/view"},
                "ui/resourceUri": "ui://x/view"
            })
        );
        assert_eq!(tool.ui_resource_uri(), Some("ui://x/view"));
        assert_eq!(tool.visibility(), vec![Visibility::Model, Visibility::App]);

        let tool = tool.app_only();
        assert_eq!(tool.meta.as_ref().unwrap()["ui"], json!({"resourceUri": "ui://x/view", "visibility": ["app"]}));
        assert_eq!(tool.visibility(), vec![Visibility::App]);
    }

    #[test]
    fn tool_openai_compat() {
        let tool = Tool::new("show", "Show it").ui_resource("ui://x/view").openai_compat();
        let meta = tool.meta.unwrap();
        assert_eq!(meta["openai/outputTemplate"], "ui://x/view");
        assert_eq!(meta["openai/widgetAccessible"], true);
        assert_eq!(meta["openai/visibility"], "public");

        let tool = Tool::new("refresh", "Refresh").app_only().openai_compat();
        let meta = tool.meta.unwrap();
        assert!(meta.get("openai/outputTemplate").is_none());
        assert_eq!(meta["openai/widgetAccessible"], true);
        assert_eq!(meta["openai/visibility"], "private");
    }

    #[test]
    fn resource_json() {
        let page = UiResource::new("ui://x/view", "view", "<html></html>")
            .title("View")
            .connect_domain("https://api.example.com")
            .resource_domain("https://cdn.example.com")
            .clipboard_write()
            .camera()
            .domain("x.example.com")
            .prefers_border(false);
        let ui = json!({
            "csp": {
                "connectDomains": ["https://api.example.com"],
                "resourceDomains": ["https://cdn.example.com"]
            },
            "permissions": {"camera": {}, "clipboardWrite": {}},
            "domain": "x.example.com",
            "prefersBorder": false
        });
        assert_eq!(
            serde_json::to_value(page.resource()).unwrap(),
            json!({
                "uri": "ui://x/view",
                "name": "view",
                "title": "View",
                "mimeType": "text/html;profile=mcp-app",
                "_meta": {"ui": ui}
            })
        );
        assert_eq!(
            serde_json::to_value(page.read_result()).unwrap(),
            json!({"contents": [{
                "uri": "ui://x/view",
                "mimeType": "text/html;profile=mcp-app",
                "text": "<html></html>",
                "_meta": {"ui": ui}
            }]})
        );

        let listing = serde_json::to_value(page.clone().meta_in_listing(false).resource()).unwrap();
        assert!(listing.get("_meta").is_none());

        let compat = page.openai_compat().contents_for("<p>hi</p>");
        let ResourceContents::Text { text, meta, .. } = compat else { panic!("text contents") };
        assert_eq!(text, "<p>hi</p>");
        let meta = meta.unwrap();
        assert_eq!(
            meta["openai/widgetCSP"],
            json!({"connect_domains": ["https://api.example.com"], "resource_domains": ["https://cdn.example.com"]})
        );
        assert_eq!(meta["openai/widgetDomain"], "x.example.com");
        assert_eq!(meta["openai/widgetPrefersBorder"], false);
    }

    #[test]
    fn bare_resource_has_no_meta() {
        let page = UiResource::new("ui://x/view", "view", "<html></html>");
        assert_eq!(
            serde_json::to_value(page.resource()).unwrap(),
            json!({"uri": "ui://x/view", "name": "view", "mimeType": "text/html;profile=mcp-app"})
        );
        assert!(serde_json::to_value(page.contents()).unwrap().get("_meta").is_none());
    }

    #[test]
    fn client_capability() {
        let caps: ClientCapabilities =
            serde_json::from_value(json!({"extensions": {EXTENSION_ID: {"mimeTypes": [MIME_TYPE]}}})).unwrap();
        assert!(client_supports_ui(&caps));
        assert_eq!(ui_capability(&caps).unwrap().mime_types, vec![MIME_TYPE.to_string()]);

        let other: ClientCapabilities =
            serde_json::from_value(json!({"extensions": {EXTENSION_ID: {"mimeTypes": ["text/uri-list"]}}})).unwrap();
        assert!(!client_supports_ui(&other));
        assert!(!client_supports_ui(&ClientCapabilities::default()));
    }
}
