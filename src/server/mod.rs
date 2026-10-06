//! Building MCP servers.

mod input;
mod results;
mod session;
pub(crate) mod stateless;
pub mod tasks;
mod template;

pub use input::InputRequired;
pub use results::{IntoPromptResult, IntoReadResult, IntoToolResult, Json};
pub use session::{RequestContext, Session};
pub use template::match_uri_template;

pub(crate) use session::{Outbound, Outlet, dispatch_text};

use crate::channel::{self, ChannelEvent, PermissionRequest};
use crate::error::{Error, Result, ToolError};
use crate::types::*;
use serde::de::DeserializeOwned;
use serde_json::Value;
use session::SessionInner;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

type ToolFn = Arc<dyn Fn(RequestContext, Option<JsonObject>) -> BoxFuture<Result<CallToolResult>> + Send + Sync>;
type ResourceFn = Arc<dyn Fn(RequestContext, String) -> BoxFuture<Result<ReadResourceResult>> + Send + Sync>;
type TemplateFn =
    Arc<dyn Fn(RequestContext, String, HashMap<String, String>) -> BoxFuture<Result<ReadResourceResult>> + Send + Sync>;
type PromptFn =
    Arc<dyn Fn(RequestContext, HashMap<String, String>) -> BoxFuture<Result<GetPromptResult>> + Send + Sync>;
type CompletionFn = Arc<dyn Fn(RequestContext, CompleteParams) -> BoxFuture<Result<Completion>> + Send + Sync>;
type RequestFn = Arc<dyn Fn(RequestContext, Option<Value>) -> BoxFuture<Result<Value>> + Send + Sync>;
type NotificationFn = Arc<dyn Fn(Session, Option<Value>) -> BoxFuture<()> + Send + Sync>;
type SessionFn = Arc<dyn Fn(Session) -> BoxFuture<()> + Send + Sync>;
type ToolFilter = Arc<dyn Fn(&Session, &Tool) -> bool + Send + Sync>;

pub(crate) struct ToolEntry {
    pub(crate) tool: Tool,
    handler: ToolFn,
}

pub(crate) struct ResourceEntry {
    resource: Resource,
    handler: ResourceFn,
}

pub(crate) struct TemplateEntry {
    template: ResourceTemplate,
    handler: TemplateFn,
}

pub(crate) struct PromptEntry {
    prompt: Prompt,
    handler: PromptFn,
}

/// What is fixed once the server is built.
struct Config {
    info: Implementation,
    instructions: Option<String>,
    experimental: JsonObject,
    extensions: JsonObject,
    tools: bool,
    resources: bool,
    prompts: bool,
    completion: Option<CompletionFn>,
    requests: HashMap<String, RequestFn>,
    notifications: HashMap<String, NotificationFn>,
    on_initialized: Vec<SessionFn>,
    tool_filter: Option<ToolFilter>,
    /// The tasks extension, when enabled (see the `tasks` module).
    tasks: Option<Arc<tasks::TaskManager>>,
    cache_ttl: Duration,
    page_size: Option<usize>,
    cache_scope: CacheScope,
}

pub(crate) struct ServerInner {
    config: Config,
    tools: RwLock<Vec<Arc<ToolEntry>>>,
    resources: RwLock<Vec<Arc<ResourceEntry>>>,
    templates: RwLock<Vec<Arc<TemplateEntry>>>,
    prompts: RwLock<Vec<Arc<PromptEntry>>>,
    sessions: Mutex<HashMap<String, Weak<SessionInner>>>,
    listeners: stateless::Listeners,
}

/// An MCP server: what it offers (tools, resources, prompts...) and its live
/// sessions. Cheap to clone; serve it with a transport such as
/// `Server::serve_stdio` or `StreamableHttp`.
///
/// Tools, resources and prompts can be added and removed while serving;
/// sessions are told their lists changed.
#[derive(Clone)]
pub struct Server {
    pub(crate) inner: Arc<ServerInner>,
}

impl Server {
    pub fn builder(name: impl Into<String>, version: impl Into<String>) -> ServerBuilder {
        ServerBuilder::new(name, version)
    }

    pub fn info(&self) -> &Implementation {
        &self.inner.config.info
    }

    pub fn instructions(&self) -> Option<&str> {
        self.inner.config.instructions.as_deref()
    }

    pub fn capabilities(&self) -> ServerCapabilities {
        let c = &self.inner.config;
        let list_changed = || Some(ListChangedCapability { list_changed: Some(true) });
        ServerCapabilities {
            experimental: (!c.experimental.is_empty()).then(|| c.experimental.clone()),
            logging: Some(JsonObject::new()),
            completions: c.completion.as_ref().map(|_| JsonObject::new()),
            prompts: if c.prompts { list_changed() } else { None },
            resources: c.resources.then_some(ResourcesCapability { subscribe: Some(true), list_changed: Some(true) }),
            tools: if c.tools { list_changed() } else { None },
            extensions: (!c.extensions.is_empty()).then(|| c.extensions.clone()),
        }
    }

    /// Live sessions that completed initialization.
    pub fn sessions(&self) -> Vec<Session> {
        let sessions = self.inner.sessions.lock().unwrap();
        sessions
            .values()
            .filter_map(Weak::upgrade)
            .map(|inner| Session { inner })
            .filter(|s| s.is_initialized() && !s.is_closed())
            .collect()
    }

    pub(crate) fn register_session(&self, session: &Session) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        sessions.retain(|_, s| s.strong_count() > 0);
        sessions.insert(session.id().to_string(), Arc::downgrade(&session.inner));
    }

    pub(crate) fn unregister_session(&self, id: &str) {
        self.inner.sessions.lock().unwrap().remove(id);
    }

    /// Send a channel event to every session. Returns how many it was sent to
    /// (clients that didn't load the server as a channel drop it).
    pub fn channel_event(&self, event: &ChannelEvent) -> usize {
        self.sessions().iter().filter(|s| s.channel_event(event).is_ok()).count()
    }

    /// Tell sessions subscribed to `uri` that it changed (and
    /// `subscriptions/listen` streams watching it).
    pub fn notify_resource_updated(&self, uri: &str) {
        for s in self.sessions() {
            if s.inner.subscribed(uri) {
                let _ = s.notify("notifications/resources/updated", Some(serde_json::json!({ "uri": uri })));
            }
        }
        self.inner.listeners.notify("notifications/resources/updated", Some(uri), None);
    }

    /// Send a list change notification to every session and to the
    /// `subscriptions/listen` streams that asked for it.
    fn broadcast(&self, method: &str) {
        for s in self.sessions() {
            let _ = s.notify(method, None);
        }
        self.inner.listeners.notify(method, None, None);
    }

    /// The tools, in registration order.
    pub fn tools(&self) -> Vec<Tool> {
        self.inner.tools.read().unwrap().iter().map(|e| e.tool.clone()).collect()
    }

    /// Add a tool, or replace the one with the same name. See
    /// [`ServerBuilder::tool`].
    pub fn add_tool<F, Fut, R>(&self, tool: Tool, handler: F)
    where
        F: Fn(RequestContext, JsonObject) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        self.insert_tool(ToolEntry { tool, handler: tool_fn(handler) });
    }

    /// Add a tool taking typed arguments, its input schema derived from `A`.
    /// See [`ServerBuilder::typed_tool`].
    #[cfg(feature = "schemars")]
    pub fn add_typed_tool<A, F, Fut, R>(&self, tool: Tool, handler: F)
    where
        A: DeserializeOwned + schemars::JsonSchema + Send + 'static,
        F: Fn(RequestContext, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        let tool = tool.input_schema(input_schema_for::<A>());
        self.insert_tool(ToolEntry { tool, handler: typed_tool_fn(handler) });
    }

    fn insert_tool(&self, entry: ToolEntry) {
        self.forget_task_tool(&entry.tool.name);
        upsert(&self.inner.tools, entry, |e| &e.tool.name);
        self.broadcast("notifications/tools/list_changed");
    }

    /// Remove a tool. Returns whether it existed.
    pub fn remove_tool(&self, name: &str) -> bool {
        let removed = remove(&self.inner.tools, |e| e.tool.name == name);
        self.forget_task_tool(name);
        if removed {
            self.broadcast("notifications/tools/list_changed");
        }
        removed
    }

    /// Add a resource, or replace the one with the same URI.
    pub fn add_resource<F, Fut, R>(&self, resource: Resource, handler: F)
    where
        F: Fn(RequestContext, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: IntoReadResult,
    {
        upsert(&self.inner.resources, resource_entry(resource, handler), |e| &e.resource.uri);
        self.broadcast("notifications/resources/list_changed");
    }

    pub fn remove_resource(&self, uri: &str) -> bool {
        let removed = remove(&self.inner.resources, |e| e.resource.uri == uri);
        if removed {
            self.broadcast("notifications/resources/list_changed");
        }
        removed
    }

    /// Add a resource template, or replace the one with the same URI template.
    pub fn add_resource_template<F, Fut, R>(&self, template: ResourceTemplate, handler: F)
    where
        F: Fn(RequestContext, String, HashMap<String, String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: IntoReadResult,
    {
        upsert(&self.inner.templates, template_entry(template, handler), |e| &e.template.uri_template);
        self.broadcast("notifications/resources/list_changed");
    }

    pub fn remove_resource_template(&self, uri_template: &str) -> bool {
        let removed = remove(&self.inner.templates, |e| e.template.uri_template == uri_template);
        if removed {
            self.broadcast("notifications/resources/list_changed");
        }
        removed
    }

    /// Add a prompt, or replace the one with the same name.
    pub fn add_prompt<F, Fut, R>(&self, prompt: Prompt, handler: F)
    where
        F: Fn(RequestContext, HashMap<String, String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: IntoPromptResult,
    {
        upsert(&self.inner.prompts, prompt_entry(prompt, handler), |e| &e.prompt.name);
        self.broadcast("notifications/prompts/list_changed");
    }

    pub fn remove_prompt(&self, name: &str) -> bool {
        let removed = remove(&self.inner.prompts, |e| e.prompt.name == name);
        if removed {
            self.broadcast("notifications/prompts/list_changed");
        }
        removed
    }

    fn tool_visible(&self, session: &Session, tool: &Tool) -> bool {
        self.inner.config.tool_filter.as_ref().is_none_or(|f| f(session, tool))
    }

    pub(crate) fn list_tools(&self, session: &Session) -> Vec<Tool> {
        let tools = self.inner.tools.read().unwrap().clone();
        tools.iter().filter(|e| self.tool_visible(session, &e.tool)).map(|e| e.tool.clone()).collect()
    }

    pub(crate) async fn call_tool(&self, ctx: RequestContext, params: CallToolParams) -> Result<CallToolResult> {
        let entry = {
            let tools = self.inner.tools.read().unwrap();
            tools.iter().find(|e| e.tool.name == params.name).cloned()
        };
        match entry {
            Some(e) if self.tool_visible(ctx.session(), &e.tool) => (e.handler)(ctx, params.arguments).await,
            _ => Err(Error::invalid_params(format!("unknown tool: {}", params.name))),
        }
    }

    /// The input schema of a tool, by name.
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub(crate) fn tool_input_schema(&self, name: &str) -> Option<Value> {
        let tools = self.inner.tools.read().unwrap();
        tools.iter().find(|e| e.tool.name == name).map(|e| e.tool.input_schema.clone())
    }

    pub(crate) fn page_size(&self) -> Option<usize> {
        self.inner.config.page_size
    }

    pub(crate) fn list_resources(&self) -> Vec<Resource> {
        self.inner.resources.read().unwrap().iter().map(|e| e.resource.clone()).collect()
    }

    pub(crate) fn list_resource_templates(&self) -> Vec<ResourceTemplate> {
        self.inner.templates.read().unwrap().iter().map(|e| e.template.clone()).collect()
    }

    pub(crate) async fn read_resource(&self, ctx: RequestContext, uri: String) -> Result<ReadResourceResult> {
        let exact = {
            let resources = self.inner.resources.read().unwrap();
            resources.iter().find(|e| e.resource.uri == uri).cloned()
        };
        if let Some(e) = exact {
            return (e.handler)(ctx, uri).await;
        }
        let matched = {
            let templates = self.inner.templates.read().unwrap();
            templates
                .iter()
                .find_map(|e| match_uri_template(&e.template.uri_template, &uri).map(|vars| (e.clone(), vars)))
        };
        match matched {
            Some((e, vars)) => (e.handler)(ctx, uri, vars).await,
            None => Err(Error::resource_not_found(&uri)),
        }
    }

    pub(crate) fn list_prompts(&self) -> Vec<Prompt> {
        self.inner.prompts.read().unwrap().iter().map(|e| e.prompt.clone()).collect()
    }

    pub(crate) async fn get_prompt(&self, ctx: RequestContext, params: GetPromptParams) -> Result<GetPromptResult> {
        let entry = {
            let prompts = self.inner.prompts.read().unwrap();
            prompts.iter().find(|e| e.prompt.name == params.name).cloned()
        };
        let Some(entry) = entry else {
            return Err(Error::invalid_params(format!("unknown prompt: {}", params.name)));
        };
        for arg in entry.prompt.arguments.iter().flatten() {
            if arg.required == Some(true) && !params.arguments.contains_key(&arg.name) {
                return Err(Error::invalid_params(format!("missing required argument: {}", arg.name)));
            }
        }
        (entry.handler)(ctx, params.arguments).await
    }

    pub(crate) async fn complete(&self, ctx: RequestContext, params: CompleteParams) -> Option<Result<Completion>> {
        let f = self.inner.config.completion.clone()?;
        Some(f(ctx, params).await)
    }

    pub(crate) fn request_handler(&self, method: &str) -> Option<RequestFn> {
        self.inner.config.requests.get(method).cloned()
    }

    pub(crate) fn notification_handler(&self, method: &str) -> Option<NotificationFn> {
        self.inner.config.notifications.get(method).cloned()
    }

    pub(crate) fn on_initialized(&self) -> &[SessionFn] {
        &self.inner.config.on_initialized
    }
}

fn upsert<T>(list: &RwLock<Vec<Arc<T>>>, entry: T, key: impl Fn(&T) -> &String) {
    let mut list = list.write().unwrap();
    match list.iter_mut().find(|e| key(e) == key(&entry)) {
        Some(slot) => *slot = Arc::new(entry),
        None => list.push(Arc::new(entry)),
    }
}

fn remove<T>(list: &RwLock<Vec<Arc<T>>>, pred: impl Fn(&T) -> bool) -> bool {
    let mut list = list.write().unwrap();
    let before = list.len();
    list.retain(|e| !pred(e));
    list.len() != before
}

/// The JSON Schema of `T`, as MCP wants it (no `$schema` key).
#[cfg(feature = "schemars")]
pub(crate) fn schema_for<T: schemars::JsonSchema>() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(T)).unwrap_or_else(|_| serde_json::json!({}));
    if let Value::Object(map) = &mut schema {
        map.remove("$schema");
    }
    schema
}

/// The schema of a tool's arguments, which must be an object schema: types
/// whose schema says nothing of their type (such as enums of structs) get
/// `"type": "object"`.
#[cfg(feature = "schemars")]
pub(crate) fn input_schema_for<T: schemars::JsonSchema>() -> Value {
    let mut schema = schema_for::<T>();
    if let Value::Object(map) = &mut schema {
        map.entry("type").or_insert_with(|| "object".into());
    }
    schema
}

fn tool_fn<F, Fut, R>(f: F) -> ToolFn
where
    F: Fn(RequestContext, JsonObject) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
    R: IntoToolResult,
{
    Arc::new(move |ctx, args| {
        let fut = f(ctx, args.unwrap_or_default());
        Box::pin(async move {
            match fut.await {
                Ok(r) => Ok(r.into_tool_result()),
                Err(e) => e.into_result(),
            }
        })
    })
}

#[cfg_attr(not(feature = "schemars"), allow(dead_code))]
fn typed_tool_fn<A, F, Fut, R>(f: F) -> ToolFn
where
    A: DeserializeOwned + Send + 'static,
    F: Fn(RequestContext, A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
    R: IntoToolResult,
{
    Arc::new(move |ctx, args| {
        // Bad arguments are a tool execution error, so the model can retry.
        let args = match serde_json::from_value::<A>(Value::Object(args.unwrap_or_default())) {
            Ok(args) => args,
            Err(e) => {
                let result = CallToolResult::error(format!("invalid arguments: {e}"));
                return Box::pin(std::future::ready(Ok(result)));
            }
        };
        let fut = f(ctx, args);
        Box::pin(async move {
            match fut.await {
                Ok(r) => Ok(r.into_tool_result()),
                Err(e) => e.into_result(),
            }
        })
    })
}

fn resource_entry<F, Fut, R>(resource: Resource, f: F) -> ResourceEntry
where
    F: Fn(RequestContext, String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
    R: IntoReadResult,
{
    let mime = resource.mime_type.clone();
    let handler: ResourceFn = Arc::new(move |ctx, uri| {
        let fut = f(ctx, uri.clone());
        let mime = mime.clone();
        Box::pin(async move { Ok(fut.await?.into_read_result(&uri, mime.as_deref())) })
    });
    ResourceEntry { resource, handler }
}

fn template_entry<F, Fut, R>(template: ResourceTemplate, f: F) -> TemplateEntry
where
    F: Fn(RequestContext, String, HashMap<String, String>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
    R: IntoReadResult,
{
    let mime = template.mime_type.clone();
    let handler: TemplateFn = Arc::new(move |ctx, uri, vars| {
        let fut = f(ctx, uri.clone(), vars);
        let mime = mime.clone();
        Box::pin(async move { Ok(fut.await?.into_read_result(&uri, mime.as_deref())) })
    });
    TemplateEntry { template, handler }
}

fn prompt_entry<F, Fut, R>(prompt: Prompt, f: F) -> PromptEntry
where
    F: Fn(RequestContext, HashMap<String, String>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
    R: IntoPromptResult,
{
    let handler: PromptFn = Arc::new(move |ctx, args| {
        let fut = f(ctx, args);
        Box::pin(async move { Ok(fut.await?.into_prompt_result()) })
    });
    PromptEntry { prompt, handler }
}

/// Builds a [`Server`].
///
/// ```no_run
/// use mcptk::{Server, Tool, ToolError};
///
/// # async fn run() -> mcptk::Result<()> {
/// let server = Server::builder("hello", "0.1.0")
///     .instructions("Greets people.")
///     .tool(Tool::new("hello", "Say hello").read_only(), |_ctx, _args| async move {
///         Ok::<_, ToolError>("Hello!")
///     })
///     .build();
/// server.serve_stdio().await
/// # }
/// ```
pub struct ServerBuilder {
    config: Config,
    tools: Vec<Arc<ToolEntry>>,
    resources: Vec<Arc<ResourceEntry>>,
    templates: Vec<Arc<TemplateEntry>>,
    prompts: Vec<Arc<PromptEntry>>,
}

impl ServerBuilder {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        ServerBuilder {
            config: Config {
                info: Implementation::new(name, version),
                instructions: None,
                experimental: JsonObject::new(),
                extensions: JsonObject::new(),
                tools: false,
                resources: false,
                prompts: false,
                completion: None,
                requests: HashMap::new(),
                notifications: HashMap::new(),
                on_initialized: Vec::new(),
                tool_filter: None,
                tasks: None,
                cache_ttl: Duration::ZERO,
                page_size: None,
                cache_scope: CacheScope::Public,
            },
            tools: Vec::new(),
            resources: Vec::new(),
            templates: Vec::new(),
            prompts: Vec::new(),
        }
    }

    /// A human-friendly name for the server.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.config.info.title = Some(title.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.config.info.description = Some(description.into());
        self
    }

    pub fn website_url(mut self, url: impl Into<String>) -> Self {
        self.config.info.website_url = Some(url.into());
        self
    }

    pub fn icon(mut self, icon: Icon) -> Self {
        self.config.info.icons.get_or_insert_with(Vec::new).push(icon);
        self
    }

    /// Instructions for the model on how to use this server. Claude Code
    /// gives them to Claude when the server connects.
    pub fn instructions(mut self, instructions: impl Into<String>) -> Self {
        self.config.instructions = Some(instructions.into());
        self
    }

    /// Declare an experimental capability.
    pub fn experimental(mut self, name: impl Into<String>, value: Value) -> Self {
        self.config.experimental.insert(name.into(), value);
        self
    }

    /// Declare support for a protocol extension (e.g.
    /// `io.modelcontextprotocol/tasks`) with its settings object.
    pub fn extension(mut self, id: impl Into<String>, settings: Value) -> Self {
        self.config.extensions.insert(id.into(), settings);
        self
    }

    /// How long clients may cache list results (`tools/list`, `prompts/list`,
    /// `resources/list`, `resources/templates/list`), `resources/read` and
    /// `server/discover` results: their `ttlMs` (2026-07-28+). Default zero:
    /// always re-fetch. Clients listening with `subscriptions/listen` are
    /// still told about changes right away.
    pub fn cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.cache_ttl = ttl;
        self
    }

    /// Return list results (`tools/list`, `prompts/list`, `resources/list`,
    /// `resources/templates/list`) in pages of at most `size` entries, with a
    /// `nextCursor` to fetch the rest. By default, lists are returned whole.
    pub fn page_size(mut self, size: usize) -> Self {
        self.config.page_size = Some(size.max(1));
        self
    }

    /// Who may cache those results: their `cacheScope` (2026-07-28+).
    /// Default [`CacheScope::Public`]; `tools/list` is always
    /// [`CacheScope::Private`] with a [`tool_filter`](Self::tool_filter).
    /// Use `Private` when results depend on who is asking.
    pub fn cache_scope(mut self, scope: CacheScope) -> Self {
        self.config.cache_scope = scope;
        self
    }

    /// Advertise the tools capability even with no tools yet (to add some
    /// later). Registering a tool does this too.
    pub fn enable_tools(mut self) -> Self {
        self.config.tools = true;
        self
    }

    /// Advertise the resources capability even with no resources yet.
    pub fn enable_resources(mut self) -> Self {
        self.config.resources = true;
        self
    }

    /// Advertise the prompts capability even with no prompts yet.
    pub fn enable_prompts(mut self) -> Self {
        self.config.prompts = true;
        self
    }

    /// Add a tool. `handler` gets the raw arguments object.
    ///
    /// Return anything [`IntoToolResult`]: a `String`, [`Content`],
    /// [`Json`], a full [`CallToolResult`]... A [`ToolError`] becomes an
    /// `isError` result the model sees.
    pub fn tool<F, Fut, R>(mut self, tool: Tool, handler: F) -> Self
    where
        F: Fn(RequestContext, JsonObject) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        self.config.tools = true;
        self.tools.retain(|e| e.tool.name != tool.name);
        self.tools.push(Arc::new(ToolEntry { tool, handler: tool_fn(handler) }));
        self
    }

    /// Add a tool taking typed arguments. Its input schema is derived from
    /// `A`, replacing the one in `tool`; arguments that don't deserialize
    /// into `A` give the model an `isError` result.
    ///
    /// ```no_run
    /// # use mcptk::{Server, Tool, ToolError};
    /// #[derive(serde::Deserialize, schemars::JsonSchema)]
    /// struct Add { a: i64, b: i64 }
    ///
    /// let server = Server::builder("calc", "1.0")
    ///     .typed_tool(Tool::new("add", "Add two numbers"), |_ctx, args: Add| async move {
    ///         Ok::<_, ToolError>((args.a + args.b).to_string())
    ///     })
    ///     .build();
    /// ```
    #[cfg(feature = "schemars")]
    pub fn typed_tool<A, F, Fut, R>(mut self, tool: Tool, handler: F) -> Self
    where
        A: DeserializeOwned + schemars::JsonSchema + Send + 'static,
        F: Fn(RequestContext, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        let tool = tool.input_schema(input_schema_for::<A>());
        self.config.tools = true;
        self.tools.retain(|e| e.tool.name != tool.name);
        self.tools.push(Arc::new(ToolEntry { tool, handler: typed_tool_fn(handler) }));
        self
    }

    /// Only list and allow, per session, the tools `filter` accepts. Call
    /// [`Session::notify_tools_list_changed`] when its answer changes.
    pub fn tool_filter(mut self, filter: impl Fn(&Session, &Tool) -> bool + Send + Sync + 'static) -> Self {
        self.config.tool_filter = Some(Arc::new(filter));
        self
    }

    /// Add a resource. `handler` gets the URI and returns its contents; a
    /// `String` becomes text contents with the resource's MIME type.
    pub fn resource<F, Fut, R>(mut self, resource: Resource, handler: F) -> Self
    where
        F: Fn(RequestContext, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: IntoReadResult,
    {
        self.config.resources = true;
        self.resources.retain(|e| e.resource.uri != resource.uri);
        self.resources.push(Arc::new(resource_entry(resource, handler)));
        self
    }

    /// Add a resource template. `handler` gets the URI and the template
    /// variables matched from it (see [`match_uri_template`]).
    pub fn resource_template<F, Fut, R>(mut self, template: ResourceTemplate, handler: F) -> Self
    where
        F: Fn(RequestContext, String, HashMap<String, String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: IntoReadResult,
    {
        self.config.resources = true;
        self.templates.retain(|e| e.template.uri_template != template.uri_template);
        self.templates.push(Arc::new(template_entry(template, handler)));
        self
    }

    /// Add a prompt. Required arguments are checked before `handler` runs.
    pub fn prompt<F, Fut, R>(mut self, prompt: Prompt, handler: F) -> Self
    where
        F: Fn(RequestContext, HashMap<String, String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: IntoPromptResult,
    {
        self.config.prompts = true;
        self.prompts.retain(|e| e.prompt.name != prompt.name);
        self.prompts.push(Arc::new(prompt_entry(prompt, handler)));
        self
    }

    /// Answer `completion/complete` requests (argument autocompletion for
    /// prompts and resource templates).
    pub fn completion<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(RequestContext, CompleteParams) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Completion>> + Send + 'static,
    {
        self.config.completion = Some(Arc::new(move |ctx, p| Box::pin(handler(ctx, p))));
        self
    }

    /// Handle a request method MCP doesn't define (or replace a standard
    /// one's handling).
    pub fn on_request<F, Fut>(mut self, method: impl Into<String>, handler: F) -> Self
    where
        F: Fn(RequestContext, Option<Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.config.requests.insert(method.into(), Arc::new(move |ctx, p| Box::pin(handler(ctx, p))));
        self
    }

    /// Handle a notification from the client, such as
    /// `notifications/roots/list_changed` or an extension's.
    pub fn on_notification<F, Fut>(mut self, method: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Session, Option<Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.config.notifications.insert(method.into(), Arc::new(move |s, p| Box::pin(handler(s, p))));
        self
    }

    /// Run `handler` when a session completes initialization: the earliest
    /// the server may send it requests and notifications.
    pub fn on_initialized<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Session) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.config.on_initialized.push(Arc::new(move |s| Box::pin(handler(s))));
        self
    }

    /// Make this server a Claude Code channel: it may then push
    /// [`ChannelEvent`]s into the session. See the [`channel` module](crate::channel).
    pub fn channel(self) -> Self {
        self.experimental(channel::CHANNEL_CAPABILITY, Value::Object(JsonObject::new()))
    }

    /// Opt in to Claude Code permission relay: `handler` gets each tool
    /// approval prompt, to forward to the approver; send their answer with
    /// [`Session::permission_verdict`]. Implies [`ServerBuilder::channel`].
    ///
    /// Anyone who can answer can approve tool use: only use this when the
    /// channel authenticates its senders.
    pub fn channel_permission<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(Session, PermissionRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.channel().experimental(channel::PERMISSION_CAPABILITY, Value::Object(JsonObject::new())).on_notification(
            channel::PERMISSION_REQUEST_NOTIFICATION,
            move |session, params| {
                let handler = handler.clone();
                async move {
                    match serde_json::from_value::<PermissionRequest>(params.unwrap_or_default()) {
                        Ok(req) => handler(session, req).await,
                        Err(e) => tracing::warn!("bad permission request: {e}"),
                    }
                }
            },
        )
    }

    pub fn build(self) -> Server {
        Server {
            inner: Arc::new(ServerInner {
                config: self.config,
                tools: RwLock::new(self.tools),
                resources: RwLock::new(self.resources),
                templates: RwLock::new(self.templates),
                prompts: RwLock::new(self.prompts),
                sessions: Mutex::new(HashMap::new()),
                listeners: Default::default(),
            }),
        }
    }
}
