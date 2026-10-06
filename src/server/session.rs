//! A server's connection with one client, and request handling.

use super::Server;
use crate::channel::{self, ChannelEvent, PermissionVerdict};
use crate::error::{Error, Result};
use crate::jsonrpc::{ErrorObject, Message, Notification, Request, RequestId};
use crate::types::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// What a session hands to its transport.
pub(crate) enum Outbound {
    Message(Message),
    /// The request finished without a response (it was cancelled).
    Done(#[cfg_attr(not(feature = "http"), allow(dead_code))] RequestId),
}

/// Where outgoing messages go: the transport's single stream (stdio), or
/// for HTTP, a session mailbox or the stream answering one POST.
#[derive(Clone)]
pub(crate) enum Outlet {
    Channel(mpsc::UnboundedSender<Outbound>),
    #[cfg(feature = "http")]
    Mailbox(Arc<crate::http::Mailbox>),
}

impl Outlet {
    pub(crate) fn send(&self, out: Outbound) -> bool {
        match self {
            Outlet::Channel(tx) => tx.send(out).is_ok(),
            #[cfg(feature = "http")]
            Outlet::Mailbox(mailbox) => {
                if let Outbound::Message(msg) = out {
                    mailbox.push(msg);
                }
                true
            }
        }
    }
}

type Pending = Mutex<HashMap<RequestId, oneshot::Sender<Result<Value, ErrorObject>>>>;

pub(crate) struct SessionInner {
    id: String,
    server: Server,
    outlet: Outlet,
    client: OnceLock<InitializeParams>,
    protocol_version: OnceLock<&'static str>,
    initialized: AtomicBool,
    next_id: AtomicI64,
    pending: Pending,
    running: Mutex<HashMap<RequestId, CancellationToken>>,
    log_level: Mutex<LoggingLevel>,
    subscriptions: Mutex<HashSet<String>>,
    data: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    closed: CancellationToken,
}

/// One client's session with the server. Cheap to clone.
///
/// Use it to send the client notifications (channel events, logs, list
/// changes) and requests (sampling, elicitation, roots), and to keep
/// per-session state with [`Session::set_data`].
#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Arc<SessionInner>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").field("id", &self.inner.id).finish_non_exhaustive()
    }
}

pub(crate) fn random_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("no system randomness");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse<T: DeserializeOwned>(params: Option<Value>) -> Result<T, ErrorObject> {
    serde_json::from_value(params.unwrap_or_else(|| json!({})))
        .map_err(|e| ErrorObject::invalid_params(format!("invalid params: {e}")))
}

fn to_value<T: Serialize>(v: T) -> Result<Value, ErrorObject> {
    serde_json::to_value(v).map_err(|e| ErrorObject::internal(e.to_string()))
}

impl Session {
    pub(crate) fn new(server: Server, outlet: Outlet) -> Session {
        let session = Session {
            inner: Arc::new(SessionInner {
                id: random_id(),
                server: server.clone(),
                outlet,
                client: OnceLock::new(),
                protocol_version: OnceLock::new(),
                initialized: AtomicBool::new(false),
                next_id: AtomicI64::new(1),
                pending: Mutex::new(HashMap::new()),
                running: Mutex::new(HashMap::new()),
                log_level: Mutex::new(LoggingLevel::Info),
                subscriptions: Mutex::new(HashSet::new()),
                data: Mutex::new(HashMap::new()),
                closed: CancellationToken::new(),
            }),
        };
        server.register_session(&session);
        session
    }

    /// A random identifier (the `Mcp-Session-Id` over HTTP).
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub fn server(&self) -> &Server {
        &self.inner.server
    }

    /// The client's name and version, once initialized.
    pub fn client_info(&self) -> Option<&Implementation> {
        self.inner.client.get().map(|c| &c.client_info)
    }

    /// What the client supports, once initialized.
    pub fn client_capabilities(&self) -> Option<&ClientCapabilities> {
        self.inner.client.get().map(|c| &c.capabilities)
    }

    /// The negotiated protocol revision, once initialized.
    pub fn protocol_version(&self) -> Option<&'static str> {
        self.inner.protocol_version.get().copied()
    }

    /// Whether the client sent `notifications/initialized`.
    pub fn is_initialized(&self) -> bool {
        self.inner.initialized.load(Ordering::Acquire)
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.is_cancelled()
    }

    /// Wait until the session ends (client gone, or [`Session::close`]).
    pub async fn closed(&self) {
        self.inner.closed.cancelled().await
    }

    /// End the session: running requests are cancelled, pending requests to
    /// the client fail.
    pub fn close(&self) {
        if self.inner.closed.is_cancelled() {
            return;
        }
        self.inner.closed.cancel();
        self.inner.pending.lock().unwrap().clear();
        self.inner.running.lock().unwrap().clear();
        self.inner.server.unregister_session(&self.inner.id);
    }

    /// Store per-session state, replacing any value of the same type.
    pub fn set_data<T: Any + Send + Sync>(&self, value: T) {
        self.inner.data.lock().unwrap().insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Per-session state of type `T`. Store an `Arc` to share it.
    pub fn data<T: Any + Send + Sync + Clone>(&self) -> Option<T> {
        let data = self.inner.data.lock().unwrap();
        data.get(&TypeId::of::<T>()).and_then(|v| v.downcast_ref::<T>()).cloned()
    }

    pub fn remove_data<T: Any + Send + Sync>(&self) -> Option<T> {
        let mut data = self.inner.data.lock().unwrap();
        data.remove(&TypeId::of::<T>()).and_then(|v| v.downcast::<T>().ok()).map(|b| *b)
    }

    fn send_via(&self, outlet: &Outlet, msg: Message) -> Result<()> {
        if self.is_closed() || !outlet.send(Outbound::Message(msg)) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    /// Send a notification.
    pub fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        self.send_via(&self.inner.outlet, Message::notification(method, params))
    }

    /// Send a request and wait for the result.
    pub async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        self.request_via(&self.inner.outlet, method, params).await
    }

    async fn request_via(&self, outlet: &Outlet, method: &str, params: Option<Value>) -> Result<Value> {
        let id = RequestId::Number(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(id.clone(), tx);
        // Forget the request if this future is dropped.
        struct Guard<'a>(&'a Pending, RequestId);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.lock().unwrap().remove(&self.1);
            }
        }
        let _guard = Guard(&self.inner.pending, id.clone());
        self.send_via(outlet, Message::request(id, method, params))?;
        tokio::select! {
            r = rx => match r {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => Err(Error::Rpc(e)),
                Err(_) => Err(Error::Closed),
            },
            _ = self.inner.closed.cancelled() => Err(Error::Closed),
        }
    }

    async fn request_as<T: DeserializeOwned>(
        &self,
        outlet: &Outlet,
        method: &str,
        params: impl Serialize,
    ) -> Result<T> {
        let v = self.request_via(outlet, method, Some(serde_json::to_value(params)?)).await?;
        Ok(serde_json::from_value(v)?)
    }

    fn log_via(&self, outlet: &Outlet, level: LoggingLevel, logger: Option<&str>, data: Value) -> Result<()> {
        if level < *self.inner.log_level.lock().unwrap() {
            return Ok(());
        }
        let mut params = json!({ "level": level, "data": data });
        if let Some(logger) = logger {
            params["logger"] = logger.into();
        }
        self.send_via(outlet, Message::notification("notifications/message", Some(params)))
    }

    /// Send a log message, if at or above the level the client asked for
    /// (`info` until it asks).
    pub fn log(&self, level: LoggingLevel, logger: Option<&str>, data: impl Into<Value>) -> Result<()> {
        self.log_via(&self.inner.outlet, level, logger, data.into())
    }

    pub fn notify_tools_list_changed(&self) -> Result<()> {
        self.notify("notifications/tools/list_changed", None)
    }

    pub fn notify_resources_list_changed(&self) -> Result<()> {
        self.notify("notifications/resources/list_changed", None)
    }

    pub fn notify_prompts_list_changed(&self) -> Result<()> {
        self.notify("notifications/prompts/list_changed", None)
    }

    /// Tell the client `uri` changed, if it subscribed to it.
    pub fn notify_resource_updated(&self, uri: &str) -> Result<()> {
        if !self.inner.subscriptions.lock().unwrap().contains(uri) {
            return Ok(());
        }
        self.notify("notifications/resources/updated", Some(json!({ "uri": uri })))
    }

    /// Push a channel event into the session (needs
    /// [`ServerBuilder::channel`](crate::ServerBuilder::channel)).
    pub fn channel_event(&self, event: &ChannelEvent) -> Result<()> {
        self.notify(channel::CHANNEL_NOTIFICATION, Some(serde_json::to_value(event)?))
    }

    /// Answer a relayed permission prompt.
    pub fn permission_verdict(&self, verdict: &PermissionVerdict) -> Result<()> {
        self.notify(channel::PERMISSION_NOTIFICATION, Some(serde_json::to_value(verdict)?))
    }

    pub async fn ping(&self) -> Result<()> {
        self.request("ping", None).await.map(|_| ())
    }

    fn require(&self, what: &'static str, has: impl Fn(&ClientCapabilities) -> bool) -> Result<()> {
        match self.client_capabilities() {
            Some(c) if has(c) => Ok(()),
            _ => Err(Error::Unsupported(what)),
        }
    }

    fn require_sampling(&self, params: &CreateMessageParams) -> Result<()> {
        self.require("sampling", |c| c.sampling.is_some())?;
        if params.uses_tools() {
            self.require("sampling with tools", ClientCapabilities::supports_sampling_tools)?;
        }
        Ok(())
    }

    fn require_elicitation(&self, params: &ElicitParams) -> Result<()> {
        match params {
            ElicitParams::Form(_) => self.require("elicitation", ClientCapabilities::supports_elicitation_form),
            ElicitParams::Url(_) => self.require("URL elicitation", ClientCapabilities::supports_elicitation_url),
        }
    }

    /// Ask the client's LLM for a completion (sampling). Requests with tools
    /// need the client's `sampling.tools` capability.
    pub async fn create_message(&self, params: CreateMessageParams) -> Result<CreateMessageResult> {
        self.require_sampling(&params)?;
        self.request_as(&self.inner.outlet, "sampling/createMessage", params).await
    }

    /// Ask the user for input (elicitation). URL mode needs the client's
    /// `elicitation.url` capability.
    pub async fn elicit(&self, params: ElicitParams) -> Result<ElicitResult> {
        self.require_elicitation(&params)?;
        self.request_as(&self.inner.outlet, "elicitation/create", params).await
    }

    /// Tell the client the out-of-band interaction of a URL mode elicitation
    /// finished (`notifications/elicitation/complete`).
    pub fn notify_elicitation_complete(&self, elicitation_id: &str) -> Result<()> {
        self.notify("notifications/elicitation/complete", Some(json!({ "elicitationId": elicitation_id })))
    }

    /// The client's roots (directories or files it lets the server work on).
    pub async fn list_roots(&self) -> Result<ListRootsResult> {
        self.require("roots", |c| c.roots.is_some())?;
        self.request_as(&self.inner.outlet, "roots/list", json!({})).await
    }

    /// Handle an incoming message; `reply` is where its response goes.
    pub(crate) fn handle(&self, msg: Message, reply: &Outlet) {
        match msg {
            Message::Request(req) => self.handle_request(req, reply),
            Message::Notification(n) => self.handle_notification(n),
            Message::Response(r) => self.resolve(r.id, Ok(r.result)),
            Message::Error(e) => match e.id {
                Some(id) => self.resolve(id, Err(e.error)),
                None => tracing::warn!("client reported an error: {}", e.error),
            },
        }
    }

    fn resolve(&self, id: RequestId, result: Result<Value, ErrorObject>) {
        match self.inner.pending.lock().unwrap().remove(&id) {
            Some(tx) => {
                let _ = tx.send(result);
            }
            None => tracing::debug!(%id, "response to an unknown request"),
        }
    }

    fn respond(reply: &Outlet, id: RequestId, result: Result<Value, ErrorObject>) {
        let msg = match result {
            Ok(v) => Message::response(id, v),
            Err(e) => Message::error(Some(id), e),
        };
        reply.send(Outbound::Message(msg));
    }

    fn handle_request(&self, req: Request, reply: &Outlet) {
        if self.is_closed() {
            return;
        }
        match req.method.as_str() {
            // Answered inline, so that the session is set up before any
            // message that follows is handled.
            "initialize" => Self::respond(reply, req.id, self.initialize(req.params)),
            "ping" => Self::respond(reply, req.id, Ok(json!({}))),
            _ if self.inner.client.get().is_none() => {
                Self::respond(reply, req.id, Err(ErrorObject::invalid_request("session not initialized")))
            }
            _ => {
                let cancel = self.inner.closed.child_token();
                self.inner.running.lock().unwrap().insert(req.id.clone(), cancel.clone());
                let meta = req.params.as_ref().and_then(|p| p.get("_meta")).and_then(Value::as_object).cloned();
                let ctx = RequestContext {
                    session: self.clone(),
                    id: req.id.clone(),
                    meta,
                    outlet: reply.clone(),
                    cancel: cancel.clone(),
                    auth: crate::auth::current(), // auth hook
                };
                let session = self.clone();
                let reply = reply.clone();
                tokio::spawn(async move {
                    let Request { id, method, params } = req;
                    let result = tokio::select! {
                        r = session.route(ctx, &method, params) => Some(r),
                        _ = cancel.cancelled() => None,
                    };
                    session.inner.running.lock().unwrap().remove(&id);
                    match result {
                        Some(r) => Self::respond(&reply, id, r),
                        None => {
                            reply.send(Outbound::Done(id));
                        }
                    }
                });
            }
        }
    }

    fn initialize(&self, params: Option<Value>) -> Result<Value, ErrorObject> {
        let params: InitializeParams = parse(params)?;
        let version = negotiate_protocol_version(&params.protocol_version);
        if self.inner.client.set(params).is_err() {
            return Err(ErrorObject::invalid_request("already initialized"));
        }
        let _ = self.inner.protocol_version.set(version);
        let server = &self.inner.server;
        to_value(InitializeResult {
            protocol_version: version.to_string(),
            capabilities: server.capabilities(),
            server_info: server.info().clone(),
            instructions: server.instructions().map(str::to_string),
        })
    }

    fn handle_notification(&self, n: Notification) {
        match n.method.as_str() {
            "notifications/initialized" => {
                if self.inner.client.get().is_some() && !self.inner.initialized.swap(true, Ordering::AcqRel) {
                    for hook in self.inner.server.on_initialized() {
                        tokio::spawn(hook(self.clone()));
                    }
                }
            }
            "notifications/cancelled" => {
                let id = n.params.as_ref().and_then(|p| p.get("requestId")).cloned();
                if let Some(id) = id.and_then(|id| serde_json::from_value::<RequestId>(id).ok())
                    && let Some(token) = self.inner.running.lock().unwrap().remove(&id)
                {
                    token.cancel();
                }
            }
            _ => {}
        }
        match self.inner.server.notification_handler(&n.method) {
            Some(handler) => {
                tokio::spawn(handler(self.clone(), n.params));
            }
            None => tracing::trace!(method = n.method, "unhandled notification"),
        }
    }

    async fn route(&self, ctx: RequestContext, method: &str, params: Option<Value>) -> Result<Value, ErrorObject> {
        let server = self.inner.server.clone();
        if let Some(handler) = server.request_handler(method) {
            return handler(ctx, params).await.map_err(|e| e.to_error_object());
        }
        match method {
            "tools/list" => to_value(ListToolsResult { tools: server.list_tools(self), next_cursor: None }),
            "tools/call" => server.route_tool_call(ctx, parse(params)?).await,
            "resources/list" => to_value(ListResourcesResult { resources: server.list_resources(), next_cursor: None }),
            "resources/templates/list" => to_value(ListResourceTemplatesResult {
                resource_templates: server.list_resource_templates(),
                next_cursor: None,
            }),
            "resources/read" => {
                let p: ReadResourceParams = parse(params)?;
                to_value(server.read_resource(ctx, p.uri).await.map_err(|e| e.to_error_object())?)
            }
            "resources/subscribe" => {
                let p: ReadResourceParams = parse(params)?;
                self.inner.subscriptions.lock().unwrap().insert(p.uri);
                Ok(json!({}))
            }
            "resources/unsubscribe" => {
                let p: ReadResourceParams = parse(params)?;
                self.inner.subscriptions.lock().unwrap().remove(&p.uri);
                Ok(json!({}))
            }
            "prompts/list" => to_value(ListPromptsResult { prompts: server.list_prompts(), next_cursor: None }),
            "prompts/get" => to_value(server.get_prompt(ctx, parse(params)?).await.map_err(|e| e.to_error_object())?),
            "logging/setLevel" => {
                let p: SetLevelParams = parse(params)?;
                *self.inner.log_level.lock().unwrap() = p.level;
                Ok(json!({}))
            }
            "completion/complete" => match server.complete(ctx, parse(params)?).await {
                Some(r) => to_value(CompleteResult { completion: r.map_err(|e| e.to_error_object())? }),
                None => Err(ErrorObject::method_not_found(method)),
            },
            _ => Err(ErrorObject::method_not_found(method)),
        }
    }
}

/// The context of a request being handled: its session, cancellation, and
/// a way to send progress, logs and nested requests along with the response.
#[derive(Clone)]
pub struct RequestContext {
    session: Session,
    id: RequestId,
    meta: Option<JsonObject>,
    outlet: Outlet,
    cancel: CancellationToken,
    /// Who the request is authenticated as (see `crate::auth`).
    pub(crate) auth: Option<Arc<crate::auth::AuthInfo>>,
}

impl RequestContext {
    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn request_id(&self) -> &RequestId {
        &self.id
    }

    /// The request's `_meta`.
    pub fn meta(&self) -> Option<&JsonObject> {
        self.meta.as_ref()
    }

    /// The progress token, if the client wants progress notifications.
    pub fn progress_token(&self) -> Option<&Value> {
        self.meta.as_ref().and_then(|m| m.get("progressToken"))
    }

    /// Whether the client cancelled the request (or the session ended).
    /// Handlers are dropped when that happens; this is for work done outside
    /// the handler's future.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }

    /// Report progress, if the client asked for it. `progress` must increase
    /// with each call.
    pub fn progress(&self, progress: f64, total: Option<f64>, message: Option<&str>) -> Result<()> {
        let Some(token) = self.progress_token() else {
            return Ok(());
        };
        let mut params = json!({ "progressToken": token, "progress": progress });
        if let Some(total) = total {
            params["total"] = total.into();
        }
        if let Some(message) = message {
            params["message"] = message.into();
        }
        self.notify("notifications/progress", Some(params))
    }

    /// Send a notification related to this request.
    pub fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        self.session.send_via(&self.outlet, Message::notification(method, params))
    }

    /// Send a log message related to this request.
    pub fn log(&self, level: LoggingLevel, logger: Option<&str>, data: impl Into<Value>) -> Result<()> {
        self.session.log_via(&self.outlet, level, logger, data.into())
    }

    /// Send a request to the client, related to this request.
    pub async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        self.session.request_via(&self.outlet, method, params).await
    }

    /// Ask the client's LLM for a completion (sampling). Requests with tools
    /// need the client's `sampling.tools` capability.
    pub async fn create_message(&self, params: CreateMessageParams) -> Result<CreateMessageResult> {
        self.session.require_sampling(&params)?;
        self.session.request_as(&self.outlet, "sampling/createMessage", params).await
    }

    /// Ask the user for input (elicitation). URL mode needs the client's
    /// `elicitation.url` capability.
    pub async fn elicit(&self, params: ElicitParams) -> Result<ElicitResult> {
        self.session.require_elicitation(&params)?;
        self.session.request_as(&self.outlet, "elicitation/create", params).await
    }

    /// The client's roots.
    pub async fn list_roots(&self) -> Result<ListRootsResult> {
        self.session.require("roots", |c| c.roots.is_some())?;
        self.session.request_as(&self.outlet, "roots/list", json!({})).await
    }
}
