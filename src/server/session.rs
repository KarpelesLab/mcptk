//! A server's connection with one client, and request handling.

use super::Server;
use super::input::InputState;
use super::stateless::{self, RequestMeta};
use crate::channel::{self, ChannelEvent, PermissionVerdict};
use crate::error::{Error, Result};
use crate::jsonrpc::{ErrorObject, Message, Notification, Request, RequestId};
use crate::types::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// What a session hands to its transport.
pub(crate) enum Outbound {
    Message(Message),
    /// The request finished without a response (it was cancelled).
    Done(RequestId),
    /// The responses to a batch of requests, to send as one JSON array.
    Batch(Vec<Message>),
}

impl Outbound {
    /// The JSON text to put on the wire, if any.
    pub(crate) fn to_json(&self) -> serde_json::Result<Option<Vec<u8>>> {
        match self {
            Outbound::Message(msg) => serde_json::to_vec(msg).map(Some),
            Outbound::Batch(msgs) => serde_json::to_vec(msgs).map(Some),
            Outbound::Done(_) => Ok(None),
        }
    }
}

/// Where outgoing messages go: the transport's single stream (stdio), or
/// for HTTP, a session mailbox or the stream answering one POST.
#[derive(Clone)]
pub(crate) enum Outlet {
    Channel(mpsc::UnboundedSender<Outbound>),
    #[cfg(feature = "http")]
    Mailbox(Arc<crate::http::Mailbox>),
    /// Collects the responses to a batch of requests.
    Batch(Arc<Batcher>),
}

/// Gathers the responses to the requests of a batch (JSON-RPC batching, in
/// MCP 2025-03-26 only), to answer with one array once all are in. Anything
/// else the requests send goes straight through.
pub(crate) struct Batcher {
    outlet: Outlet,
    /// Requests not answered yet, and the responses so far.
    state: Mutex<(HashSet<RequestId>, Vec<Message>)>,
}

impl Batcher {
    fn send(&self, out: Outbound) -> bool {
        let mut state = self.state.lock().unwrap();
        match out {
            Outbound::Message(msg) if msg.response_id().is_some_and(|id| state.0.remove(id)) => state.1.push(msg),
            Outbound::Done(id) => {
                state.0.remove(&id);
            }
            other => {
                drop(state);
                return self.outlet.send(other);
            }
        }
        if state.0.is_empty() && !state.1.is_empty() {
            return self.outlet.send(Outbound::Batch(std::mem::take(&mut state.1)));
        }
        true
    }
}

/// Handle the messages in one JSON text received on a single-stream
/// transport (a line, a WebSocket frame): `handle` gets each message and
/// where to answer it. Requests sent as a batch are answered as one.
pub(crate) fn dispatch_text(text: &[u8], outlet: &Outlet, mut handle: impl FnMut(Message, &Outlet)) {
    let decoded = crate::jsonrpc::decode(text);
    let is_batch = text.trim_ascii_start().first() == Some(&b'[') && decoded.iter().any(Result::is_ok);
    if !is_batch {
        for msg in decoded {
            match msg {
                Ok(msg) => handle(msg, outlet),
                Err(error) => {
                    outlet.send(Outbound::Message(error));
                }
            }
        }
        return;
    }
    let mut pending = HashSet::new();
    let mut messages = Vec::new();
    let mut errors = Vec::new();
    for msg in decoded {
        match msg {
            Ok(msg) => {
                if let Message::Request(req) = &msg {
                    pending.insert(req.id.clone());
                }
                messages.push(msg);
            }
            Err(error) => errors.push(error),
        }
    }
    if pending.is_empty() {
        if !errors.is_empty() {
            outlet.send(Outbound::Batch(errors));
        }
        messages.into_iter().for_each(|msg| handle(msg, outlet));
        return;
    }
    let batch = Outlet::Batch(Arc::new(Batcher { outlet: outlet.clone(), state: Mutex::new((pending, errors)) }));
    for msg in messages {
        match msg {
            Message::Request(_) => handle(msg, &batch),
            other => handle(other, outlet),
        }
    }
}

impl Outlet {
    pub(crate) fn send(&self, out: Outbound) -> bool {
        match self {
            Outlet::Channel(tx) => tx.send(out).is_ok(),
            #[cfg(feature = "http")]
            Outlet::Mailbox(mailbox) => {
                match out {
                    Outbound::Message(msg) => mailbox.push(msg),
                    Outbound::Batch(msgs) => msgs.into_iter().for_each(|msg| mailbox.push(msg)),
                    Outbound::Done(_) => {}
                }
                true
            }
            Outlet::Batch(batcher) => batcher.send(out),
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
    /// Signalled each time a request finishes.
    finished: tokio::sync::Notify,
    /// How many running requests are open `subscriptions/listen` streams,
    /// which never finish on their own.
    listening: std::sync::atomic::AtomicUsize,
    log_level: Mutex<LoggingLevel>,
    subscriptions: Mutex<HashSet<String>>,
    data: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    closed: CancellationToken,
}

impl SessionInner {
    /// Whether the client subscribed to `uri` with `resources/subscribe`.
    pub(crate) fn subscribed(&self, uri: &str) -> bool {
        self.subscriptions.lock().unwrap().contains(uri)
    }
}

/// Most times a handler is run again with client input, for one request.
const MAX_INPUT_ROUNDS: usize = 16;

/// One client's session with the server. Cheap to clone.
///
/// Use it to send the client notifications (channel events, logs, list
/// changes) and requests (sampling, elicitation, roots), and to keep
/// per-session state with [`Session::set_data`].
///
/// A session is a connection: one stdio stream, or one `Mcp-Session-Id` over
/// HTTP. Clients on protocol 2026-07-28 have no session: their requests
/// carry everything in `_meta` (see [`RequestContext`]), and don't initialize
/// the connection they arrive on. Over HTTP, each of their requests gets a
/// fresh session of its own. Session-wide notifications (logs, channel
/// events, list changes) only go to initialized sessions; 2026-07-28 clients
/// get list changes through `subscriptions/listen`.
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

/// The page of `items` a list request asks for with its `cursor` (an offset,
/// opaque to clients), and the cursor of the page after it.
fn page<T>(
    items: Vec<T>,
    params: &Option<Value>,
    size: Option<usize>,
) -> Result<(Vec<T>, Option<String>), ErrorObject> {
    let start = match params.as_ref().and_then(|p| p.get("cursor")) {
        None | Some(Value::Null) => 0,
        Some(cursor) => cursor
            .as_str()
            .and_then(|c| c.parse::<usize>().ok())
            .filter(|start| *start <= items.len())
            .ok_or_else(|| ErrorObject::invalid_params("invalid cursor"))?,
    };
    let end = start.saturating_add(size.unwrap_or(usize::MAX)).min(items.len());
    let next = (end < items.len()).then(|| end.to_string());
    Ok((items.into_iter().skip(start).take(end - start).collect(), next))
}

fn to_value<T: Serialize>(v: T) -> Result<Value, ErrorObject> {
    serde_json::to_value(v).map_err(|e| ErrorObject::internal(e.to_string()))
}

fn json<T: Serialize>(v: T) -> Result<Value> {
    Ok(to_value(v)?)
}

impl Session {
    pub(crate) fn new(server: Server, outlet: Outlet) -> Session {
        let session = Self::create(server.clone(), outlet);
        server.register_session(&session);
        session
    }

    /// A session for one stateless request, not listed in
    /// [`Server::sessions`].
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub(crate) fn detached(server: Server, outlet: Outlet) -> Session {
        Self::create(server, outlet)
    }

    fn create(server: Server, outlet: Outlet) -> Session {
        Session {
            inner: Arc::new(SessionInner {
                id: random_id(),
                server,
                outlet,
                client: OnceLock::new(),
                protocol_version: OnceLock::new(),
                initialized: AtomicBool::new(false),
                next_id: AtomicI64::new(1),
                pending: Mutex::new(HashMap::new()),
                running: Mutex::new(HashMap::new()),
                finished: tokio::sync::Notify::new(),
                listening: std::sync::atomic::AtomicUsize::new(0),
                log_level: Mutex::new(LoggingLevel::Info),
                subscriptions: Mutex::new(HashSet::new()),
                data: Mutex::new(HashMap::new()),
                closed: CancellationToken::new(),
            }),
        }
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

    /// The client stopped sending (end of input): let running requests
    /// finish and answer, for at most `grace`. Requests waiting on the client
    /// fail right away, since no answer can come.
    pub(crate) async fn drain(&self, grace: std::time::Duration) {
        self.inner.pending.lock().unwrap().clear();
        let idle = async {
            loop {
                let finished = self.inner.finished.notified();
                tokio::pin!(finished);
                finished.as_mut().enable();
                let listening = self.inner.listening.load(Ordering::Acquire);
                if self.inner.running.lock().unwrap().len() <= listening {
                    return;
                }
                finished.await;
            }
        };
        tokio::select! {
            _ = idle => {}
            _ = tokio::time::sleep(grace) => {}
            _ = self.closed() => {}
        }
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

    /// Send a log message if `threshold` is set and `level` is at or above it.
    fn log_via(
        &self,
        outlet: &Outlet,
        threshold: Option<LoggingLevel>,
        level: LoggingLevel,
        logger: Option<&str>,
        data: Value,
    ) -> Result<()> {
        if threshold.is_none_or(|t| level < t) {
            return Ok(());
        }
        let mut params = json!({ "level": level, "data": data });
        if let Some(logger) = logger {
            params["logger"] = logger.into();
        }
        self.send_via(outlet, Message::notification("notifications/message", Some(params)))
    }

    /// Whether the client opened this session with `initialize` (a
    /// handshake-based protocol revision).
    fn handshake(&self) -> bool {
        self.inner.client.get().is_some()
    }

    /// Send a log message, if at or above the level the client asked for
    /// (`info` until it asks). Only initialized sessions get these:
    /// 2026-07-28 clients only get logs related to their requests (see
    /// [`RequestContext::log`]).
    pub fn log(&self, level: LoggingLevel, logger: Option<&str>, data: impl Into<Value>) -> Result<()> {
        let threshold = self.handshake().then(|| *self.inner.log_level.lock().unwrap());
        self.log_via(&self.inner.outlet, threshold, level, logger, data.into())
    }

    /// Tell an initialized client, and the `subscriptions/listen` streams
    /// opened over this connection that asked for it, that a list changed.
    fn notify_list_changed(&self, method: &str) -> Result<()> {
        self.inner.server.inner.listeners.notify(method, None, Some(self));
        if !self.handshake() {
            return Ok(());
        }
        self.notify(method, None)
    }

    pub fn notify_tools_list_changed(&self) -> Result<()> {
        self.notify_list_changed("notifications/tools/list_changed")
    }

    pub fn notify_resources_list_changed(&self) -> Result<()> {
        self.notify_list_changed("notifications/resources/list_changed")
    }

    pub fn notify_prompts_list_changed(&self) -> Result<()> {
        self.notify_list_changed("notifications/prompts/list_changed")
    }

    /// Tell the client `uri` changed, if it subscribed to it (with
    /// `resources/subscribe`, or `subscriptions/listen` over this
    /// connection).
    pub fn notify_resource_updated(&self, uri: &str) -> Result<()> {
        self.inner.server.inner.listeners.notify("notifications/resources/updated", Some(uri), Some(self));
        if !self.inner.subscribed(uri) {
            return Ok(());
        }
        self.notify("notifications/resources/updated", Some(json!({ "uri": uri })))
    }

    /// Push a channel event into the session (needs
    /// [`ServerBuilder::channel`](crate::ServerBuilder::channel)).
    ///
    /// Channels need a handshake-based protocol revision: this fails with
    /// [`Error::Unsupported`] until the client sent `initialize`.
    pub fn channel_event(&self, event: &ChannelEvent) -> Result<()> {
        if !self.handshake() {
            return Err(Error::Unsupported("channel events (they need an initialized session)"));
        }
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
        // Protocol 2026-07-28+: the request carries its own protocol
        // metadata, and needs no initialized session.
        if req.method != "initialize" && stateless::is_stateless_request(&req.method, req.params.as_ref()) {
            match stateless::parse_meta(req.params.as_ref()) {
                Ok(meta) => self.spawn_request(req, reply, Some(Arc::new(meta))),
                Err(e) => Self::respond(reply, req.id, Err(e)),
            }
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
            _ => self.spawn_request(req, reply, None),
        }
    }

    fn spawn_request(&self, req: Request, reply: &Outlet, protocol: Option<Arc<RequestMeta>>) {
        let cancel = self.inner.closed.child_token();
        self.inner.running.lock().unwrap().insert(req.id.clone(), cancel.clone());
        let meta = req.params.as_ref().and_then(|p| p.get("_meta")).and_then(Value::as_object).cloned();
        let input = match protocol {
            Some(_) => InputState::from_params(req.params.as_ref()),
            None => InputState::default(),
        };
        let ctx = RequestContext {
            session: self.clone(),
            id: req.id.clone(),
            meta,
            protocol,
            input: Arc::new(input),
            outlet: reply.clone(),
            cancel: cancel.clone(),
            auth: crate::auth::current(), // auth hook
        };
        let session = self.clone();
        let reply = reply.clone();
        tokio::spawn(async move {
            let Request { id, method, params } = req;
            let is_stateless = ctx.is_stateless();
            let result = tokio::select! {
                r = session.route(ctx, &method, params) => Some(r),
                _ = cancel.cancelled() => None,
            };
            session.inner.running.lock().unwrap().remove(&id);
            match result {
                Some(r) if is_stateless => {
                    Self::respond(&reply, id, stateless::finish(&session.inner.server, &method, r))
                }
                Some(r) => Self::respond(&reply, id, r),
                None => {
                    reply.send(Outbound::Done(id));
                }
            }
            session.inner.finished.notify_waiters();
        });
    }

    /// Run a handler that may ask for client input ([`Error::InputRequired`]):
    /// on stateless requests, the request is answered with an
    /// `input_required` result; on handshake sessions, mcptk sends the input
    /// requests to the client and runs the handler again with the answers.
    async fn with_input<F, Fut>(&self, mut ctx: RequestContext, run: F) -> Result<Value>
    where
        F: Fn(RequestContext) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        for _ in 0..MAX_INPUT_ROUNDS {
            let input = match run(ctx.clone()).await {
                Err(Error::InputRequired(input)) => input,
                other => return other,
            };
            if let Some(missing) = input.missing_capabilities(ctx.client_capabilities()) {
                return Err(Error::Rpc(ErrorObject::missing_client_capability(missing)));
            }
            if input.input_requests.is_empty() && input.request_state.is_none() {
                return Err(Error::internal("input required, but no input request nor state given"));
            }
            if ctx.is_stateless() {
                return Ok(input.into_result());
            }
            let mut responses = JsonObject::new();
            for (key, req) in input.input_requests {
                let method = req.get("method").and_then(Value::as_str).unwrap_or_default();
                let answer = self.request_via(&ctx.outlet, method, req.get("params").cloned()).await?;
                responses.insert(key, answer);
            }
            ctx.input = Arc::new(InputState::new(Some(responses), input.request_state));
        }
        Err(Error::internal("too many rounds of client input"))
    }

    /// A `subscriptions/listen` request: acknowledge it, then deliver
    /// notifications until it is cancelled (it never completes otherwise).
    async fn listen(&self, ctx: &RequestContext, params: ListenParams) -> Result<Value, ErrorObject> {
        let server = &self.inner.server;
        let mut filter = stateless::honored(server, &params.notifications);
        filter.task_ids = server.watchable_tasks(ctx, params.notifications.task_ids).await;
        let _guard = server.inner.listeners.add(server, self, ctx.id.clone(), filter, ctx.outlet.clone());
        struct Listening<'a>(&'a SessionInner);
        impl Drop for Listening<'_> {
            fn drop(&mut self) {
                self.0.listening.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.inner.listening.fetch_add(1, Ordering::AcqRel);
        let _listening = Listening(&self.inner);
        self.inner.finished.notify_waiters();
        std::future::pending().await
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
        let is_stateless = ctx.is_stateless();
        let rpc = |e: Error| e.to_error_object();
        match method {
            "server/discover" if is_stateless => to_value(stateless::discover(&server)),
            "subscriptions/listen" if is_stateless => self.listen(&ctx, parse(params)?).await,
            // Removed in 2026-07-28.
            "ping" | "logging/setLevel" | "resources/subscribe" | "resources/unsubscribe" if is_stateless => {
                Err(ErrorObject::method_not_found(method))
            }
            "tools/list" => {
                let (tools, next_cursor) = page(server.list_tools(self), &params, server.page_size())?;
                to_value(ListToolsResult { tools, next_cursor })
            }
            "tools/call" => {
                let p: CallToolParams = parse(params)?;
                let run = |ctx| {
                    let (server, p) = (server.clone(), p.clone());
                    async move { server.route_tool_call(ctx, p).await }
                };
                let mut result = self.with_input(ctx, run).await.map_err(rpc)?;
                // Before 2026-07-28, structured content had to be an object.
                if !is_stateless && result.get("structuredContent").is_some_and(|s| !s.is_object()) {
                    result.as_object_mut().map(|r| r.remove("structuredContent"));
                }
                Ok(result)
            }
            "resources/list" => {
                let (resources, next_cursor) = page(server.list_resources(), &params, server.page_size())?;
                to_value(ListResourcesResult { resources, next_cursor })
            }
            "resources/templates/list" => {
                let (resource_templates, next_cursor) =
                    page(server.list_resource_templates(), &params, server.page_size())?;
                to_value(ListResourceTemplatesResult { resource_templates, next_cursor })
            }
            "resources/read" => {
                let p: ReadResourceParams = parse(params)?;
                let run = |ctx| {
                    let (server, uri) = (server.clone(), p.uri.clone());
                    async move { json(server.read_resource(ctx, uri).await?) }
                };
                self.with_input(ctx, run).await.map_err(rpc)
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
            "prompts/list" => {
                let (prompts, next_cursor) = page(server.list_prompts(), &params, server.page_size())?;
                to_value(ListPromptsResult { prompts, next_cursor })
            }
            "prompts/get" => {
                let p: GetPromptParams = parse(params)?;
                let run = |ctx| {
                    let (server, p) = (server.clone(), p.clone());
                    async move { json(server.get_prompt(ctx, p).await?) }
                };
                self.with_input(ctx, run).await.map_err(rpc)
            }
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
///
/// Handlers don't need to care which protocol revision the client speaks:
/// [`client_info`](Self::client_info) and
/// [`client_capabilities`](Self::client_capabilities) come from the request
/// itself (2026-07-28) or from the session's `initialize`, and
/// [`elicit`](Self::elicit), [`create_message`](Self::create_message) and
/// [`list_roots`](Self::list_roots) use multi round-trip requests or
/// server-to-client requests as needed.
#[derive(Clone)]
pub struct RequestContext {
    session: Session,
    id: RequestId,
    meta: Option<JsonObject>,
    /// Set for stateless (2026-07-28+) requests.
    protocol: Option<Arc<RequestMeta>>,
    input: Arc<InputState>,
    outlet: Outlet,
    cancel: CancellationToken,
    /// Who the request is authenticated as (see `crate::auth`).
    pub(crate) auth: Option<Arc<crate::auth::AuthInfo>>,
}

impl RequestContext {
    /// The session (connection) the request came in on. Stateless requests
    /// over HTTP each get a fresh one.
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

    /// Whether the request carries its own protocol metadata (revision
    /// 2026-07-28 and later) rather than belonging to an initialized session.
    pub fn is_stateless(&self) -> bool {
        self.protocol.is_some()
    }

    /// The protocol revision of this request.
    pub fn protocol_version(&self) -> Option<&'static str> {
        match &self.protocol {
            Some(p) => Some(p.version),
            None => self.session.protocol_version(),
        }
    }

    /// The client's name and version, from the request or the session.
    pub fn client_info(&self) -> Option<&Implementation> {
        match &self.protocol {
            Some(p) => p.client_info.as_ref(),
            None => self.session.client_info(),
        }
    }

    /// What the client supports, from the request or the session.
    pub fn client_capabilities(&self) -> Option<&ClientCapabilities> {
        match &self.protocol {
            Some(p) => Some(&p.capabilities),
            None => self.session.client_capabilities(),
        }
    }

    /// The lowest level [`log`](Self::log) sends, if any: the request's
    /// `io.modelcontextprotocol/logLevel`, or the session's level.
    pub fn log_level(&self) -> Option<LoggingLevel> {
        match &self.protocol {
            Some(p) => p.log_level,
            None => Some(*self.session.inner.log_level.lock().unwrap()),
        }
    }

    /// The client's answers to an [`InputRequired`](crate::InputRequired),
    /// by key (`inputResponses`), when this run follows one.
    pub fn input_responses(&self) -> Option<&JsonObject> {
        self.input.responses()
    }

    /// The client's answer to the input request `key`, if given; e.g. an
    /// [`ElicitResult`] for an elicitation.
    pub fn input_response<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.input.response(key)
    }

    /// The `requestState` of the [`InputRequired`](crate::InputRequired)
    /// this run follows. It went through the client: don't trust it.
    pub fn request_state(&self) -> Option<&str> {
        self.input.state()
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

    /// The progress token, if the client wants progress notifications.
    pub fn progress_token(&self) -> Option<&Value> {
        self.meta.as_ref().and_then(|m| m.get("progressToken"))
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

    /// Send a log message related to this request, if at or above
    /// [`log_level`](Self::log_level). Stateless requests only get logs when
    /// they ask with `io.modelcontextprotocol/logLevel`.
    pub fn log(&self, level: LoggingLevel, logger: Option<&str>, data: impl Into<Value>) -> Result<()> {
        self.session.log_via(&self.outlet, self.log_level(), level, logger, data.into())
    }

    /// Send a request to the client, related to this request.
    ///
    /// Stateless requests (2026-07-28) can't: the server asks for input with
    /// an [`InputRequired`](crate::InputRequired) instead. This fails with
    /// [`Error::Unsupported`] for them.
    pub async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        if self.is_stateless() {
            return Err(Error::Unsupported("server-to-client requests (use InputRequired)"));
        }
        self.session.request_via(&self.outlet, method, params).await
    }

    fn require(&self, what: &'static str, has: impl Fn(&ClientCapabilities) -> bool) -> Result<()> {
        match self.client_capabilities() {
            Some(c) if has(c) => Ok(()),
            _ => Err(Error::Unsupported(what)),
        }
    }

    /// Send a request to the client and wait for its answer, or on stateless
    /// requests, take the answer from the client's retry (or fail with the
    /// [`Error::InputRequired`] that asks for it).
    pub(crate) async fn ask<T: DeserializeOwned>(&self, method: &str, params: impl Serialize) -> Result<T> {
        if self.is_stateless() {
            return self.input.next(method, serde_json::to_value(params)?);
        }
        self.session.request_as(&self.outlet, method, params).await
    }

    /// Ask the client's LLM for a completion (sampling). Requests with tools
    /// need the client's `sampling.tools` capability.
    ///
    /// On stateless requests (2026-07-28) this is a multi round-trip
    /// request: the first time, it fails with [`Error::InputRequired`]. Let
    /// it propagate with `?` (tool handlers too): the client then retries
    /// the request with the answer, and this call returns it. The handler
    /// runs again from the start each time, so it must make its
    /// `create_message`, `elicit` and `list_roots` calls in the same order.
    pub async fn create_message(&self, params: CreateMessageParams) -> Result<CreateMessageResult> {
        self.require("sampling", |c| c.sampling.is_some())?;
        if params.uses_tools() {
            self.require("sampling with tools", ClientCapabilities::supports_sampling_tools)?;
        }
        self.ask("sampling/createMessage", params).await
    }

    /// Ask the user for input (elicitation). URL mode needs the client's
    /// `elicitation.url` capability. See
    /// [`create_message`](Self::create_message) for stateless requests.
    pub async fn elicit(&self, params: ElicitParams) -> Result<ElicitResult> {
        match &params {
            ElicitParams::Form(_) => self.require("elicitation", ClientCapabilities::supports_elicitation_form)?,
            ElicitParams::Url(_) => self.require("URL elicitation", ClientCapabilities::supports_elicitation_url)?,
        }
        self.ask("elicitation/create", params).await
    }

    /// The client's roots. See [`create_message`](Self::create_message) for
    /// stateless requests.
    pub async fn list_roots(&self) -> Result<ListRootsResult> {
        self.require("roots", |c| c.roots.is_some())?;
        self.ask("roots/list", json!({})).await
    }
}
