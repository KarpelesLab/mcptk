//! The MCP Streamable HTTP transport (server side).
//!
//! One endpoint (`/mcp` by default) takes:
//! - `POST`: client messages. Requests are answered with an SSE stream that
//!   carries their responses plus related notifications and requests (or a
//!   plain JSON body, see [`StreamableHttp::json_response`]); responses and
//!   notifications get `202 Accepted`.
//! - `GET`: an SSE stream for messages the server sends on its own (channel
//!   events, list changes, logs...). They are queued while no stream is open.
//! - `DELETE`: ends the session.
//!
//! `initialize` creates a session; its id comes back in the `Mcp-Session-Id`
//! header, which the client sends with every later request.
//!
//! Clients on protocol revision 2026-07-28 have no session: each POST holds
//! one request carrying its protocol version and capabilities in `_meta`,
//! and must have matching `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`
//! and `Mcp-Param-*` headers (else `400` with a `HeaderMismatch` error).
//! Closing a response stream cancels its request. Change notifications come
//! on the stream answering `subscriptions/listen`, which stays open. Both
//! kinds of clients can use the same endpoint.
//!
//! Serve it with [`StreamableHttp::serve`], or route requests to
//! [`StreamableHttp::handle`] from your own hyper/axum server.
//!
//! Not supported yet: resuming streams with `Last-Event-ID`.

use crate::error::Result;
use crate::jsonrpc::{self, ErrorObject, Message, RequestId};
use crate::server::stateless;
use crate::server::{Outbound, Outlet, Server, Session};
use crate::types::{SUPPORTED_PROTOCOL_VERSIONS, is_stateless_protocol_version};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, SizeHint};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

pub const SESSION_ID_HEADER: &str = "mcp-session-id";
pub const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// Most messages queued for a session with no `GET` stream open; older ones
/// are dropped beyond that.
const MAILBOX_LIMIT: usize = 1000;

/// Messages waiting for a session's `GET` stream.
pub(crate) struct Mailbox {
    queue: Mutex<VecDeque<Message>>,
    notify: Notify,
}

impl Mailbox {
    fn new() -> Self {
        Mailbox { queue: Mutex::new(VecDeque::new()), notify: Notify::new() }
    }

    pub(crate) fn push(&self, msg: Message) {
        let mut queue = self.queue.lock().unwrap();
        if queue.len() >= MAILBOX_LIMIT {
            queue.pop_front();
            tracing::warn!("session mailbox full, dropping oldest message");
        }
        queue.push_back(msg);
        drop(queue);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<Message> {
        self.queue.lock().unwrap().pop_front()
    }

    fn unpop(&self, msg: Message) {
        self.queue.lock().unwrap().push_front(msg);
    }
}

struct HttpSession {
    session: Session,
    mailbox: Arc<Mailbox>,
    /// Stops the current `GET` stream, if any.
    stream: Mutex<Option<CancellationToken>>,
    last_seen: Mutex<Instant>,
}

impl HttpSession {
    fn touch(&self) {
        *self.last_seen.lock().unwrap() = Instant::now();
    }

    fn has_stream(&self) -> bool {
        self.stream.lock().unwrap().as_ref().is_some_and(|t| !t.is_cancelled())
    }
}

#[derive(Clone)]
struct Settings {
    path: Option<String>,
    allowed_origins: Vec<String>,
    any_origin: bool,
    json_response: bool,
    max_body_size: usize,
    session_timeout: Duration,
    keepalive: Duration,
    auth: Option<Arc<crate::auth::ProtectedResource>>,
}

#[derive(Default)]
struct State {
    sessions: Mutex<HashMap<String, Arc<HttpSession>>>,
    sweeper: OnceLock<()>,
}

/// An MCP server served over Streamable HTTP. Cheap to clone.
///
/// ```no_run
/// # async fn run(server: mcptk::Server) -> mcptk::Result<()> {
/// mcptk::http::StreamableHttp::new(server).serve("127.0.0.1:8080").await
/// # }
/// ```
#[derive(Clone)]
pub struct StreamableHttp {
    server: Server,
    settings: Arc<Settings>,
    state: Arc<State>,
}

impl StreamableHttp {
    pub fn new(server: Server) -> Self {
        StreamableHttp {
            server,
            settings: Arc::new(Settings {
                path: Some("/mcp".into()),
                allowed_origins: Vec::new(),
                any_origin: false,
                json_response: false,
                max_body_size: 4 << 20,
                session_timeout: Duration::from_secs(3600),
                keepalive: Duration::from_secs(25),
                auth: None,
            }),
            state: Arc::default(),
        }
    }

    fn settings(&mut self) -> &mut Settings {
        Arc::make_mut(&mut self.settings)
    }

    /// The endpoint path (default `/mcp`). `None` answers on any path, for
    /// when a router in front already picked the route.
    pub fn path(mut self, path: Option<&str>) -> Self {
        self.settings().path = path.map(str::to_string);
        self
    }

    /// Accept requests from this browser origin (e.g. `https://app.example`).
    ///
    /// Requests carrying an `Origin` header are refused unless it is listed
    /// here or is a localhost origin, to block DNS rebinding attacks.
    /// Requests without one (non-browser clients) are always accepted.
    pub fn allow_origin(mut self, origin: impl Into<String>) -> Self {
        self.settings().allowed_origins.push(origin.into());
        self
    }

    /// Accept requests from any origin.
    pub fn allow_any_origin(mut self) -> Self {
        self.settings().any_origin = true;
        self
    }

    /// Answer requests with a plain JSON body instead of an SSE stream.
    /// Notifications and requests related to them then go to the `GET`
    /// stream.
    pub fn json_response(mut self, enabled: bool) -> Self {
        self.settings().json_response = enabled;
        self
    }

    /// The largest request body accepted (default 4 MiB).
    pub fn max_body_size(mut self, bytes: usize) -> Self {
        self.settings().max_body_size = bytes;
        self
    }

    /// End sessions idle this long with no `GET` stream open (default 1h).
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.settings().session_timeout = timeout;
        self
    }

    /// How often to send an SSE comment on idle `GET` streams (default 25s),
    /// so proxies keep them open and dead clients are noticed.
    pub fn keepalive(mut self, interval: Duration) -> Self {
        self.settings().keepalive = interval;
        self
    }

    /// Require OAuth access tokens, and serve the protected resource
    /// metadata. See [`crate::auth`].
    pub fn auth(mut self, resource: crate::auth::ProtectedResource) -> Self {
        if resource.authorization_servers().is_empty() {
            tracing::warn!("ProtectedResource has no authorization server: clients can't log in");
        }
        self.settings().auth = Some(Arc::new(resource));
        self
    }

    /// The authorization settings, if any.
    pub fn protected_resource(&self) -> Option<&crate::auth::ProtectedResource> {
        self.settings.auth.as_deref()
    }

    pub fn server(&self) -> &Server {
        &self.server
    }

    /// Listen on `addr` and serve until an error occurs.
    pub async fn serve(self, addr: impl ToSocketAddrs) -> Result<()> {
        self.serve_listener(TcpListener::bind(addr).await?).await
    }

    /// Serve connections from `listener`.
    pub async fn serve_listener(self, listener: TcpListener) -> Result<()> {
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    // Usually out of file descriptors: back off.
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let this = self.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    let this = this.clone();
                    async move { Ok::<_, Infallible>(this.handle(req).await) }
                });
                let io = hyper_util::rt::TokioIo::new(stream);
                if let Err(e) = hyper::server::conn::http1::Builder::new().serve_connection(io, service).await {
                    tracing::debug!("connection error: {e}");
                }
            });
        }
    }

    /// Handle one HTTP request.
    ///
    /// With [`auth`](Self::auth), this also answers requests for the
    /// protected resource metadata, on its well-known paths, whatever
    /// [`path`](Self::path) is.
    pub async fn handle<B>(&self, mut req: Request<B>) -> Response<McpBody>
    where
        B: Body,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // auth hook: metadata document
        if let Some(auth) = &self.settings.auth
            && let Some(res) = auth.serve_metadata(&req)
        {
            return res;
        }
        if let Some(path) = &self.settings.path
            && req.uri().path() != path
        {
            return plain(StatusCode::NOT_FOUND, "not found");
        }
        if !self.origin_allowed(req.headers().get(header::ORIGIN)) {
            return rpc_error(StatusCode::FORBIDDEN, "Forbidden: origin not allowed");
        }
        // auth hook: bearer token
        if let Some(auth) = &self.settings.auth {
            match auth.authenticate(req.headers()).await {
                Ok(info) => req.extensions_mut().insert(info),
                Err(res) => return res,
            };
        }
        if let Some(v) = req.headers().get(PROTOCOL_VERSION_HEADER)
            && !SUPPORTED_PROTOCOL_VERSIONS.iter().any(|s| v.as_bytes() == s.as_bytes())
        {
            let error = ErrorObject::unsupported_protocol_version(&String::from_utf8_lossy(v.as_bytes()));
            return json_body(StatusCode::BAD_REQUEST, &Message::error(None, error), None);
        }
        match *req.method() {
            Method::POST => self.post(req).await,
            Method::GET => self.get(req),
            Method::DELETE => self.delete(req),
            _ => {
                let mut res = plain(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
                res.headers_mut().insert(header::ALLOW, HeaderValue::from_static("GET, POST, DELETE"));
                res
            }
        }
    }

    fn origin_allowed(&self, origin: Option<&HeaderValue>) -> bool {
        let Some(origin) = origin else { return true };
        let Ok(origin) = origin.to_str() else { return false };
        if self.settings.any_origin || self.settings.allowed_origins.iter().any(|o| o == origin) {
            return true;
        }
        let host = origin.split_once("://").map_or(origin, |(_, rest)| rest);
        let host = match host.strip_prefix('[') {
            Some(v6) => v6.split(']').next().unwrap_or(""),
            None => host.split(':').next().unwrap_or(""),
        };
        matches!(host, "localhost" | "127.0.0.1" | "::1")
    }

    #[allow(clippy::result_large_err)]
    fn lookup<B>(&self, req: &Request<B>) -> Result<Arc<HttpSession>, Response<McpBody>> {
        let Some(id) = req.headers().get(SESSION_ID_HEADER).and_then(|v| v.to_str().ok()) else {
            return Err(rpc_error(StatusCode::BAD_REQUEST, "Bad Request: missing session id"));
        };
        let session = self.state.sessions.lock().unwrap().get(id).cloned();
        match session {
            // auth hook: only the subject that created a session may use it
            Some(s) if !s.session.is_closed() && crate::auth::owns_session(&s.session, req.extensions()) => {
                s.touch();
                Ok(s)
            }
            _ => Err(rpc_error(StatusCode::NOT_FOUND, "Session not found")),
        }
    }

    fn create_session(&self) -> Arc<HttpSession> {
        let mailbox = Arc::new(Mailbox::new());
        let session = Session::new(self.server.clone(), Outlet::Mailbox(mailbox.clone()));
        let http_session =
            Arc::new(HttpSession { session, mailbox, stream: Mutex::new(None), last_seen: Mutex::new(Instant::now()) });
        let id = http_session.session.id().to_string();
        self.state.sessions.lock().unwrap().insert(id, http_session.clone());
        self.state.sweeper.get_or_init(|| {
            tokio::spawn(sweep(Arc::downgrade(&self.state), self.settings.session_timeout));
        });
        http_session
    }

    async fn post<B>(&self, req: Request<B>) -> Response<McpBody>
    where
        B: Body,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        if let Some(ct) = req.headers().get(header::CONTENT_TYPE)
            && !ct.to_str().is_ok_and(|ct| ct.starts_with("application/json"))
        {
            return rpc_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type: expected application/json");
        }
        let accept = req.headers().get(header::ACCEPT).and_then(|v| v.to_str().ok()).unwrap_or("");
        let use_sse = !self.settings.json_response && (accept.contains("text/event-stream") || accept.is_empty());

        let (parts, body) = req.into_parts();
        let body = match http_body_util::Limited::new(body, self.settings.max_body_size).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) if e.is::<http_body_util::LengthLimitError>() => {
                return rpc_error(StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large");
            }
            Err(e) => return rpc_error(StatusCode::BAD_REQUEST, &format!("Bad Request: {e}")),
        };
        let batch = body.trim_ascii_start().first() == Some(&b'[');
        let mut messages = Vec::new();
        for m in jsonrpc::decode(&body) {
            match m {
                Ok(m) => messages.push(m),
                Err(error) => return json_body(StatusCode::BAD_REQUEST, &error, None),
            }
        }
        // auth hook: scopes needed by these requests
        if let Some(auth) = &self.settings.auth
            && let Err(res) = auth.check_scopes(&messages, &parts.extensions)
        {
            return res;
        }
        let auth = crate::auth::from_extensions(&parts.extensions);

        // Protocol 2026-07-28+: no session, one message per POST.
        let stateless = messages.iter().any(|m| match m {
            Message::Request(r) => stateless::is_stateless_request(&r.method, r.params.as_ref()),
            _ => false,
        });
        if stateless {
            let Some(Message::Request(req)) = messages.pop().filter(|_| messages.is_empty() && !batch) else {
                return rpc_error(StatusCode::BAD_REQUEST, "Bad Request: send one request per POST");
            };
            return self.post_stateless(&parts.headers, req, use_sse, auth).await;
        }
        let modern_header = parts
            .headers
            .get(PROTOCOL_VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(is_stateless_protocol_version);
        if modern_header
            && !parts.headers.contains_key(SESSION_ID_HEADER)
            && messages.iter().all(|m| matches!(m, Message::Notification(_)))
        {
            // Nothing to do: notifications/cancelled isn't used over HTTP.
            return empty(StatusCode::ACCEPTED);
        }

        let initializing = messages.iter().any(|m| matches!(m, Message::Request(r) if r.method == "initialize"));
        let session = if initializing {
            if messages.len() > 1 {
                return rpc_error(StatusCode::BAD_REQUEST, "Bad Request: initialize must be sent alone");
            }
            let session = self.create_session();
            crate::auth::bind_session(&session.session, &parts.extensions); // auth hook
            session
        } else {
            match self.lookup(&Request::from_parts(parts, ())) {
                Ok(s) => s,
                Err(res) => return res,
            }
        };
        let new_session_id = initializing.then(|| session.session.id().to_string());

        let mailbox = Outlet::Mailbox(session.mailbox.clone());
        let mut requests = Vec::new();
        crate::auth::scope(auth.clone(), || {
            for m in messages {
                match m {
                    Message::Request(r) => requests.push(r),
                    other => session.session.handle(other, &mailbox),
                }
            }
        });
        if requests.is_empty() {
            return empty(StatusCode::ACCEPTED);
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let outlet = Outlet::Channel(tx);
        let pending: HashSet<RequestId> = requests.iter().map(|r| r.id.clone()).collect();
        crate::auth::scope(auth, || {
            for r in requests {
                session.session.handle(Message::Request(r), &outlet);
            }
        });
        drop(outlet);

        if use_sse {
            let (body_tx, body_rx) = mpsc::channel(16);
            tokio::spawn(stream_responses(rx, pending, body_tx, session.session.clone()));
            sse_response(body_rx, new_session_id.as_deref())
        } else {
            let responses = collect_responses(rx, pending, &session).await;
            match (batch, responses.len()) {
                (_, 0) => empty(StatusCode::ACCEPTED),
                (false, 1) => json_body(StatusCode::OK, &responses[0], new_session_id.as_deref()),
                _ => json_body(StatusCode::OK, &responses, new_session_id.as_deref()),
            }
        }
    }

    fn get<B>(&self, req: Request<B>) -> Response<McpBody> {
        let accept = req.headers().get(header::ACCEPT).and_then(|v| v.to_str().ok()).unwrap_or("");
        if !accept.contains("text/event-stream") {
            return plain(StatusCode::METHOD_NOT_ALLOWED, "GET needs Accept: text/event-stream");
        }
        let session = match self.lookup(&req) {
            Ok(s) => s,
            Err(res) => return res,
        };
        // A new stream replaces the previous one, which may belong to a
        // client that reconnected.
        let token = CancellationToken::new();
        if let Some(old) = session.stream.lock().unwrap().replace(token.clone()) {
            old.cancel();
        }
        let (body_tx, body_rx) = mpsc::channel(16);
        tokio::spawn(stream_mailbox(session, token, body_tx, self.settings.keepalive));
        sse_response(body_rx, None)
    }

    fn delete<B>(&self, req: Request<B>) -> Response<McpBody> {
        let session = match self.lookup(&req) {
            Ok(s) => s,
            Err(res) => return res,
        };
        self.state.sessions.lock().unwrap().remove(session.session.id());
        session.session.close();
        empty(StatusCode::OK)
    }

    /// Answer a request of a stateless revision (2026-07-28+): it gets a
    /// session of its own, which ends with the response, or when the client
    /// closes the stream (which cancels the request).
    async fn post_stateless(
        &self,
        headers: &HeaderMap,
        req: jsonrpc::Request,
        use_sse: bool,
        auth: Option<Arc<crate::auth::AuthInfo>>,
    ) -> Response<McpBody> {
        let id = req.id.clone();
        let reject = |status, error| json_body(status, &Message::error(Some(id.clone()), error), None);
        let meta = match stateless::parse_meta(req.params.as_ref()) {
            Ok(meta) => meta,
            Err(e) => return reject(StatusCode::BAD_REQUEST, e),
        };
        if let Err(e) = self.check_headers(headers, &req, meta.version) {
            return reject(StatusCode::BAD_REQUEST, e);
        }
        // The listen stream only makes sense as SSE.
        let use_sse = use_sse || req.method == "subscriptions/listen";

        let (tx, mut rx) = mpsc::unbounded_channel();
        let outlet = Outlet::Channel(tx);
        let session = Session::detached(self.server.clone(), outlet.clone());
        crate::auth::scope(auth, || session.handle(Message::Request(req), &outlet));
        drop(outlet);

        // Wait for the first message, to pick the HTTP status of errors.
        let first = tokio::select! {
            out = rx.recv() => out,
            _ = session.closed() => None,
        };
        let first = match first {
            Some(Outbound::Message(msg)) => msg,
            _ => {
                session.close();
                return empty(StatusCode::ACCEPTED);
            }
        };
        if let Message::Error(e) = &first {
            let status = match e.error.code {
                jsonrpc::METHOD_NOT_FOUND => Some(StatusCode::NOT_FOUND),
                jsonrpc::HEADER_MISMATCH
                | jsonrpc::MISSING_REQUIRED_CLIENT_CAPABILITY
                | jsonrpc::UNSUPPORTED_PROTOCOL_VERSION => Some(StatusCode::BAD_REQUEST),
                _ => None,
            };
            if let Some(status) = status {
                session.close();
                return json_body(status, &first, None);
            }
        }
        if use_sse {
            let (body_tx, body_rx) = mpsc::channel(16);
            tokio::spawn(stream_stateless(first, rx, id, body_tx, session, self.settings.keepalive));
            return sse_response(body_rx, None);
        }
        // JSON: skip related notifications, they have nowhere to go.
        let mut msg = Some(first);
        while let Some(m) = msg.take() {
            if m.response_id() == Some(&id) {
                session.close();
                return json_body(StatusCode::OK, &m, None);
            }
            msg = tokio::select! {
                out = rx.recv() => match out {
                    Some(Outbound::Message(m)) => Some(m),
                    _ => None,
                },
                _ = session.closed() => None,
            };
        }
        session.close();
        empty(StatusCode::ACCEPTED)
    }

    /// Check the headers a stateless request must carry against its body:
    /// `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` and the
    /// `Mcp-Param-*` headers of tool parameters marked with `x-mcp-header`.
    fn check_headers(&self, headers: &HeaderMap, req: &jsonrpc::Request, version: &str) -> Result<(), ErrorObject> {
        let get = |name: &str| -> Result<Option<String>, ErrorObject> {
            match headers.get(name) {
                None => Ok(None),
                Some(v) => match v.to_str() {
                    Ok(v) => decode_header_value(v)
                        .map(Some)
                        .ok_or_else(|| ErrorObject::header_mismatch(format!("Header mismatch: invalid {name} header"))),
                    Err(_) => Err(ErrorObject::header_mismatch(format!("Header mismatch: invalid {name} header"))),
                },
            }
        };
        let expect = |name: &str, body: &str| -> Result<(), ErrorObject> {
            match get(name)? {
                Some(v) if v == body => Ok(()),
                Some(v) => Err(ErrorObject::header_mismatch(format!(
                    "Header mismatch: {name} header value '{v}' does not match body value '{body}'"
                ))),
                None => Err(ErrorObject::header_mismatch(format!("Header mismatch: missing {name} header"))),
            }
        };
        // Decoding doesn't apply to these, but they never look encoded.
        expect(PROTOCOL_VERSION_HEADER, version)?;
        expect(METHOD_HEADER, &req.method)?;
        let param = |key: &str| req.params.as_ref().and_then(|p| p.get(key)).and_then(Value::as_str).unwrap_or("");
        match req.method.as_str() {
            "tools/call" | "prompts/get" => expect(NAME_HEADER, param("name"))?,
            "resources/read" => expect(NAME_HEADER, param("uri"))?,
            _ => {}
        }
        if req.method != "tools/call" {
            return Ok(());
        }
        let Some(schema) = self.server.tool_input_schema(param("name")) else {
            return Ok(());
        };
        let args = req.params.as_ref().and_then(|p| p.get("arguments"));
        let mut marked = Vec::new();
        header_params(&schema, &mut Vec::new(), &mut marked);
        for (path, name) in marked {
            let header = format!("mcp-param-{}", name.to_ascii_lowercase());
            let value = args.and_then(|a| path.iter().try_fold(a, |v, key| v.get(key))).filter(|v| !v.is_null());
            let given = get(&header)?;
            let matches = match (value, &given) {
                (None, None) => true,
                (Some(Value::String(s)), Some(h)) => s == h,
                (Some(Value::Bool(b)), Some(h)) => h == if *b { "true" } else { "false" },
                (Some(Value::Number(n)), Some(h)) => h.trim().parse::<f64>().ok() == n.as_f64(),
                // Not a primitive: the annotation is invalid, ignore it.
                (Some(v), None) if !(v.is_string() || v.is_boolean() || v.is_number()) => true,
                _ => false,
            };
            if !matches {
                return Err(ErrorObject::header_mismatch(format!(
                    "Header mismatch: Mcp-Param-{name} header does not match argument {}",
                    path.join(".")
                )));
            }
        }
        Ok(())
    }
}

/// `Mcp-Method` header (2026-07-28+).
pub const METHOD_HEADER: &str = "mcp-method";
/// `Mcp-Name` header (2026-07-28+).
pub const NAME_HEADER: &str = "mcp-name";

/// The parameters of a tool's input schema marked with `x-mcp-header`,
/// reachable through `properties` only: their path and header name.
fn header_params(schema: &Value, path: &mut Vec<String>, out: &mut Vec<(Vec<String>, String)>) {
    let Some(props) = schema.get("properties").and_then(Value::as_object) else { return };
    for (key, prop) in props {
        path.push(key.clone());
        if let Some(name) = prop.get("x-mcp-header").and_then(Value::as_str) {
            out.push((path.clone(), name.to_string()));
        }
        header_params(prop, path, out);
        path.pop();
    }
}

/// A header value, decoded if it uses the `=?base64?...?=` form.
fn decode_header_value(v: &str) -> Option<String> {
    match v.strip_prefix("=?base64?").and_then(|v| v.strip_suffix("?=")) {
        Some(encoded) => String::from_utf8(base64_decode(encoded)?).ok(),
        None => Some(v.to_string()),
    }
}

/// Decode standard base64 (padding optional).
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0);
    for c in s.trim_end_matches('=').bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Forward a stateless request's messages to its SSE stream until its
/// response; a stream closed by the client cancels the request.
async fn stream_stateless(
    first: Message,
    mut rx: mpsc::UnboundedReceiver<Outbound>,
    id: RequestId,
    body: mpsc::Sender<Bytes>,
    session: Session,
    keepalive: Duration,
) {
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + keepalive, keepalive);
    let mut next = Some(first);
    loop {
        let msg = match next.take() {
            Some(msg) => msg,
            None => tokio::select! {
                out = rx.recv() => match out {
                    Some(Outbound::Message(msg)) => msg,
                    _ => break,
                },
                _ = body.closed() => break,
                _ = session.closed() => break,
                _ = tick.tick() => {
                    if body.send(Bytes::from_static(b": keepalive\n\n")).await.is_err() {
                        break;
                    }
                    continue;
                }
            },
        };
        let last = msg.response_id() == Some(&id);
        if body.send(sse_event(&msg)).await.is_err() || last {
            break;
        }
    }
    session.close();
}

async fn sweep(state: Weak<State>, timeout: Duration) {
    let period = (timeout / 4).clamp(Duration::from_secs(1), Duration::from_secs(60));
    loop {
        tokio::time::sleep(period).await;
        let Some(state) = state.upgrade() else { return };
        let expired: Vec<_> = {
            let mut sessions = state.sessions.lock().unwrap();
            let expired: Vec<_> = sessions
                .iter()
                .filter(|(_, s)| {
                    s.session.is_closed() || (!s.has_stream() && s.last_seen.lock().unwrap().elapsed() > timeout)
                })
                .map(|(id, _)| id.clone())
                .collect();
            expired.iter().filter_map(|id| sessions.remove(id)).collect()
        };
        for s in expired {
            tracing::debug!(session = s.session.id(), "session expired");
            s.session.close();
        }
    }
}

fn sse_event(msg: &Message) -> Bytes {
    let json = serde_json::to_string(msg).unwrap_or_default();
    Bytes::from(format!("event: message\ndata: {json}\n\n"))
}

/// Forward a POST's responses (and related messages) to its SSE stream,
/// until every request in it is answered.
async fn stream_responses(
    mut rx: mpsc::UnboundedReceiver<Outbound>,
    mut pending: HashSet<RequestId>,
    body: mpsc::Sender<Bytes>,
    session: Session,
) {
    while !pending.is_empty() {
        let out = tokio::select! {
            out = rx.recv() => out,
            _ = session.closed() => None,
        };
        match out {
            None => return,
            Some(Outbound::Done(id)) => {
                pending.remove(&id);
            }
            Some(Outbound::Batch(_)) => {}
            Some(Outbound::Message(msg)) => {
                if let Some(id) = msg.response_id() {
                    pending.remove(id);
                }
                if body.send(sse_event(&msg)).await.is_err() {
                    return; // the client went away; the requests still run
                }
            }
        }
    }
}

/// Wait for a POST's responses; other messages go to the session mailbox.
async fn collect_responses(
    mut rx: mpsc::UnboundedReceiver<Outbound>,
    mut pending: HashSet<RequestId>,
    session: &HttpSession,
) -> Vec<Message> {
    let mut responses = Vec::new();
    while !pending.is_empty() {
        let out = tokio::select! {
            out = rx.recv() => out,
            _ = session.session.closed() => None,
        };
        match out {
            None => break,
            Some(Outbound::Done(id)) => {
                pending.remove(&id);
            }
            Some(Outbound::Batch(_)) => {}
            Some(Outbound::Message(msg)) => match msg.response_id() {
                Some(id) => {
                    pending.remove(id);
                    responses.push(msg);
                }
                None => session.mailbox.push(msg),
            },
        }
    }
    responses
}

/// Feed a session's mailbox to its GET stream.
async fn stream_mailbox(
    session: Arc<HttpSession>,
    stop: CancellationToken,
    body: mpsc::Sender<Bytes>,
    keepalive: Duration,
) {
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + keepalive, keepalive);
    loop {
        while let Some(msg) = session.mailbox.pop() {
            if body.send(sse_event(&msg)).await.is_err() {
                session.mailbox.unpop(msg);
                stop.cancel();
                return;
            }
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = session.session.closed() => return,
            _ = session.mailbox.notify.notified() => {}
            _ = tick.tick() => {
                if body.send(Bytes::from_static(b": keepalive\n\n")).await.is_err() {
                    stop.cancel();
                    return;
                }
            }
        }
    }
}

/// The body of responses from [`StreamableHttp::handle`]: complete, or an
/// SSE stream.
pub struct McpBody(BodyKind);

enum BodyKind {
    Full(Option<Bytes>),
    Stream(mpsc::Receiver<Bytes>),
}

impl Body for McpBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        match &mut self.0 {
            BodyKind::Full(data) => Poll::Ready(data.take().map(|d| Ok(Frame::data(d)))),
            BodyKind::Stream(rx) => rx.poll_recv(cx).map(|d| d.map(|d| Ok(Frame::data(d)))),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(&self.0, BodyKind::Full(None))
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            BodyKind::Full(Some(d)) => SizeHint::with_exact(d.len() as u64),
            BodyKind::Full(None) => SizeHint::with_exact(0),
            BodyKind::Stream(_) => SizeHint::default(),
        }
    }
}

impl McpBody {
    pub(crate) fn full(data: impl Into<Bytes>) -> Self {
        McpBody(BodyKind::Full(Some(data.into())))
    }

    fn empty() -> Self {
        McpBody(BodyKind::Full(None))
    }

    /// Read the whole body (for tests and tools; don't use on a `GET`
    /// stream, it never ends).
    pub async fn collect_bytes(self) -> Bytes {
        match BodyExt::collect(self).await {
            Ok(c) => c.to_bytes(),
            Err(never) => match never {},
        }
    }
}

fn response(status: StatusCode, content_type: Option<&'static str>, body: McpBody) -> Response<McpBody> {
    let mut res = Response::new(body);
    *res.status_mut() = status;
    if let Some(ct) = content_type {
        res.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(ct));
    }
    res
}

fn plain(status: StatusCode, text: &'static str) -> Response<McpBody> {
    response(status, Some("text/plain; charset=utf-8"), McpBody::full(text))
}

fn empty(status: StatusCode) -> Response<McpBody> {
    response(status, None, McpBody::empty())
}

fn rpc_error(status: StatusCode, message: &str) -> Response<McpBody> {
    json_body(status, &Message::error(None, ErrorObject::new(-32000, message)), None)
}

fn with_session_id(mut res: Response<McpBody>, session_id: Option<&str>) -> Response<McpBody> {
    if let Some(id) = session_id.and_then(|id| HeaderValue::from_str(id).ok()) {
        res.headers_mut().insert(SESSION_ID_HEADER, id);
    }
    res
}

fn json_body(status: StatusCode, value: &impl serde::Serialize, session_id: Option<&str>) -> Response<McpBody> {
    let body = serde_json::to_vec(value).unwrap_or_default();
    with_session_id(response(status, Some("application/json"), McpBody::full(body)), session_id)
}

fn sse_response(rx: mpsc::Receiver<Bytes>, session_id: Option<&str>) -> Response<McpBody> {
    let mut res = response(StatusCode::OK, Some("text/event-stream"), McpBody(BodyKind::Stream(rx)));
    res.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    res.headers_mut().insert("x-accel-buffering", HeaderValue::from_static("no"));
    with_session_id(res, session_id)
}
