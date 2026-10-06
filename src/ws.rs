//! The WebSocket transport (server side).
//!
//! Each WebSocket connection is one MCP session, handled like a stdio
//! connection: every JSON-RPC message travels in its own text frame, both
//! ways. This is what Claude Code expects from a `"type": "ws"` server in
//! `.mcp.json`, and what the TypeScript SDK's `WebSocketClientTransport`
//! speaks. Clients ask for the [`SUBPROTOCOL`] `mcp`, which the server
//! echoes back.
//!
//! The HTTP upgrade runs on hyper, like [`StreamableHttp`](crate::http::StreamableHttp),
//! so the same `Origin` checks apply, and both can share a port and path:
//! see [`WebSocketServer::with_http`].
//!
//! ```no_run
//! # async fn run(server: mcptk::Server) -> mcptk::Result<()> {
//! mcptk::ws::WebSocketServer::new(server).serve("127.0.0.1:8080").await // ws://127.0.0.1:8080/mcp
//! # }
//! ```
//!
//! Serve it with [`WebSocketServer::serve`], route upgrade requests to
//! [`WebSocketServer::handle`] from your own hyper/axum server, or hand an
//! already upgraded socket to [`Server::connect_ws`].

use crate::error::{Error, Result};
use crate::io::Connection;
#[cfg(feature = "http")]
use crate::jsonrpc;
use crate::server::{Outbound, Outlet, Server, Session, dispatch_text};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};
use hyper::body::{Body, Frame, SizeHint};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};

pub use tokio_tungstenite;

/// The WebSocket subprotocol MCP clients ask for.
pub const SUBPROTOCOL: &str = "mcp";

/// How long to wait for the client to answer our close frame.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Settings {
    path: Option<String>,
    allowed_origins: Vec<String>,
    any_origin: bool,
    max_message_size: usize,
    keepalive: Option<Duration>,
}

/// An MCP server served over WebSocket. Cheap to clone.
///
/// ```no_run
/// # async fn run(server: mcptk::Server) -> mcptk::Result<()> {
/// use mcptk::ws::WebSocketServer;
///
/// WebSocketServer::new(server)
///     .allow_origin("https://app.example") // localhost origins are always allowed
///     .serve("127.0.0.1:8080")             // endpoint: ws://127.0.0.1:8080/mcp
///     .await
/// # }
/// ```
#[derive(Clone)]
pub struct WebSocketServer {
    server: Server,
    settings: Arc<Settings>,
    #[cfg(feature = "http")]
    http: Option<crate::http::StreamableHttp>,
    #[cfg(feature = "http")]
    auth: Option<Arc<crate::auth::ProtectedResource>>,
}

/// Who a connection was authenticated as, if the server requires it.
#[derive(Clone, Default)]
struct Identity {
    #[cfg(feature = "http")]
    auth: Option<(Arc<crate::auth::ProtectedResource>, Arc<crate::auth::AuthInfo>)>,
}

impl WebSocketServer {
    pub fn new(server: Server) -> Self {
        WebSocketServer {
            server,
            settings: Arc::new(Settings {
                path: Some("/mcp".into()),
                allowed_origins: Vec::new(),
                any_origin: false,
                max_message_size: 4 << 20,
                keepalive: Some(Duration::from_secs(25)),
            }),
            #[cfg(feature = "http")]
            http: None,
            #[cfg(feature = "http")]
            auth: None,
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

    /// Accept connections from this browser origin (e.g. `https://app.example`).
    ///
    /// Upgrade requests carrying an `Origin` header are refused unless it is
    /// listed here or is a localhost origin, to block DNS rebinding attacks
    /// and cross-site WebSocket hijacking. Requests without one (non-browser
    /// clients such as Claude Code) are always accepted.
    pub fn allow_origin(mut self, origin: impl Into<String>) -> Self {
        self.settings().allowed_origins.push(origin.into());
        self
    }

    /// Accept connections from any origin.
    pub fn allow_any_origin(mut self) -> Self {
        self.settings().any_origin = true;
        self
    }

    /// The largest message (and frame) accepted from the client (default
    /// 4 MiB). Bigger ones close the connection with code 1009.
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.settings().max_message_size = bytes;
        self
    }

    /// How often to ping idle clients (default 25s), so proxies keep the
    /// connection open and dead peers are noticed. `None` disables it.
    pub fn keepalive(mut self, interval: Option<Duration>) -> Self {
        self.settings().keepalive = interval;
        self
    }

    /// Hand requests that aren't WebSocket upgrades for this endpoint to
    /// `http`, so one port (and one path) serves both transports.
    #[cfg(feature = "http")]
    pub fn with_http(mut self, http: crate::http::StreamableHttp) -> Self {
        self.http = Some(http);
        self
    }

    /// Require an OAuth access token to connect, as
    /// [`StreamableHttp::auth`](crate::http::StreamableHttp::auth) does.
    ///
    /// The token comes in the upgrade request's `Authorization: Bearer`
    /// header (Claude Code sends the server's configured `headers`); without
    /// a valid one the upgrade is refused with `401` and a
    /// `WWW-Authenticate` challenge, or `403` if it lacks the scopes every
    /// request needs. Handlers then see the identity with `ctx.auth()`.
    /// A request needing more scopes (tool or method scopes) gets a
    /// JSON-RPC error, and the connection is closed once the token expires.
    ///
    /// The Protected Resource Metadata is served here too, unless
    /// [`with_http`](Self::with_http) hands those requests to a
    /// `StreamableHttp` (give it the same `auth`).
    #[cfg(feature = "http")]
    pub fn auth(mut self, resource: crate::auth::ProtectedResource) -> Self {
        self.auth = Some(Arc::new(resource));
        self
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
                let conn = hyper::server::conn::http1::Builder::new().serve_connection(io, service).with_upgrades();
                if let Err(e) = conn.await {
                    tracing::debug!("connection error: {e}");
                }
            });
        }
    }

    /// Handle one HTTP request: accept a WebSocket upgrade and serve an MCP
    /// session on it in the background.
    ///
    /// The request must come from hyper (or a framework on it, such as axum),
    /// whose connection is served `with_upgrades()`, so the upgraded socket
    /// can be taken from it. Other requests get an error response, or go to
    /// the [`StreamableHttp`](crate::http::StreamableHttp) set with
    /// [`with_http`](Self::with_http).
    pub async fn handle<B>(&self, req: Request<B>) -> Response<WsBody>
    where
        B: Body,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let on_path = self.settings.path.as_deref().is_none_or(|p| req.uri().path() == p);
        if !(on_path && is_upgrade_request(&req)) {
            #[cfg(feature = "http")]
            if let Some(http) = &self.http {
                return http.handle(req).await.map(|b| WsBody(BodyKind::Http(b)));
            }
            #[cfg(feature = "http")]
            if let Some(auth) = &self.auth
                && let Some(res) = auth.serve_metadata(&req)
            {
                return res.map(|b| WsBody(BodyKind::Http(b)));
            }
            if !on_path {
                return plain(StatusCode::NOT_FOUND, "not found");
            }
            let mut res = plain(StatusCode::UPGRADE_REQUIRED, "expected a WebSocket upgrade");
            res.headers_mut().insert(header::UPGRADE, HeaderValue::from_static("websocket"));
            res.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
            return res;
        }
        if !origin_allowed(&self.settings, req.headers().get(header::ORIGIN)) {
            return plain(StatusCode::FORBIDDEN, "Forbidden: origin not allowed");
        }
        #[allow(unused_mut)]
        let mut identity = Identity::default();
        #[cfg(feature = "http")]
        if let Some(auth) = &self.auth {
            match auth.authenticate_connection(req.headers()).await {
                Ok(info) => identity.auth = Some((auth.clone(), info)),
                Err(res) => return res.map(|b| WsBody(BodyKind::Http(b))),
            }
        }
        self.upgrade(req, identity)
    }

    fn upgrade<B>(&self, mut req: Request<B>, identity: Identity) -> Response<WsBody> {
        let headers = req.headers();
        if headers.get(header::SEC_WEBSOCKET_VERSION).is_none_or(|v| v.as_bytes() != b"13") {
            let mut res = plain(StatusCode::UPGRADE_REQUIRED, "unsupported WebSocket version");
            res.headers_mut().insert(header::SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
            return res;
        }
        let Some(key) = headers.get(header::SEC_WEBSOCKET_KEY) else {
            return plain(StatusCode::BAD_REQUEST, "missing Sec-WebSocket-Key");
        };
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        let mcp = offers_subprotocol(headers);
        let Some(on_upgrade) = req.extensions_mut().remove::<hyper::upgrade::OnUpgrade>() else {
            tracing::warn!("WebSocket upgrade request without hyper's OnUpgrade extension");
            return plain(StatusCode::INTERNAL_SERVER_ERROR, "connection can't be upgraded");
        };

        let server = self.server.clone();
        let settings = self.settings.clone();
        tokio::spawn(async move {
            let upgraded = match on_upgrade.await {
                Ok(u) => u,
                Err(e) => {
                    tracing::debug!("WebSocket upgrade failed: {e}");
                    return;
                }
            };
            let io = hyper_util::rt::TokioIo::new(upgraded);
            let ws = WebSocketStream::from_raw_socket(io, Role::Server, Some(config(settings.max_message_size))).await;
            if let Err(e) = connect(&server, ws, settings.keepalive, identity).wait().await {
                tracing::debug!("WebSocket session ended with an error: {e}");
            }
        });

        let mut res = Response::new(WsBody(BodyKind::Full(None)));
        *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        let h = res.headers_mut();
        h.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        h.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        h.insert(header::SEC_WEBSOCKET_ACCEPT, HeaderValue::from_str(&accept).expect("base64 is a valid header"));
        if mcp {
            // Clients (browsers, Node's `ws`) fail the connection unless the
            // subprotocol they asked for is echoed.
            h.insert(header::SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(SUBPROTOCOL));
        }
        res
    }
}

/// The WebSocket configuration [`WebSocketServer`] uses, for sockets you
/// upgrade yourself before [`Server::connect_ws`].
pub fn config(max_message_size: usize) -> WebSocketConfig {
    WebSocketConfig::default().max_message_size(Some(max_message_size)).max_frame_size(Some(max_message_size))
}

/// Whether `req` asks for a WebSocket upgrade (`GET` with `Connection:
/// upgrade` and `Upgrade: websocket`), for routing.
pub fn is_upgrade_request<B>(req: &Request<B>) -> bool {
    let has_token = |name: header::HeaderName, token: &str| {
        req.headers()
            .get_all(name)
            .iter()
            .any(|v| v.to_str().is_ok_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token))))
    };
    req.method() == Method::GET && has_token(header::CONNECTION, "upgrade") && has_token(header::UPGRADE, "websocket")
}

fn offers_subprotocol(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .any(|v| v.to_str().is_ok_and(|v| v.split(',').any(|p| p.trim() == SUBPROTOCOL)))
}

fn origin_allowed(settings: &Settings, origin: Option<&HeaderValue>) -> bool {
    let Some(origin) = origin else { return true };
    let Ok(origin) = origin.to_str() else { return false };
    if settings.any_origin || settings.allowed_origins.iter().any(|o| o == origin) {
        return true;
    }
    let host = origin.split_once("://").map_or(origin, |(_, rest)| rest);
    let host = match host.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => host.split(':').next().unwrap_or(""),
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

impl Server {
    /// Serve one session over an already upgraded WebSocket, in the
    /// background.
    ///
    /// Each text frame carries one JSON-RPC message (or batch). Pings are
    /// answered; the session ends when the socket closes, on an error, or
    /// when closed (which sends a close frame). Limit message sizes with the
    /// socket's [`WebSocketConfig`] (see [`config`]).
    pub fn connect_ws<S>(&self, ws: WebSocketStream<S>) -> Connection
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        connect(self, ws, None, Identity::default())
    }

    /// Serve one session over an already upgraded WebSocket until it ends.
    pub async fn serve_ws<S>(&self, ws: WebSocketStream<S>) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.connect_ws(ws).wait().await
    }
}

fn connect<S>(server: &Server, ws: WebSocketStream<S>, keepalive: Option<Duration>, identity: Identity) -> Connection
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel();
    let outlet = Outlet::Channel(tx);
    let session = Session::new(server.clone(), outlet.clone());
    let s = session.clone();
    let task = tokio::spawn(async move {
        let result = run(ws, &s, &outlet, rx, keepalive, identity).await;
        s.close();
        result
    });
    Connection { session, task }
}

async fn run<S>(
    mut ws: WebSocketStream<S>,
    session: &Session,
    outlet: &Outlet,
    mut rx: mpsc::UnboundedReceiver<Outbound>,
    keepalive: Option<Duration>,
    identity: Identity,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let period = keepalive.unwrap_or(Duration::from_secs(3600));
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    // With keepalive pings, a live client is heard from (a pong at least)
    // every period: one silent for three is gone.
    let mut last_heard = tokio::time::Instant::now();
    loop {
        tokio::select! {
            biased;
            out = rx.recv() => {
                // The outlet we hold keeps the channel open.
                if let Some(out) = out {
                    send(&mut ws, out).await?;
                }
            }
            _ = session.closed() => {
                // Flush what is already queued, then say goodbye.
                while let Ok(out) = rx.try_recv() {
                    send(&mut ws, out).await?;
                }
                return close(ws, None).await;
            }
            frame = ws.next() => { last_heard = tokio::time::Instant::now(); match frame {
                None => return Ok(()),
                Some(Ok(WsMessage::Text(text))) => dispatch(text.as_bytes(), session, outlet, &identity),
                // Lenient: some clients send JSON in binary frames.
                Some(Ok(WsMessage::Binary(data))) => dispatch(&data, session, outlet, &identity),
                // tungstenite answers pings, and replies to a close frame on
                // the next poll, after which the stream ends.
                Some(Ok(_)) => {}
                Some(Err(tokio_tungstenite::tungstenite::Error::Capacity(e))) => {
                    tracing::debug!("WebSocket message too large: {e}");
                    let frame = CloseFrame { code: CloseCode::Size, reason: "message too large".into() };
                    return close(ws, Some(frame)).await;
                }
                Some(Err(e)) => return Err(ws_error(e)),
            }},
            _ = tick.tick(), if keepalive.is_some() => {
                if last_heard.elapsed() > period * 3 {
                    tracing::debug!("WebSocket client unresponsive, closing");
                    return Err(Error::Other("WebSocket client stopped responding".into()));
                }
                ws.send(WsMessage::Ping(Bytes::new())).await.map_err(ws_error)?;
            }
        }
    }
}

fn dispatch(text: &[u8], session: &Session, outlet: &Outlet, identity: &Identity) {
    if text.iter().all(u8::is_ascii_whitespace) {
        return;
    }
    #[cfg(not(feature = "http"))]
    let _ = identity;
    #[cfg(feature = "http")]
    if let Some((_, info)) = &identity.auth
        && info.is_expired()
    {
        tracing::debug!("access token expired, closing the WebSocket session");
        return session.close();
    }
    dispatch_text(text, outlet, |msg, reply| {
        #[cfg(feature = "http")]
        if let Some((resource, info)) = &identity.auth {
            if let jsonrpc::Message::Request(req) = &msg {
                let missing = resource.missing_scopes(req, info);
                if !missing.is_empty() {
                    let error = jsonrpc::ErrorObject::new(-32000, "Forbidden: the token lacks required scopes")
                        .with_data(serde_json::json!({ "error": "insufficient_scope", "scopes": missing }));
                    reply.send(Outbound::Message(jsonrpc::Message::error(Some(req.id.clone()), error)));
                    return;
                }
            }
            return crate::auth::scope(Some(info.clone()), || session.handle(msg, reply));
        }
        session.handle(msg, reply)
    });
}

async fn send<S: AsyncRead + AsyncWrite + Unpin>(ws: &mut WebSocketStream<S>, out: Outbound) -> Result<()> {
    let Some(json) = out.to_json()? else { return Ok(()) };
    let text = String::from_utf8(json).map_err(|e| Error::Other(e.to_string()))?;
    ws.send(WsMessage::text(text)).await.map_err(ws_error)
}

/// Send a close frame and wait (briefly) for the client's reply.
async fn close<S: AsyncRead + AsyncWrite + Unpin>(mut ws: WebSocketStream<S>, frame: Option<CloseFrame>) -> Result<()> {
    if ws.close(frame).await.is_err() {
        return Ok(()); // already closed
    }
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, async { while let Some(Ok(_)) = ws.next().await {} }).await;
    Ok(())
}

fn ws_error(e: tokio_tungstenite::tungstenite::Error) -> Error {
    use tokio_tungstenite::tungstenite::Error as E;
    match e {
        E::Io(e) => Error::Io(e),
        e => Error::Other(format!("WebSocket error: {e}")),
    }
}

/// The body of responses from [`WebSocketServer::handle`].
pub struct WsBody(BodyKind);

enum BodyKind {
    Full(Option<Bytes>),
    #[cfg(feature = "http")]
    Http(crate::http::McpBody),
}

impl Body for WsBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        #[cfg(not(feature = "http"))]
        let _ = cx;
        match &mut self.0 {
            BodyKind::Full(data) => Poll::Ready(data.take().map(|d| Ok(Frame::data(d)))),
            #[cfg(feature = "http")]
            BodyKind::Http(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            BodyKind::Full(data) => data.is_none(),
            #[cfg(feature = "http")]
            BodyKind::Http(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            BodyKind::Full(Some(d)) => SizeHint::with_exact(d.len() as u64),
            BodyKind::Full(None) => SizeHint::with_exact(0),
            #[cfg(feature = "http")]
            BodyKind::Http(body) => body.size_hint(),
        }
    }
}

impl WsBody {
    /// Read the whole body (for tests and tools; don't use on an SSE
    /// stream from [`WebSocketServer::with_http`], it may never end).
    pub async fn collect_bytes(self) -> Bytes {
        match http_body_util::BodyExt::collect(self).await {
            Ok(c) => c.to_bytes(),
            Err(never) => match never {},
        }
    }
}

fn plain(status: StatusCode, text: &'static str) -> Response<WsBody> {
    let mut res = Response::new(WsBody(BodyKind::Full(Some(Bytes::from_static(text.as_bytes())))));
    *res.status_mut() = status;
    res.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    res
}
