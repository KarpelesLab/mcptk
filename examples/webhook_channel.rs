//! A two-way Claude Code channel with permission relay: the Rust version of
//! the webhook example in the Claude Code channels reference.
//!
//! - `POST /` with an `X-Sender: dev` header pushes the body into the
//!   session as a channel event (or, for `yes <id>` / `no <id>`, answers a
//!   permission prompt).
//! - `GET /events` streams Claude's replies and permission prompts.
//!
//! Register it in `.mcp.json`:
//!     {"mcpServers": {"webhook": {"command": "cargo", "args": ["run", "-q", "--example", "webhook_channel"]}}}
//! then start Claude Code with:
//!     claude --dangerously-load-development-channels server:webhook
//! and in other terminals:
//!     curl -N localhost:8788/events
//!     curl -d "list the files in this directory" -H "X-Sender: dev" localhost:8788

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response, StatusCode};
use mcptk::channel::parse_permission_reply;
use mcptk::{ChannelEvent, Server, Session, Tool, ToolError};
use serde_json::Value;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Senders allowed to talk to Claude. A real bridge would check the chat
/// platform's user id.
const ALLOWED_SENDERS: &[&str] = &["dev"];

/// Outbound: everyone watching `GET /events`.
#[derive(Clone, Default)]
struct Listeners(Arc<Mutex<Vec<mpsc::UnboundedSender<String>>>>);

impl Listeners {
    fn send(&self, text: &str) {
        let chunk: String = text.lines().map(|l| format!("data: {l}\n")).collect::<String>() + "\n";
        self.0.lock().unwrap().retain(|tx| tx.send(chunk.clone()).is_ok());
    }

    fn subscribe(&self) -> mpsc::UnboundedReceiver<String> {
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = tx.send(": connected\n\n".into());
        self.0.lock().unwrap().push(tx);
        rx
    }
}

#[tokio::main]
async fn main() -> mcptk::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).init();
    let listeners = Listeners::default();

    let reply_out = listeners.clone();
    let prompt_out = listeners.clone();
    let server = Server::builder("webhook", "0.0.1")
        .instructions(
            "Messages arrive as <channel source=\"webhook\" chat_id=\"...\">. \
             Reply with the reply tool, passing the chat_id from the tag.",
        )
        .tool(
            Tool::new("reply", "Send a message back over this channel").input_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "chat_id": {"type": "string", "description": "The conversation to reply in"},
                    "text": {"type": "string", "description": "The message to send"}
                },
                "required": ["chat_id", "text"]
            })),
            move |_ctx, args| {
                let out = reply_out.clone();
                async move {
                    let field = |k| args.get(k).and_then(Value::as_str).unwrap_or_default().to_string();
                    out.send(&format!("Reply to {}: {}", field("chat_id"), field("text")));
                    Ok::<_, ToolError>("sent")
                }
            },
        )
        // Opt in to permission relay: Claude Code sends us tool approval
        // prompts, which we forward to the listeners.
        .channel_permission(move |_session, req| {
            let out = prompt_out.clone();
            async move { out.send(&req.prompt_text()) }
        })
        .build();

    let conn = server.connect_stdio();
    let session = conn.session().clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8788").await?;
    tokio::spawn(async move {
        let next_chat = Arc::new(AtomicU64::new(1));
        loop {
            let Ok((stream, _)) = listener.accept().await else { continue };
            let (session, listeners, next_chat) = (session.clone(), listeners.clone(), next_chat.clone());
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    handle(req, session.clone(), listeners.clone(), next_chat.clone())
                });
                let io = hyper_util::rt::TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });

    // Runs until Claude Code closes our stdin.
    conn.wait().await
}

type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;

fn text(status: StatusCode, body: &'static str) -> Response<Body> {
    let mut res = Response::new(Full::new(Bytes::from_static(body.as_bytes())).boxed_unsync());
    *res.status_mut() = status;
    res
}

async fn handle(
    req: Request<Incoming>,
    session: Session,
    listeners: Listeners,
    next_chat: Arc<AtomicU64>,
) -> Result<Response<Body>, Infallible> {
    // GET /events: an SSE stream of replies and permission prompts.
    if req.method() == hyper::Method::GET && req.uri().path() == "/events" {
        let mut res = Response::new(SseBody(listeners.subscribe()).boxed_unsync());
        res.headers_mut().insert("content-type", "text/event-stream".parse().unwrap());
        res.headers_mut().insert("cache-control", "no-cache".parse().unwrap());
        return Ok(res);
    }

    // Everything else is inbound: gate on the sender first.
    let sender = req.headers().get("x-sender").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    if !ALLOWED_SENDERS.contains(&sender.as_str()) {
        return Ok(text(StatusCode::FORBIDDEN, "forbidden"));
    }
    let path = req.uri().path().to_string();
    let Ok(body) = req.into_body().collect().await else {
        return Ok(text(StatusCode::BAD_REQUEST, "bad body"));
    };
    let body = String::from_utf8_lossy(&body.to_bytes()).into_owned();

    // A verdict goes to Claude Code, never to Claude.
    if let Some(verdict) = parse_permission_reply(&body) {
        let _ = session.permission_verdict(&verdict);
        return Ok(text(StatusCode::OK, "verdict recorded"));
    }

    let chat_id = next_chat.fetch_add(1, Ordering::Relaxed).to_string();
    let event = ChannelEvent::new(body).meta("chat_id", chat_id).meta("path", path);
    let _ = session.channel_event(&event);
    Ok(text(StatusCode::OK, "ok"))
}

/// A body streaming what a listener receives.
struct SseBody(mpsc::UnboundedReceiver<String>);

impl hyper::body::Body for SseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.poll_recv(cx).map(|s| s.map(|s| Ok(Frame::data(Bytes::from(s)))))
    }
}
