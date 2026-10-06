//! Streamable HTTP transport tests.

use ::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use mcptk::http::{McpBody, StreamableHttp};
use mcptk::*;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn server() -> Server {
    Server::builder("http-test", "1")
        .channel()
        .tool(Tool::new("echo", "Echo"), |ctx, args| async move {
            ctx.progress(1.0, None, None)?;
            Ok::<_, ToolError>(args.get("text").and_then(Value::as_str).unwrap_or("").to_string())
        })
        .build()
}

fn post(session: Option<&str>, accept: &str, body: Value) -> Request<Full<Bytes>> {
    let mut req = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", accept)
        .header("mcp-protocol-version", "2025-06-18");
    if let Some(s) = session {
        req = req.header("mcp-session-id", s);
    }
    req.body(Full::new(Bytes::from(body.to_string()))).unwrap()
}

const BOTH: &str = "application/json, text/event-stream";

fn init_body() -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}
    }})
}

async fn json_of(res: ::http::Response<McpBody>) -> Value {
    serde_json::from_slice(&res.into_body().collect_bytes().await).unwrap()
}

/// Parse the `data:` lines of an SSE body.
fn sse_messages(body: &[u8]) -> Vec<Value> {
    std::str::from_utf8(body)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|d| serde_json::from_str(d).unwrap())
        .collect()
}

async fn initialize(http: &StreamableHttp) -> String {
    let res = http.handle(post(None, "application/json", init_body())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let id = res.headers()["mcp-session-id"].to_str().unwrap().to_string();
    let notif = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
    assert_eq!(http.handle(post(Some(&id), BOTH, notif)).await.status(), StatusCode::ACCEPTED);
    id
}

#[tokio::test]
async fn json_mode_lifecycle() {
    let http = StreamableHttp::new(server()).json_response(true);
    let res = http.handle(post(None, BOTH, init_body())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "application/json");
    let id = res.headers()["mcp-session-id"].to_str().unwrap().to_string();
    let init = json_of(res).await;
    assert_eq!(init["result"]["serverInfo"]["name"], "http-test");

    // Requests need the session id...
    let res = http.handle(post(None, BOTH, json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    // ...a known one.
    let res = http.handle(post(Some("nope"), BOTH, json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let res = http.handle(post(Some(&id), BOTH, json!({"jsonrpc":"2.0","method":"notifications/initialized"}))).await;
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    let res = http
        .handle(post(
            Some(&id),
            BOTH,
            json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                "name":"echo","arguments":{"text":"hi"}
            }}),
        ))
        .await;
    assert_eq!(json_of(res).await["result"]["content"][0]["text"], "hi");

    // Batches get a batch back.
    let res = http
        .handle(post(
            Some(&id),
            BOTH,
            json!([{"jsonrpc":"2.0","id":4,"method":"ping"},{"jsonrpc":"2.0","id":5,"method":"ping"}]),
        ))
        .await;
    assert_eq!(json_of(res).await.as_array().unwrap().len(), 2);

    let del = Request::delete("/mcp").header("mcp-session-id", &id).body(Full::new(Bytes::new())).unwrap();
    assert_eq!(http.handle(del).await.status(), StatusCode::OK);
    let res = http.handle(post(Some(&id), BOTH, json!({"jsonrpc":"2.0","id":6,"method":"ping"}))).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sse_mode_streams_progress_then_response() {
    let http = StreamableHttp::new(server());
    let id = initialize(&http).await;
    let res = http
        .handle(post(
            Some(&id),
            BOTH,
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                "name":"echo","arguments":{"text":"yo"},"_meta":{"progressToken":7}
            }}),
        ))
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "text/event-stream");
    let body = tokio::time::timeout(Duration::from_secs(5), res.into_body().collect_bytes()).await.unwrap();
    let msgs = sse_messages(&body);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["method"], "notifications/progress");
    assert_eq!(msgs[1]["result"]["content"][0]["text"], "yo");
}

#[tokio::test]
async fn get_stream_delivers_server_messages() {
    let server = server();
    let http = StreamableHttp::new(server.clone());
    let id = initialize(&http).await;

    // Queued before the stream opens...
    server.channel_event(&ChannelEvent::new("first"));
    let get = Request::get("/mcp")
        .header("accept", "text/event-stream")
        .header("mcp-session-id", &id)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let res = http.handle(get).await;
    assert_eq!(res.status(), StatusCode::OK);
    let mut body = res.into_body();
    // ...and sent while it is open.
    server.channel_event(&ChannelEvent::new("second").meta("n", "2"));

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let frame = tokio::time::timeout(Duration::from_secs(5), body.frame()).await.unwrap().unwrap().unwrap();
        seen.extend(sse_messages(&frame.into_data().unwrap()));
    }
    assert_eq!(seen[0]["params"]["content"], "first");
    assert_eq!(seen[1]["params"], json!({"content":"second","meta":{"n":"2"}}));
}

#[tokio::test]
async fn rejects_bad_requests() {
    let http = StreamableHttp::new(server());

    let mut evil = post(None, BOTH, init_body());
    evil.headers_mut().insert("origin", "https://evil.example".parse().unwrap());
    assert_eq!(http.handle(evil).await.status(), StatusCode::FORBIDDEN);

    let mut local = post(None, BOTH, init_body());
    local.headers_mut().insert("origin", "http://localhost:3000".parse().unwrap());
    assert_eq!(http.handle(local).await.status(), StatusCode::OK);

    let allowed = StreamableHttp::new(server()).allow_origin("https://app.example");
    let mut ok = post(None, BOTH, init_body());
    ok.headers_mut().insert("origin", "https://app.example".parse().unwrap());
    assert_eq!(allowed.handle(ok).await.status(), StatusCode::OK);

    let mut old = post(None, BOTH, init_body());
    old.headers_mut().insert("mcp-protocol-version", "1999-01-01".parse().unwrap());
    assert_eq!(http.handle(old).await.status(), StatusCode::BAD_REQUEST);

    let wrong_path = Request::post("/other").body(Full::new(Bytes::new())).unwrap();
    assert_eq!(http.handle(wrong_path).await.status(), StatusCode::NOT_FOUND);

    let garbage = Request::post("/mcp").body(Full::new(Bytes::from_static(b"{nope"))).unwrap();
    let res = http.handle(garbage).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_of(res).await["error"]["code"], -32700);

    let small = StreamableHttp::new(server()).max_body_size(10);
    assert_eq!(small.handle(post(None, BOTH, init_body())).await.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let put = Request::put("/mcp").body(Full::new(Bytes::new())).unwrap();
    assert_eq!(http.handle(put).await.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn idle_sessions_expire() {
    let server = server();
    let http = StreamableHttp::new(server.clone()).session_timeout(Duration::from_millis(100));
    let id = initialize(&http).await;
    assert_eq!(server.sessions().len(), 1);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(server.sessions().is_empty());
    let res = http.handle(post(Some(&id), BOTH, json!({"jsonrpc":"2.0","id":2,"method":"ping"}))).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn serves_over_tcp() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(StreamableHttp::new(server()).json_response(true).serve_listener(listener));

    let body = init_body().to_string();
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nAccept: {BOTH}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response)).await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.to_ascii_lowercase().contains("mcp-session-id: "));
    assert!(response.contains(r#""serverInfo":{"name":"http-test","version":"1"}"#));
}
