//! End-to-end tests of the WebSocket transport over a loopback socket.
#![cfg(feature = "ws")]

use futures_util::{SinkExt, StreamExt};
use mcptk::ws::WebSocketServer;
use mcptk::*;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn server() -> Server {
    Server::builder("ws-test", "0.0.1")
        .channel()
        .tool(Tool::new("echo", "Echo the text back"), |_ctx, args: serde_json::Map<String, Value>| async move {
            Ok::<_, ToolError>(args["text"].as_str().unwrap_or_default().to_string())
        })
        .build()
}

async fn start(ws: WebSocketServer) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ws.serve_listener(listener));
    addr
}

async fn connect(addr: SocketAddr, origin: Option<&str>) -> Result<Socket, WsError> {
    let mut req = format!("ws://{addr}/mcp").into_client_request().unwrap();
    req.headers_mut().insert("sec-websocket-protocol", "mcp".parse().unwrap());
    if let Some(origin) = origin {
        req.headers_mut().insert("origin", origin.parse().unwrap());
    }
    let (socket, res) = tokio_tungstenite::connect_async(req).await?;
    assert_eq!(res.headers()["sec-websocket-protocol"], "mcp");
    Ok(socket)
}

async fn send(ws: &mut Socket, msg: Value) {
    ws.send(Message::text(msg.to_string())).await.unwrap();
}

async fn recv(ws: &mut Socket) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for the server")
            .expect("server closed the socket")
            .unwrap();
        match frame {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

async fn init(ws: &mut Socket) -> Value {
    send(
        ws,
        json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{
            "protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"claude-code","version":"2"}
        }}),
    )
    .await;
    let res = recv(ws).await;
    send(ws, json!({"jsonrpc":"2.0","method":"notifications/initialized"})).await;
    res
}

async fn wait_for_sessions(server: &Server, n: usize) {
    for _ in 0..100 {
        if server.sessions().len() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("expected {n} sessions, have {}", server.sessions().len());
}

#[tokio::test]
async fn session_over_websocket() {
    let server = server();
    let addr = start(WebSocketServer::new(server.clone())).await;
    let mut ws = connect(addr, None).await.unwrap();

    let res = init(&mut ws).await;
    assert_eq!(res["id"], 0);
    assert_eq!(res["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(res["result"]["serverInfo"]["name"], "ws-test");
    wait_for_sessions(&server, 1).await;

    send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hi"}}}),
    )
    .await;
    let res = recv(&mut ws).await;
    assert_eq!(res["id"], 1);
    assert_eq!(res["result"]["content"][0]["text"], "hi");

    // Server-initiated: a channel event reaches the client unprompted.
    assert_eq!(server.channel_event(&ChannelEvent::new("disk full").meta("severity", "high")), 1);
    assert_eq!(
        recv(&mut ws).await,
        json!({"jsonrpc":"2.0","method":"notifications/claude/channel","params":{
            "content":"disk full","meta":{"severity":"high"}
        }})
    );

    // Pings are answered.
    ws.send(Message::Ping(b"hello"[..].into())).await.unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(pong, Message::Pong(b"hello"[..].into()));

    // Invalid JSON gets a parse error, and the session goes on.
    ws.send(Message::text("{not json")).await.unwrap();
    assert_eq!(recv(&mut ws).await["error"]["code"], -32700);

    // Closing the socket ends the session.
    ws.close(None).await.unwrap();
    while let Some(Ok(_)) = ws.next().await {}
    wait_for_sessions(&server, 0).await;
}

#[tokio::test]
async fn closing_the_session_closes_the_socket() {
    let server = server();
    let addr = start(WebSocketServer::new(server.clone())).await;
    let mut ws = connect(addr, None).await.unwrap();
    init(&mut ws).await;
    wait_for_sessions(&server, 1).await;

    server.sessions()[0].close();
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap();
    assert!(matches!(frame, Some(Ok(Message::Close(_)))), "{frame:?}");
}

#[tokio::test]
async fn origin_checks() {
    let server = server();
    let addr = start(WebSocketServer::new(server.clone()).allow_origin("https://app.example")).await;

    match connect(addr, Some("https://evil.example")).await {
        Err(WsError::Http(res)) => assert_eq!(res.status(), 403),
        other => panic!("expected a 403, got {other:?}"),
    }
    // Localhost and allowed origins are accepted.
    connect(addr, Some("http://localhost:3000")).await.unwrap();
    connect(addr, Some("https://app.example")).await.unwrap();
}

#[tokio::test]
async fn oversized_messages_close_the_socket() {
    let server = server();
    let addr = start(WebSocketServer::new(server.clone()).max_message_size(1024)).await;
    let mut ws = connect(addr, None).await.unwrap();
    init(&mut ws).await;
    wait_for_sessions(&server, 1).await;

    ws.send(Message::text("x".repeat(4096))).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap();
    match frame {
        Some(Ok(Message::Close(Some(close)))) => assert_eq!(u16::from(close.code), 1009),
        other => panic!("expected a close frame, got {other:?}"),
    }
    wait_for_sessions(&server, 0).await;
}

#[tokio::test]
async fn non_upgrade_requests_are_refused() {
    let addr = start(WebSocketServer::new(server())).await;
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tcp.write_all(b"GET /mcp HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    tcp.read_to_end(&mut buf).await.unwrap();
    assert!(buf.starts_with(b"HTTP/1.1 426"), "{}", String::from_utf8_lossy(&buf));

    let mut tcp = TcpStream::connect(addr).await.unwrap();
    tcp.write_all(b"GET /other HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    tcp.read_to_end(&mut buf).await.unwrap();
    assert!(buf.starts_with(b"HTTP/1.1 404"), "{}", String::from_utf8_lossy(&buf));
}

#[cfg(feature = "http")]
#[tokio::test]
async fn shares_a_port_with_streamable_http() {
    let server = server();
    let ws = WebSocketServer::new(server.clone()).with_http(mcptk::http::StreamableHttp::new(server.clone()));
    let addr = start(ws).await;

    // WebSocket on /mcp...
    let mut socket = connect(addr, None).await.unwrap();
    assert_eq!(init(&mut socket).await["id"], 0);

    // ...and Streamable HTTP POSTs on the same path.
    let body = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}
    }})
    .to_string();
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let req = format!(
        "POST /mcp HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    tcp.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), tcp.read_to_end(&mut buf)).await.unwrap().unwrap();
    let text = String::from_utf8_lossy(&buf).to_lowercase();
    assert!(text.starts_with("http/1.1 200"), "{text}");
    assert!(text.contains("mcp-session-id:"), "{text}");
}

#[tokio::test]
async fn connect_ws_over_a_raw_stream() {
    use tokio_tungstenite::tungstenite::protocol::Role;
    let server = server();
    let (a, b) = tokio::io::duplex(1 << 16);
    let conn =
        server.connect_ws(WebSocketStream::from_raw_socket(a, Role::Server, Some(mcptk::ws::config(1 << 20))).await);
    let mut client = WebSocketStream::from_raw_socket(b, Role::Client, None).await;

    client
        .send(Message::text(
            json!({"jsonrpc":"2.0","id":7,"method":"initialize","params":{
                "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}
            }})
            .to_string(),
        ))
        .await
        .unwrap();
    let Some(Ok(Message::Text(text))) = client.next().await else { panic!("no response") };
    let res: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(res["id"], 7);

    let session = conn.session().clone();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), conn.wait()).await.unwrap().ok();
    assert!(session.is_closed());
}
