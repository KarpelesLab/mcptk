//! Claude Code channel support, end to end over a byte stream.

use mcptk::channel::parse_permission_reply;
use mcptk::*;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

#[tokio::test]
async fn channel_events_and_permission_relay() {
    let (prompts_tx, mut prompts_rx) = mpsc::unbounded_channel::<(Session, PermissionRequest)>();
    let server = Server::builder("webhook", "0.0.1")
        .instructions("Events arrive as <channel source=\"webhook\">.")
        .channel_permission(move |session, req| {
            let tx = prompts_tx.clone();
            async move {
                let _ = tx.send((session, req));
            }
        })
        .on_initialized(|session| async move {
            let _ = session.channel_event(&ChannelEvent::new("hello from the channel").meta("kind", "greeting"));
        })
        .build();

    let (client, server_side) = tokio::io::duplex(1 << 16);
    let (sr, sw) = tokio::io::split(server_side);
    let conn = server.connect_io(sr, sw);
    let (cr, mut cw) = tokio::io::split(client);
    let mut lines = BufReader::new(cr).lines();
    let mut recv = async || -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line()).await.unwrap().unwrap().unwrap();
        serde_json::from_str(&line).unwrap()
    };
    let mut send = async |v: Value| {
        cw.write_all(format!("{v}\n").as_bytes()).await.unwrap();
    };

    send(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"claude-code","version":"2"}
    }}))
    .await;
    let init = recv().await;
    assert_eq!(
        init["result"]["capabilities"]["experimental"],
        json!({"claude/channel": {}, "claude/channel/permission": {}})
    );
    // A one-way channel without tools doesn't advertise them.
    assert!(init["result"]["capabilities"].get("tools").is_none());

    send(json!({"jsonrpc":"2.0","method":"notifications/initialized"})).await;
    let event = recv().await;
    assert_eq!(
        event,
        json!({"jsonrpc":"2.0","method":"notifications/claude/channel","params":{
            "content":"hello from the channel","meta":{"kind":"greeting"}
        }})
    );

    // Broadcast from the server reaches the session too.
    assert_eq!(server.channel_event(&ChannelEvent::new("ping")), 1);
    assert_eq!(recv().await["params"], json!({"content":"ping"}));

    // Claude Code relays a permission prompt; the handler gets it.
    send(json!({"jsonrpc":"2.0","method":"notifications/claude/channel/permission_request","params":{
        "request_id":"abcde","tool_name":"Bash","description":"List files","input_preview":"{\"command\":\"ls\"}"
    }}))
    .await;
    let (session, req) = tokio::time::timeout(Duration::from_secs(5), prompts_rx.recv()).await.unwrap().unwrap();
    assert_eq!(req.tool_name, "Bash");
    assert!(req.prompt_text().contains("Reply \"yes abcde\" or \"no abcde\""));
    assert_eq!(session.id(), conn.session().id());

    // The approver answers from their phone.
    let verdict = parse_permission_reply("Yes abcde").unwrap();
    session.permission_verdict(&verdict).unwrap();
    assert_eq!(
        recv().await,
        json!({"jsonrpc":"2.0","method":"notifications/claude/channel/permission","params":{
            "request_id":"abcde","behavior":"allow"
        }})
    );
}
