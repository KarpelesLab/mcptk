//! MCP Apps (`io.modelcontextprotocol/ui`): the JSON a host sees.

use mcptk::apps::{self, AppsBuilderExt, ToolUiExt, UiResource, UiSupport};
use mcptk::*;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines};

struct Client {
    writer: tokio::io::WriteHalf<DuplexStream>,
    lines: Lines<BufReader<tokio::io::ReadHalf<DuplexStream>>>,
    _conn: Connection,
}

impl Client {
    fn connect(server: &Server) -> Client {
        let (client_side, server_side) = tokio::io::duplex(1 << 16);
        let (sr, sw) = tokio::io::split(server_side);
        let conn = server.connect_io(sr, sw);
        let (cr, cw) = tokio::io::split(client_side);
        Client { writer: cw, lines: BufReader::new(cr).lines(), _conn: conn }
    }

    async fn send(&mut self, msg: Value) {
        let mut line = msg.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    async fn call(&mut self, id: i64, method: &str, params: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).await;
        loop {
            let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
                .await
                .expect("timed out waiting for the server")
                .unwrap()
                .expect("server closed the stream");
            let msg: Value = serde_json::from_str(&line).unwrap();
            if msg["id"] == id && msg.get("method").is_none() {
                return msg;
            }
        }
    }

    async fn init(&mut self, capabilities: Value) -> Value {
        let res = self
            .call(
                0,
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": capabilities,
                    "clientInfo": {"name": "host", "version": "1"}
                }),
            )
            .await;
        self.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"})).await;
        res
    }
}

const HTML: &str = "<!doctype html><html><body>hi</body></html>";

fn ui_caps() -> Value {
    json!({"extensions": {"io.modelcontextprotocol/ui": {"mimeTypes": ["text/html;profile=mcp-app"]}}})
}

fn server() -> Server {
    Server::builder("apps", "1.0")
        .ui_resource(
            UiResource::new("ui://apps/view", "view", HTML)
                .description("The view")
                .connect_domain("https://api.example.com")
                .prefers_border(true),
        )
        .tool(Tool::new("show", "Show the view").ui_resource("ui://apps/view"), |ctx, _args| async move {
            let mode = if ctx.supports_ui() { "ui" } else { "text" };
            Ok::<_, ToolError>(CallToolResult::text(format!("mode={mode}")).structured(json!({"n": 1})))
        })
        .tool(Tool::new("refresh", "Refresh the view").ui_resource("ui://apps/view").app_only(), |_ctx, _args| async {
            Ok::<_, ToolError>("refreshed")
        })
        .tool_filter(apps::hide_app_only_tools_without_ui)
        .build()
}

#[tokio::test]
async fn host_with_ui() {
    let mut c = Client::connect(&server());
    let init = c.init(ui_caps()).await;
    assert_eq!(init["result"]["capabilities"]["extensions"], json!({"io.modelcontextprotocol/ui": {}}));

    let tools = c.call(1, "tools/list", json!({})).await;
    assert_eq!(
        tools["result"]["tools"],
        json!([
            {
                "name": "show",
                "description": "Show the view",
                "inputSchema": {"type": "object"},
                "_meta": {
                    "ui": {"resourceUri": "ui://apps/view"},
                    "ui/resourceUri": "ui://apps/view"
                }
            },
            {
                "name": "refresh",
                "description": "Refresh the view",
                "inputSchema": {"type": "object"},
                "_meta": {
                    "ui": {"resourceUri": "ui://apps/view", "visibility": ["app"]},
                    "ui/resourceUri": "ui://apps/view"
                }
            }
        ])
    );

    let ui = json!({"csp": {"connectDomains": ["https://api.example.com"]}, "prefersBorder": true});
    let resources = c.call(2, "resources/list", json!({})).await;
    assert_eq!(
        resources["result"]["resources"],
        json!([{
            "uri": "ui://apps/view",
            "name": "view",
            "description": "The view",
            "mimeType": "text/html;profile=mcp-app",
            "_meta": {"ui": ui}
        }])
    );

    let read = c.call(3, "resources/read", json!({"uri": "ui://apps/view"})).await;
    assert_eq!(
        read["result"],
        json!({"contents": [{
            "uri": "ui://apps/view",
            "mimeType": "text/html;profile=mcp-app",
            "text": HTML,
            "_meta": {"ui": ui}
        }]})
    );

    let call = c.call(4, "tools/call", json!({"name": "show", "arguments": {}})).await;
    assert_eq!(
        call["result"],
        json!({"content": [{"type": "text", "text": "mode=ui"}], "structuredContent": {"n": 1}})
    );

    let call = c.call(5, "tools/call", json!({"name": "refresh", "arguments": {}})).await;
    assert_eq!(call["result"]["content"][0]["text"], "refreshed");
}

#[tokio::test]
async fn text_only_host() {
    let mut c = Client::connect(&server());
    c.init(json!({})).await;

    let tools = c.call(1, "tools/list", json!({})).await;
    let names: Vec<&str> =
        tools["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["show"]);

    let call = c.call(2, "tools/call", json!({"name": "show", "arguments": {}})).await;
    assert_eq!(call["result"]["content"][0]["text"], "mode=text");

    // Client capabilities carried by the request itself (2026-07-28 style) win.
    let call = c
        .call(
            3,
            "tools/call",
            json!({
                "name": "show",
                "arguments": {},
                "_meta": {"io.modelcontextprotocol/clientCapabilities": ui_caps()}
            }),
        )
        .await;
    assert_eq!(call["result"]["content"][0]["text"], "mode=ui");
}
