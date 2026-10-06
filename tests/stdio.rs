//! End-to-end tests over an in-memory byte stream, as a stdio client would
//! see the server.

use mcptk::types::{CompleteParams, Completion, CompletionRef, CreateMessageParams, LoggingLevel};
use mcptk::*;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines};

struct Client {
    writer: tokio::io::WriteHalf<DuplexStream>,
    lines: Lines<BufReader<tokio::io::ReadHalf<DuplexStream>>>,
    conn: Option<Connection>,
}

impl Client {
    fn connect(server: &Server) -> Client {
        let (client_side, server_side) = tokio::io::duplex(1 << 16);
        let (sr, sw) = tokio::io::split(server_side);
        let conn = server.connect_io(sr, sw);
        let (cr, cw) = tokio::io::split(client_side);
        Client { writer: cw, lines: BufReader::new(cr).lines(), conn: Some(conn) }
    }

    async fn send(&mut self, msg: Value) {
        let mut line = msg.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    async fn recv(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .expect("timed out waiting for the server")
            .unwrap()
            .expect("server closed the stream");
        serde_json::from_str(&line).unwrap()
    }

    async fn call(&mut self, id: i64, method: &str, params: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).await;
        loop {
            let msg = self.recv().await;
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
                    "protocolVersion": "2025-06-18",
                    "capabilities": capabilities,
                    "clientInfo": {"name": "test", "version": "1"}
                }),
            )
            .await;
        self.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"})).await;
        res
    }

    fn session(&self) -> &Session {
        self.conn.as_ref().unwrap().session()
    }
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct AddArgs {
    a: i64,
    b: i64,
}

#[derive(serde::Serialize, schemars::JsonSchema)]
struct Sum {
    sum: i64,
}

fn test_server() -> Server {
    Server::builder("test-server", "1.2.3")
        .title("Test")
        .instructions("Be nice.")
        .typed_tool(Tool::new("add", "Add numbers").read_only(), |_ctx, a: AddArgs| async move {
            Ok::<_, ToolError>(a.a + a.b).map(|sum| Json(Sum { sum }))
        })
        .tool(Tool::new("fail", "Always fails"), |_ctx, _args| async move { Err::<(), _>(ToolError::msg("it broke")) })
        .tool(Tool::new("io_fail", "Fails with an io error"), |_ctx, _args| async move {
            std::fs::read("/definitely/not/here")?;
            Ok::<_, ToolError>("unreachable")
        })
        .tool(Tool::new("slow", "Reports progress then sleeps"), |ctx, _args| async move {
            ctx.progress(0.5, Some(1.0), Some("halfway"))?;
            ctx.log(LoggingLevel::Warning, Some("slow"), "taking a while")?;
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<_, ToolError>("done")
        })
        .tool(Tool::new("ask", "Samples the client's LLM"), |ctx, _args| async move {
            let res = ctx
                .create_message(CreateMessageParams {
                    messages: vec![types::SamplingMessage { role: types::Role::User, content: Content::text("hi") }],
                    max_tokens: 10,
                    ..Default::default()
                })
                .await
                .map_err(ToolError::msg)?;
            Ok::<_, ToolError>(format!("model said: {}", res.content.as_text().unwrap_or("")))
        })
        .resource(Resource::new("mem://readme", "readme").mime_type("text/plain"), |_ctx, _uri| async move {
            Ok("hello world")
        })
        .resource_template(ResourceTemplate::new("mem://users/{id}", "user"), |_ctx, uri, vars| async move {
            Ok(ResourceContents::text(uri, Some("application/json"), json!({"id": vars["id"]}).to_string()))
        })
        .prompt(Prompt::new("greet", "Greet someone").argument("name", "Who", true), |_ctx, args| async move {
            Ok(format!("Please greet {}", args["name"]))
        })
        .completion(|_ctx, p: CompleteParams| async move {
            let names = ["alice", "albert", "bob"];
            let values = match p.reference {
                CompletionRef::Prompt { .. } => {
                    names.iter().filter(|n| n.starts_with(&p.argument.value)).map(|n| n.to_string()).collect()
                }
                _ => vec![],
            };
            Ok(Completion::new(values))
        })
        .build()
}

#[tokio::test]
async fn initialize_and_list() {
    let server = test_server();
    let mut c = Client::connect(&server);

    // Requests before initialize are refused.
    let early = c.call(99, "tools/list", json!({})).await;
    assert_eq!(early["error"]["code"], -32600);
    // Ping works anytime.
    assert_eq!(c.call(98, "ping", json!({})).await["result"], json!({}));

    let init = c.init(json!({})).await;
    let r = &init["result"];
    assert_eq!(r["protocolVersion"], "2025-06-18");
    assert_eq!(r["serverInfo"], json!({"name":"test-server","version":"1.2.3","title":"Test"}));
    assert_eq!(r["instructions"], "Be nice.");
    assert_eq!(r["capabilities"]["tools"], json!({"listChanged": true}));
    assert_eq!(r["capabilities"]["resources"], json!({"subscribe": true, "listChanged": true}));
    assert!(r["capabilities"]["completions"].is_object());
    assert!(r["capabilities"].get("experimental").is_none());

    let again = c.call(1, "initialize", json!({"protocolVersion":"2025-06-18"})).await;
    assert_eq!(again["error"]["code"], -32600);

    let tools = c.call(2, "tools/list", json!({})).await;
    let tools = tools["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 5);
    assert_eq!(tools[0]["name"], "add");
    assert_eq!(tools[0]["annotations"]["readOnlyHint"], true);
    let schema = &tools[0]["inputSchema"];
    assert_eq!(schema["type"], "object");
    assert!(schema.get("$schema").is_none());
    assert_eq!(schema["required"], json!(["a", "b"]));

    let unknown = c.call(3, "nope/nothing", json!({})).await;
    assert_eq!(unknown["error"]["code"], -32601);
}

#[tokio::test]
async fn unknown_protocol_version_gets_latest() {
    let server = test_server();
    let mut c = Client::connect(&server);
    let res = c.call(1, "initialize", json!({"protocolVersion":"1990-01-01","capabilities":{}})).await;
    assert_eq!(res["result"]["protocolVersion"], types::LATEST_PROTOCOL_VERSION);
}

#[tokio::test]
async fn tool_calls() {
    let server = test_server();
    let mut c = Client::connect(&server);
    c.init(json!({})).await;

    let ok = c.call(1, "tools/call", json!({"name":"add","arguments":{"a":2,"b":3}})).await;
    assert_eq!(ok["result"]["structuredContent"], json!({"sum": 5}));
    assert_eq!(ok["result"]["content"][0]["text"], r#"{"sum":5}"#);
    assert!(ok["result"].get("isError").is_none());

    let bad_args = c.call(2, "tools/call", json!({"name":"add","arguments":{"a":"x"}})).await;
    assert_eq!(bad_args["result"]["isError"], true);
    assert!(bad_args["result"]["content"][0]["text"].as_str().unwrap().starts_with("invalid arguments"));

    let fail = c.call(3, "tools/call", json!({"name":"fail"})).await;
    assert_eq!(fail["result"], json!({"content":[{"type":"text","text":"it broke"}],"isError":true}));

    let io_fail = c.call(4, "tools/call", json!({"name":"io_fail"})).await;
    assert_eq!(io_fail["result"]["isError"], true);

    let unknown = c.call(5, "tools/call", json!({"name":"missing"})).await;
    assert_eq!(unknown["error"]["code"], -32602);
}

#[tokio::test]
async fn progress_logging_and_cancellation() {
    let server = test_server();
    let mut c = Client::connect(&server);
    c.init(json!({})).await;

    c.send(
        json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"slow","_meta":{"progressToken":"tok"}}}),
    )
    .await;
    let progress = c.recv().await;
    assert_eq!(progress["method"], "notifications/progress");
    assert_eq!(progress["params"], json!({"progressToken":"tok","progress":0.5,"total":1.0,"message":"halfway"}));
    let log = c.recv().await;
    assert_eq!(log["method"], "notifications/message");
    assert_eq!(log["params"], json!({"level":"warning","logger":"slow","data":"taking a while"}));

    c.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}})).await;
    // No response comes for the cancelled request: the next message is the
    // answer to this ping.
    let pong = c.call(8, "ping", json!({})).await;
    assert_eq!(pong["id"], 8);

    // Raise the level: warnings are now filtered out.
    c.call(9, "logging/setLevel", json!({"level":"error"})).await;
    c.session().log(LoggingLevel::Warning, None, "hidden").unwrap();
    c.session().log(LoggingLevel::Error, None, "shown").unwrap();
    assert_eq!(c.recv().await["params"]["data"], "shown");
}

#[tokio::test]
async fn resources_and_prompts() {
    let server = test_server();
    let mut c = Client::connect(&server);
    c.init(json!({})).await;

    let list = c.call(1, "resources/list", json!({})).await;
    assert_eq!(list["result"]["resources"][0]["uri"], "mem://readme");
    let templates = c.call(2, "resources/templates/list", json!({})).await;
    assert_eq!(templates["result"]["resourceTemplates"][0]["uriTemplate"], "mem://users/{id}");

    let read = c.call(3, "resources/read", json!({"uri":"mem://readme"})).await;
    assert_eq!(
        read["result"]["contents"],
        json!([{"uri":"mem://readme","mimeType":"text/plain","text":"hello world"}])
    );
    let user = c.call(4, "resources/read", json!({"uri":"mem://users/42"})).await;
    assert_eq!(user["result"]["contents"][0]["text"], r#"{"id":"42"}"#);
    let missing = c.call(5, "resources/read", json!({"uri":"mem://nothing"})).await;
    assert_eq!(missing["error"]["code"], -32002);

    let prompts = c.call(6, "prompts/list", json!({})).await;
    assert_eq!(prompts["result"]["prompts"][0]["arguments"][0]["required"], true);
    let got = c.call(7, "prompts/get", json!({"name":"greet","arguments":{"name":"Bob"}})).await;
    assert_eq!(got["result"]["messages"], json!([{"role":"user","content":{"type":"text","text":"Please greet Bob"}}]));
    let missing_arg = c.call(8, "prompts/get", json!({"name":"greet"})).await;
    assert_eq!(missing_arg["error"]["code"], -32602);

    let completion = c
        .call(
            9,
            "completion/complete",
            json!({"ref":{"type":"ref/prompt","name":"greet"},"argument":{"name":"name","value":"al"}}),
        )
        .await;
    assert_eq!(completion["result"]["completion"]["values"], json!(["alice", "albert"]));

    // Subscriptions gate update notifications.
    server.notify_resource_updated("mem://readme");
    c.call(10, "resources/subscribe", json!({"uri":"mem://readme"})).await;
    server.notify_resource_updated("mem://readme");
    let updated = c.recv().await;
    assert_eq!(
        updated,
        json!({"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":"mem://readme"}})
    );
}

#[tokio::test]
async fn sampling_round_trip() {
    let server = test_server();
    let mut c = Client::connect(&server);

    // Without the capability, the tool reports an error.
    c.init(json!({})).await;
    let res = c.call(1, "tools/call", json!({"name":"ask"})).await;
    assert_eq!(res["result"]["isError"], true);

    let mut c = Client::connect(&server);
    c.init(json!({"sampling": {}})).await;
    c.send(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ask"}})).await;
    let req = c.recv().await;
    assert_eq!(req["method"], "sampling/createMessage");
    assert_eq!(req["params"]["maxTokens"], 10);
    c.send(json!({"jsonrpc":"2.0","id":req["id"],"result":{
        "role":"assistant","content":{"type":"text","text":"hello"},"model":"m"
    }}))
    .await;
    let res = c.recv().await;
    assert_eq!(res["id"], 2);
    assert_eq!(res["result"]["content"][0]["text"], "model said: hello");
}

#[tokio::test]
async fn dynamic_tools_notify() {
    let server = test_server();
    let mut c = Client::connect(&server);
    c.init(json!({})).await;
    // Wait until the server saw `initialized`.
    c.call(1, "ping", json!({})).await;

    server.add_tool(Tool::new("extra", "Added later"), |_ctx, _args| async move { Ok::<_, ToolError>("hi") });
    assert_eq!(c.recv().await["method"], "notifications/tools/list_changed");
    let res = c.call(2, "tools/call", json!({"name":"extra"})).await;
    assert_eq!(res["result"]["content"][0]["text"], "hi");

    assert!(server.remove_tool("extra"));
    assert_eq!(c.recv().await["method"], "notifications/tools/list_changed");
    assert!(!server.remove_tool("extra"));
}

#[tokio::test]
async fn tool_filter_per_session() {
    #[derive(Clone)]
    struct Admin;
    let server = Server::builder("filtered", "1")
        .tool(Tool::new("public", "Anyone"), |_c, _a| async move { Ok::<_, ToolError>("ok") })
        .tool(Tool::new("admin", "Admins only"), |_c, _a| async move { Ok::<_, ToolError>("ok") })
        .tool(Tool::new("elevate", "Become admin"), |ctx, _a| async move {
            ctx.session().set_data(Admin);
            ctx.session().notify_tools_list_changed()?;
            Ok::<_, ToolError>("you are admin")
        })
        .tool_filter(|session, tool| tool.name != "admin" || session.data::<Admin>().is_some())
        .build();
    let mut c = Client::connect(&server);
    c.init(json!({})).await;

    let list = c.call(1, "tools/list", json!({})).await;
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 2);
    assert_eq!(c.call(2, "tools/call", json!({"name":"admin"})).await["error"]["code"], -32602);

    c.call(3, "tools/call", json!({"name":"elevate"})).await;
    let list = c.call(4, "tools/list", json!({})).await;
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn malformed_input() {
    let server = test_server();
    let mut c = Client::connect(&server);
    c.writer.write_all(b"{not json\n").await.unwrap();
    let err = c.recv().await;
    assert_eq!(err["error"]["code"], -32700);
    assert_eq!(err["id"], Value::Null);

    c.send(json!({"jsonrpc":"2.0","id":5})).await;
    let err = c.recv().await;
    assert_eq!(err["error"]["code"], -32600);
    assert_eq!(err["id"], 5);
}

#[tokio::test]
async fn end_of_input_closes_session() {
    let server = test_server();
    let mut c = Client::connect(&server);
    c.init(json!({})).await;
    c.call(1, "ping", json!({})).await;
    assert_eq!(server.sessions().len(), 1);
    let session = c.session().clone();
    let conn = c.conn.take().unwrap();
    drop(c);
    tokio::time::timeout(Duration::from_secs(5), conn.wait()).await.unwrap().unwrap();
    assert!(session.is_closed());
    assert!(server.sessions().is_empty());
}
