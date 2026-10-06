//! The tasks extension (`io.modelcontextprotocol/tasks`), end to end over an
//! in-memory byte stream and over Streamable HTTP.
#![cfg(feature = "http")]

use mcptk::tasks::{EXTENSION_ID, TaskConfig, TaskContext, TaskMode};
use mcptk::types::ElicitParams;
use mcptk::*;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines};
use tokio::sync::Semaphore;

struct Client {
    writer: tokio::io::WriteHalf<DuplexStream>,
    lines: Lines<BufReader<tokio::io::ReadHalf<DuplexStream>>>,
    _conn: Connection,
    next_id: i64,
}

/// `_meta` declaring the extension (and elicitation) for one request.
fn caps() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {
            "elicitation": {},
            "extensions": { EXTENSION_ID: {} }
        }
    })
}

impl Client {
    async fn connect(server: &Server) -> Client {
        let (client_side, server_side) = tokio::io::duplex(1 << 16);
        let (sr, sw) = tokio::io::split(server_side);
        let conn = server.connect_io(sr, sw);
        let (cr, cw) = tokio::io::split(client_side);
        let mut c = Client { writer: cw, lines: BufReader::new(cr).lines(), _conn: conn, next_id: 1 };
        let init = c
            .call_raw(
                "initialize",
                json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}}),
            )
            .await;
        assert_eq!(init["result"]["capabilities"]["extensions"], json!({ EXTENSION_ID: {} }));
        c.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"})).await;
        c
    }

    async fn send(&mut self, msg: Value) {
        let mut line = msg.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    async fn call_raw(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
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

    /// A request declaring the extension.
    async fn call(&mut self, method: &str, mut params: Value) -> Value {
        params["_meta"] = caps();
        self.call_raw(method, params).await
    }

    async fn get(&mut self, task_id: &str) -> Value {
        self.call("tasks/get", json!({"taskId": task_id})).await
    }

    /// Poll until the task's status is no longer `from`.
    async fn wait_past(&mut self, task_id: &str, from: &str) -> Value {
        for _ in 0..200 {
            let res = self.get(task_id).await;
            if res["result"]["status"] != from {
                return res;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("task {task_id} stuck in {from}");
    }

    /// Call a tool, expecting a task; returns its id.
    async fn start(&mut self, tool: &str) -> String {
        let res = self.call("tools/call", json!({"name": tool, "arguments": {}})).await;
        let r = &res["result"];
        assert_eq!(r["resultType"], "task", "{res}");
        assert_eq!(r["status"], "working");
        r["taskId"].as_str().unwrap().to_string()
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Fixture {
    server: Server,
    gate: Arc<Semaphore>,
    dropped: Arc<AtomicBool>,
}

fn fixture(config: TaskConfig) -> Fixture {
    let gate = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let (g, d) = (gate.clone(), dropped.clone());
    let server = Server::builder("tasks-test", "1")
        .tasks(config)
        .task_tool(Tool::new("gated", "Waits for the test to let it finish"), move |ctx: TaskContext, _args| {
            let gate = g.clone();
            async move {
                ctx.progress(0.0, None, Some("waiting at the gate")).await?;
                gate.acquire().await?.forget();
                Ok::<_, ToolError>(format!("done (task: {})", ctx.is_task()))
            }
        })
        .task_tool(Tool::new("forever", "Never finishes"), move |_ctx: TaskContext, _args| {
            let flag = DropFlag(d.clone());
            async move {
                let _flag = flag;
                std::future::pending::<()>().await;
                Ok::<_, ToolError>("unreachable")
            }
        })
        .task_tool(Tool::new("quick", "Finishes right away"), |_ctx: TaskContext, _args| async move {
            Ok::<_, ToolError>("quick")
        })
        .task_tool(Tool::new("broken", "Fails with a JSON-RPC error"), |_ctx: TaskContext, _args| async move {
            Err::<(), _>(ToolError::protocol(jsonrpc::ErrorObject::internal("API rate limit exceeded")))
        })
        .task_tool(Tool::new("oops", "Fails as a tool"), |_ctx: TaskContext, _args| async move {
            Err::<(), _>(ToolError::msg("invalid input"))
        })
        .task_tool(Tool::new("hello", "Asks the user's name"), |ctx: TaskContext, _args| async move {
            let res = ctx
                .elicit(ElicitParams::form(
                    "Please enter your name.",
                    json!({"type":"object","properties":{"name":{"type":"string"}}}),
                ))
                .await?;
            let name = res.content.and_then(|c| c.get("name").cloned()).unwrap_or_default();
            Ok::<_, ToolError>(format!("Hello, {}!", name.as_str().unwrap_or("?")))
        })
        .task_tool(Tool::new("must_task", "Only runs as a task"), |_ctx: TaskContext, _args| async move {
            Ok::<_, ToolError>("ok")
        })
        .task_mode("must_task", TaskMode::Required)
        .tool(Tool::new("plain", "Not a task tool"), |_ctx, _args| async move { Ok::<_, ToolError>("plain") })
        .build();
    Fixture { server, gate, dropped }
}

#[tokio::test]
async fn create_poll_complete() {
    let f = fixture(TaskConfig::new().poll_interval(Some(Duration::from_millis(250))));
    let mut c = Client::connect(&f.server).await;

    let res = c.call("tools/call", json!({"name":"gated","arguments":{}})).await;
    let r = &res["result"];
    assert_eq!(r["resultType"], "task");
    assert_eq!(r["status"], "working");
    assert_eq!(r["ttlMs"], 3_600_000);
    assert_eq!(r["pollIntervalMs"], 250);
    assert!(r["createdAt"].as_str().unwrap().ends_with('Z'));
    let id = r["taskId"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 32);

    // Still working, with the progress message as status message.
    let mut polled = c.get(&id).await;
    for _ in 0..100 {
        if polled["result"]["statusMessage"] == "waiting at the gate" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        polled = c.get(&id).await;
    }
    let p = &polled["result"];
    assert_eq!(p["resultType"], "complete");
    assert_eq!(p["status"], "working");
    assert_eq!(p["statusMessage"], "waiting at the gate");
    assert_eq!(p["taskId"], id.as_str());
    assert!(p.get("result").is_none());
    assert_eq!(f.server.task(&id).await.unwrap().status, tasks::TaskStatus::Working);

    f.gate.add_permits(1);
    let done = c.wait_past(&id, "working").await;
    let d = &done["result"];
    assert_eq!(d["resultType"], "complete");
    assert_eq!(d["status"], "completed");
    assert_eq!(d["result"], json!({"content":[{"type":"text","text":"done (task: true)"}]}));

    // Polling a finished task keeps returning it.
    assert_eq!(c.get(&id).await["result"]["status"], "completed");
}

#[tokio::test]
async fn without_the_extension_tools_run_inline() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;

    f.gate.add_permits(1);
    let res = c.call_raw("tools/call", json!({"name":"gated"})).await;
    assert_eq!(res["result"], json!({"content":[{"type":"text","text":"done (task: false)"}]}));

    // A handshake protocol version in _meta isn't a valid stateless request.
    let mut meta = caps();
    meta["io.modelcontextprotocol/protocolVersion"] = "2025-11-25".into();
    let res = c.call_raw("tools/call", json!({"name":"quick","_meta":meta})).await;
    assert_eq!(res["error"]["code"], -32022);

    // Tools that must run as tasks need the extension.
    let res = c.call_raw("tools/call", json!({"name":"must_task"})).await;
    assert_eq!(res["error"]["code"], -32021);
    assert_eq!(res["error"]["data"], json!({"requiredCapabilities":{"extensions":{EXTENSION_ID:{}}}}));
    let res = c.call("tools/call", json!({"name":"must_task"})).await;
    assert_eq!(res["result"]["resultType"], "task");

    // Plain tools never become tasks.
    let res = c.call("tools/call", json!({"name":"plain"})).await;
    assert_eq!(res["result"]["resultType"], "complete");
    assert_eq!(res["result"]["content"], json!([{"type":"text","text":"plain"}]));

    // Unknown tools are an error, not a task.
    let res = c.call("tools/call", json!({"name":"missing"})).await;
    assert_eq!(res["error"]["code"], -32602);
}

#[tokio::test]
async fn failures() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;

    // A JSON-RPC error fails the task.
    let id = c.start("broken").await;
    let res = c.wait_past(&id, "working").await;
    let r = &res["result"];
    assert_eq!(r["status"], "failed");
    assert_eq!(r["error"], json!({"code": -32603, "message": "API rate limit exceeded"}));
    assert_eq!(r["statusMessage"], "API rate limit exceeded");
    assert!(r.get("result").is_none());

    // A tool error completes it, with isError.
    let id = c.start("oops").await;
    let res = c.wait_past(&id, "working").await;
    assert_eq!(res["result"]["status"], "completed");
    assert_eq!(res["result"]["result"], json!({"content":[{"type":"text","text":"invalid input"}],"isError":true}));
}

#[tokio::test]
async fn cancellation() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;

    let id = c.start("forever").await;
    assert_eq!(c.get(&id).await["result"]["status"], "working");
    let ack = c.call("tasks/cancel", json!({"taskId": id})).await;
    assert_eq!(ack["result"]["resultType"], "complete", "{ack}");
    let res = c.wait_past(&id, "working").await;
    assert_eq!(res["result"]["status"], "cancelled");
    assert!(f.dropped.load(Ordering::SeqCst), "the handler was dropped");

    // Cancelling a finished task is acknowledged and changes nothing.
    let ack = c.call("tasks/cancel", json!({"taskId": id})).await;
    assert_eq!(ack["result"]["resultType"], "complete", "{ack}");
    assert_eq!(c.get(&id).await["result"]["status"], "cancelled");

    // The server can cancel too.
    let id = c.start("forever").await;
    assert!(f.server.cancel_task(&id));
    assert_eq!(c.wait_past(&id, "working").await["result"]["status"], "cancelled");
    assert!(!f.server.cancel_task(&id));
}

#[tokio::test]
async fn input_required_and_update() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;

    let id = c.start("hello").await;
    let res = c.wait_past(&id, "working").await;
    let r = &res["result"];
    assert_eq!(r["status"], "input_required");
    assert_eq!(
        r["inputRequests"],
        json!({"input-1": {"method": "elicitation/create", "params": {
            "message": "Please enter your name.",
            "requestedSchema": {"type":"object","properties":{"name":{"type":"string"}}}
        }}})
    );
    // Polling again shows the same outstanding request.
    assert_eq!(c.get(&id).await["result"]["inputRequests"], r["inputRequests"]);

    // Responses to unknown keys are ignored.
    let ack = c.call("tasks/update", json!({"taskId": id, "inputResponses": {"nope": {"action": "cancel"}}})).await;
    assert_eq!(ack["result"]["resultType"], "complete", "{ack}");
    assert_eq!(c.get(&id).await["result"]["status"], "input_required");

    let ack = c
        .call(
            "tasks/update",
            json!({"taskId": id, "inputResponses": {"input-1": {"action": "accept", "content": {"name": "Luca"}}}}),
        )
        .await;
    assert_eq!(ack["result"]["resultType"], "complete", "{ack}");
    let res = c.wait_past(&id, "input_required").await;
    let res = if res["result"]["status"] == "working" { c.wait_past(&id, "working").await } else { res };
    assert_eq!(res["result"]["status"], "completed");
    assert!(res["result"].get("inputRequests").is_none());
    assert_eq!(res["result"]["result"]["content"][0]["text"], "Hello, Luca!");
}

#[tokio::test]
async fn expiry() {
    let f = fixture(TaskConfig::new().ttl(Some(Duration::from_millis(300))));
    let mut c = Client::connect(&f.server).await;

    let done = c.start("quick").await;
    assert_eq!(c.wait_past(&done, "working").await["result"]["status"], "completed");
    let running = c.start("forever").await;
    assert_eq!(c.get(&running).await["result"]["ttlMs"], 300);

    tokio::time::sleep(Duration::from_millis(400)).await;
    for id in [&done, &running] {
        let res = c.get(id).await;
        assert_eq!(res["error"]["code"], -32602, "{res}");
        let message = res["error"]["message"].as_str().unwrap();
        assert!(message.contains("expired") || message.contains("not found"), "{message}");
    }
    // The running one was stopped.
    for _ in 0..100 {
        if f.dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(f.dropped.load(Ordering::SeqCst));
    assert!(f.server.task(&done).await.is_none());
}

#[tokio::test]
async fn protocol_errors() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;

    for method in ["tasks/get", "tasks/cancel"] {
        let res = c.call(method, json!({"taskId": "0123456789abcdef"})).await;
        assert_eq!(res["error"]["code"], -32602);
        assert!(res["error"]["message"].as_str().unwrap().ends_with("Task not found"));
    }
    let res = c.call("tasks/update", json!({"taskId": "nope", "inputResponses": {}})).await;
    assert_eq!(res["error"]["code"], -32602);
    let res = c.call("tasks/get", json!({})).await;
    assert_eq!(res["error"]["code"], -32602);

    // Task methods need the extension declared on the request.
    let id = c.start("quick").await;
    let res = c.call_raw("tasks/get", json!({"taskId": id})).await;
    assert_eq!(res["error"]["code"], -32021);

    // The 2025-11-25 experimental methods are gone.
    for method in ["tasks/result", "tasks/list"] {
        let res = c.call(method, json!({"taskId": id})).await;
        assert_eq!(res["error"]["code"], -32601);
    }
}

#[tokio::test]
async fn runtime_task_tools() {
    let server = Server::builder("rt", "1").tasks(TaskConfig::new()).build();
    server.add_task_tool(Tool::new("later", "Added later"), |ctx: TaskContext, _args| async move {
        Ok::<_, ToolError>(ctx.task_id().unwrap_or("inline").len().to_string())
    });
    let mut c = Client::connect(&server).await;
    let id = c.start("later").await;
    assert_eq!(c.wait_past(&id, "working").await["result"]["result"]["content"][0]["text"], "32");

    server.set_task_mode("later", TaskMode::Never);
    let res = c.call("tools/call", json!({"name":"later"})).await;
    assert_eq!(res["result"]["content"][0]["text"], "6");

    assert!(server.remove_task_tool("later"));
    let res = c.call("tools/call", json!({"name":"later"})).await;
    assert_eq!(res["error"]["code"], -32602);
}

#[cfg(feature = "http")]
mod http {
    use super::*;
    use ::http::{Request, StatusCode};
    use bytes::Bytes;
    use http_body_util::Full;
    use mcptk::http::StreamableHttp;

    async fn post(http: &StreamableHttp, session: Option<&str>, body: Value) -> (Option<String>, Value) {
        post_as(http, None, session, body).await
    }

    async fn post_as(
        http: &StreamableHttp,
        token: Option<&str>,
        session: Option<&str>,
        body: Value,
    ) -> (Option<String>, Value) {
        // Stateless (2026-07-28) requests need headers matching the body.
        let version =
            body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str().unwrap_or("2025-11-25");
        let mut req = Request::post("/mcp")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version);
        if let Some(method) = body["method"].as_str() {
            req = req.header("mcp-method", method);
        }
        if let Some(name) = body["params"]["name"].as_str() {
            req = req.header("mcp-name", name);
        }
        if let Some(s) = session {
            req = req.header("mcp-session-id", s);
        }
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let res = http.handle(req.body(Full::new(Bytes::from(body.to_string()))).unwrap()).await;
        assert_eq!(res.status(), StatusCode::OK);
        let sid = res.headers().get("mcp-session-id").map(|v| v.to_str().unwrap().to_string());
        (sid, serde_json::from_slice(&res.into_body().collect_bytes().await).unwrap())
    }

    async fn session(http: &StreamableHttp) -> String {
        session_as(http, None).await
    }

    async fn session_as(http: &StreamableHttp, token: Option<&str>) -> String {
        let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}
        }});
        let (sid, _) = post_as(http, token, None, init).await;
        sid.unwrap()
    }

    fn request(id: i64, method: &str, mut params: Value) -> Value {
        params["_meta"] = caps();
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
    }

    /// Tasks live in the server: one created in a session can be polled
    /// from another, with each request its own HTTP exchange.
    #[tokio::test]
    async fn tasks_outlive_requests_and_sessions() {
        let f = fixture(TaskConfig::new());
        let http = StreamableHttp::new(f.server.clone()).json_response(true);
        let a = session(&http).await;
        let b = session(&http).await;

        let (_, res) = post(&http, Some(&a), request(2, "tools/call", json!({"name":"gated"}))).await;
        assert_eq!(res["result"]["resultType"], "task");
        let id = res["result"]["taskId"].as_str().unwrap().to_string();

        let (_, res) = post(&http, Some(&b), request(3, "tasks/get", json!({"taskId": id}))).await;
        assert_eq!(res["result"]["status"], "working");

        f.gate.add_permits(1);
        let mut status = Value::Null;
        for n in 0..200 {
            let (_, res) = post(&http, Some(&b), request(10 + n, "tasks/get", json!({"taskId": id}))).await;
            status = res["result"].clone();
            if status["status"] != "working" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(status["status"], "completed");
        assert_eq!(status["result"]["content"][0]["text"], "done (task: true)");
    }

    /// With OAuth, a task belongs to the subject that created it, and its
    /// handler still sees the caller's identity while running in the
    /// background.
    #[tokio::test]
    async fn tasks_belong_to_their_creator() {
        use mcptk::auth::{AuthInfo, ProtectedResource, StaticTokens};
        let server = Server::builder("auth-tasks", "1")
            .task_tool(Tool::new("whoami", "Who called"), |ctx: TaskContext, _args| async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let who = ctx.request().auth().map(|a| a.subject.clone()).unwrap_or_default();
                Ok::<_, ToolError>(who)
            })
            .build();
        let tokens = StaticTokens::new().token("a", AuthInfo::new("alice")).token("b", AuthInfo::new("bob"));
        let http = StreamableHttp::new(server)
            .json_response(true)
            .auth(ProtectedResource::new("https://mcp.example.com/mcp", tokens));
        let alice = session_as(&http, Some("a")).await;
        let bob = session_as(&http, Some("b")).await;

        let call = request(2, "tools/call", json!({"name":"whoami"}));
        let (_, res) = post_as(&http, Some("a"), Some(&alice), call).await;
        let id = res["result"]["taskId"].as_str().unwrap().to_string();
        assert!(res["result"].get("owner").is_none());

        for method in ["tasks/get", "tasks/cancel"] {
            let (_, res) = post_as(&http, Some("b"), Some(&bob), request(3, method, json!({"taskId": id}))).await;
            assert_eq!(res["error"]["code"], -32602, "{res}");
            assert!(res["error"]["message"].as_str().unwrap().ends_with("Task not found"));
        }

        let mut status = Value::Null;
        for n in 0..200 {
            let get = request(10 + n, "tasks/get", json!({"taskId": id}));
            let (_, res) = post_as(&http, Some("a"), Some(&alice), get).await;
            status = res["result"].clone();
            if status["status"] != "working" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(status["status"], "completed");
        assert_eq!(status["result"]["content"][0]["text"], "alice");
        assert!(status.get("owner").is_none());
    }
}

#[tokio::test]
async fn status_notifications_on_listen_streams() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;
    let id = c.start("gated").await;

    // Watch the task (and one that doesn't exist, which isn't honored).
    c.send(json!({"jsonrpc":"2.0","id":"watch","method":"subscriptions/listen","params":{
        "notifications": {"taskIds": [id, "no-such-task"]},
        "_meta": caps()
    }}))
    .await;
    let mut next = async || -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), c.lines.next_line()).await.unwrap().unwrap().unwrap();
        serde_json::from_str(&line).unwrap()
    };
    let ack = next().await;
    assert_eq!(ack["method"], "notifications/subscriptions/acknowledged", "{ack}");
    assert_eq!(ack["params"]["notifications"], json!({"taskIds": [id]}));

    // The task finishes: its new state is pushed, tagged with the stream.
    f.gate.add_permits(1);
    let update = next().await;
    assert_eq!(update["method"], "notifications/tasks", "{update}");
    let p = &update["params"];
    assert_eq!(p["taskId"], id);
    assert_eq!(p["status"], "completed");
    assert_eq!(p["result"]["content"][0]["text"], "done (task: true)");
    assert_eq!(p["_meta"]["io.modelcontextprotocol/subscriptionId"], "watch");
    assert!(p.get("owner").is_none());
}

#[tokio::test]
async fn replacing_a_task_tool_with_a_plain_one() {
    let f = fixture(TaskConfig::new());
    let mut c = Client::connect(&f.server).await;
    f.server.add_tool(Tool::new("quick", "Now a plain tool"), |_ctx, _args| async move { Ok::<_, ToolError>("plain") });
    let res = c.call("tools/call", json!({"name": "quick", "arguments": {}})).await;
    assert_eq!(res["result"]["resultType"], "complete", "{res}");
    assert_eq!(res["result"]["content"][0]["text"], "plain");
}
