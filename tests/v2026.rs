//! Protocol revision 2026-07-28: stateless requests with per-request
//! `_meta`, over a byte stream and over Streamable HTTP, alongside the
//! handshake-based revisions.

use ::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use mcptk::http::{McpBody, StreamableHttp};
use mcptk::types::{CacheScope, ElicitAction, ElicitParams, LoggingLevel};
use mcptk::*;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines};

const V: &str = "2026-07-28";

fn meta(caps: Value) -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": V,
        "io.modelcontextprotocol/clientCapabilities": caps,
        "io.modelcontextprotocol/clientInfo": {"name": "modern", "version": "9"},
    })
}

/// Params with the per-request `_meta` (and optional extra `_meta` keys).
fn with_meta(mut params: Value, caps: Value, extra: Value) -> Value {
    let mut m = meta(caps);
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        m[k] = v;
    }
    params["_meta"] = m;
    params
}

fn server() -> Server {
    Server::builder("modern-server", "2.0")
        .instructions("Be modern.")
        .cache_ttl(Duration::from_secs(60))
        .tool(Tool::new("echo", "Echo"), |ctx, args| async move {
            ctx.progress(1.0, None, None)?;
            ctx.log(LoggingLevel::Info, None, "echoing")?;
            let who = ctx.client_info().map(|c| c.name.clone()).unwrap_or_default();
            let text = args.get("text").and_then(Value::as_str).unwrap_or("").to_string();
            Ok::<_, ToolError>(format!("{text} (from {who}, {})", ctx.protocol_version().unwrap_or("?")))
        })
        .tool(Tool::new("list", "Returns an array"), |_ctx, _args| async move { Ok::<_, ToolError>(Json(vec![1, 2])) })
        .tool(Tool::new("ask", "Asks two questions"), |ctx, _args| async move {
            let schema = json!({"type":"object","properties":{"v":{"type":"string"}}});
            let a = ctx.elicit(ElicitParams::form("first?", schema.clone())).await?;
            let b = ctx.elicit(ElicitParams::form("second?", schema)).await?;
            let v =
                |r: &types::ElicitResult| r.content.as_ref().and_then(|c| c["v"].as_str()).unwrap_or("").to_string();
            Ok::<_, ToolError>(format!("{} {}", v(&a), v(&b)))
        })
        .tool(Tool::new("confirm", "Explicit input request"), |ctx, _args| async move {
            match ctx.input_response::<types::ElicitResult>("ok")? {
                Some(r) if r.action == ElicitAction::Accept => {
                    Ok(format!("confirmed with state {}", ctx.request_state().unwrap_or("-")))
                }
                Some(_) => Ok("declined".to_string()),
                None => Err(InputRequired::new()
                    .elicit("ok", ElicitParams::form("Sure?", json!({"type":"object","properties":{}})))
                    .state("s1")
                    .into()),
            }
        })
        .tool(
            Tool::new("region", "Has a header parameter").input_schema(json!({
                "type": "object",
                "properties": {
                    "region": {"type": "string", "x-mcp-header": "Region"},
                    "n": {"type": "integer", "x-mcp-header": "N"},
                    "q": {"type": "string"}
                }
            })),
            |_ctx, _args| async move { Ok::<_, ToolError>("ok") },
        )
        .resource(Resource::new("mem://a", "a"), |_ctx, _uri| async move { Ok("A") })
        .prompt(Prompt::new("p", "A prompt"), |_ctx, _args| async move { Ok("hi".to_string()) })
        .build()
}

// --- byte stream (stdio) ---

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
        self.writer.write_all(format!("{msg}\n").as_bytes()).await.unwrap();
    }

    async fn recv(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .expect("timed out waiting for the server")
            .unwrap()
            .expect("server closed the stream");
        serde_json::from_str(&line).unwrap()
    }

    /// Send a request; return the messages received up to its response.
    async fn call_all(&mut self, id: i64, method: &str, params: Value) -> Vec<Value> {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).await;
        let mut seen = Vec::new();
        loop {
            let msg = self.recv().await;
            let done = msg["id"] == id && msg.get("method").is_none();
            seen.push(msg);
            if done {
                return seen;
            }
        }
    }

    async fn call(&mut self, id: i64, method: &str, params: Value) -> Value {
        self.call_all(id, method, params).await.pop().unwrap()
    }
}

#[tokio::test]
async fn discover_and_stateless_requests() {
    let mut c = Client::connect(&server());

    let d = c.call(1, "server/discover", with_meta(json!({}), json!({}), json!({}))).await;
    let r = &d["result"];
    assert_eq!(r["resultType"], "complete");
    assert_eq!(r["supportedVersions"][0], V);
    assert!(r["supportedVersions"].as_array().unwrap().contains(&json!("2025-11-25")));
    assert_eq!(r["capabilities"]["tools"], json!({"listChanged": true}));
    assert_eq!(r["instructions"], "Be modern.");
    assert_eq!(r["ttlMs"], 60000);
    assert_eq!(r["cacheScope"], "public");
    assert_eq!(r["_meta"]["io.modelcontextprotocol/serverInfo"], json!({"name":"modern-server","version":"2.0"}));

    // No initialize needed.
    let list = c.call(2, "tools/list", with_meta(json!({}), json!({}), json!({}))).await;
    let r = &list["result"];
    assert_eq!(r["resultType"], "complete");
    assert_eq!(r["ttlMs"], 60000);
    assert_eq!(r["cacheScope"], "public");
    let names: Vec<_> = r["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["echo", "list", "ask", "confirm", "region"]);

    // Progress, but no logs without a log level.
    let msgs = c
        .call_all(
            3,
            "tools/call",
            with_meta(json!({"name":"echo","arguments":{"text":"hi"}}), json!({}), json!({"progressToken": 1})),
        )
        .await;
    assert_eq!(msgs.len(), 2, "{msgs:?}");
    assert_eq!(msgs[0]["method"], "notifications/progress");
    let r = &msgs[1]["result"];
    assert_eq!(r["content"][0]["text"], "hi (from modern, 2026-07-28)");
    assert_eq!(r["resultType"], "complete");
    assert!(r.get("ttlMs").is_none(), "tool results aren't cacheable");

    // With a log level, logs at or above it.
    let params = with_meta(json!({"name":"echo"}), json!({}), json!({"io.modelcontextprotocol/logLevel": "info"}));
    let msgs = c.call_all(4, "tools/call", params).await;
    assert_eq!(msgs[0]["method"], "notifications/message");
    let params = with_meta(json!({"name":"echo"}), json!({}), json!({"io.modelcontextprotocol/logLevel": "error"}));
    assert_eq!(c.call_all(5, "tools/call", params).await.len(), 1);
    let params = with_meta(json!({"name":"echo"}), json!({}), json!({"io.modelcontextprotocol/logLevel": "loud"}));
    assert_eq!(c.call(6, "tools/call", params).await["error"]["code"], -32602);

    // Any JSON value as structured content.
    let res = c.call(7, "tools/call", with_meta(json!({"name":"list"}), json!({}), json!({}))).await;
    assert_eq!(res["result"]["structuredContent"], json!([1, 2]));

    // Resources: caching hints, and the new not-found code.
    let read = c.call(8, "resources/read", with_meta(json!({"uri":"mem://a"}), json!({}), json!({}))).await;
    assert_eq!(read["result"]["contents"][0]["text"], "A");
    assert_eq!(read["result"]["ttlMs"], 60000);
    let missing = c.call(9, "resources/read", with_meta(json!({"uri":"mem://zz"}), json!({}), json!({}))).await;
    assert_eq!(missing["error"]["code"], -32602);
    for (id, method) in [(10, "prompts/list"), (11, "resources/list"), (12, "resources/templates/list")] {
        let res = c.call(id, method, with_meta(json!({}), json!({}), json!({}))).await;
        assert_eq!(res["result"]["cacheScope"], "public", "{method}");
    }
    let got = c.call(13, "prompts/get", with_meta(json!({"name":"p"}), json!({}), json!({}))).await;
    assert_eq!(got["result"]["resultType"], "complete");

    // Removed methods.
    for (id, method) in [(14, "ping"), (15, "logging/setLevel"), (16, "resources/subscribe")] {
        let res = c.call(id, method, with_meta(json!({"uri":"mem://a","level":"info"}), json!({}), json!({}))).await;
        assert_eq!(res["error"]["code"], -32601, "{method}");
    }
}

#[tokio::test]
async fn bad_metadata() {
    let mut c = Client::connect(&server());
    let mut m = meta(json!({}));
    m["io.modelcontextprotocol/protocolVersion"] = "1900-01-01".into();
    let res = c.call(1, "tools/list", json!({"_meta": m})).await;
    assert_eq!(res["error"]["code"], -32022);
    assert_eq!(res["error"]["data"]["requested"], "1900-01-01");
    assert_eq!(res["error"]["data"]["supported"][0], V);

    // Capabilities are required.
    let res = c.call(2, "tools/list", json!({"_meta": {"io.modelcontextprotocol/protocolVersion": V}})).await;
    assert_eq!(res["error"]["code"], -32602);
    // So is the version, for server/discover.
    assert_eq!(c.call(3, "server/discover", json!({})).await["error"]["code"], -32602);
}

#[tokio::test]
async fn mrtr_round_trips() {
    let mut c = Client::connect(&server());
    let caps = json!({"elicitation": {}});

    // Without the capability, the tool can't ask.
    let res = c.call(1, "tools/call", with_meta(json!({"name":"confirm"}), json!({}), json!({}))).await;
    assert_eq!(res["error"]["code"], -32021);
    assert_eq!(res["error"]["data"]["requiredCapabilities"], json!({"elicitation": {}}));

    // Explicit input request, then retry with the answer.
    let res = c.call(2, "tools/call", with_meta(json!({"name":"confirm"}), caps.clone(), json!({}))).await;
    let r = &res["result"];
    assert_eq!(r["resultType"], "input_required");
    assert_eq!(r["inputRequests"]["ok"]["method"], "elicitation/create");
    assert_eq!(r["inputRequests"]["ok"]["params"]["message"], "Sure?");
    assert_eq!(r["requestState"], "s1");
    assert!(r.get("ttlMs").is_none());
    let retry = json!({"name":"confirm","inputResponses":{"ok":{"action":"accept","content":{}}},"requestState":"s1"});
    let res = c.call(3, "tools/call", with_meta(retry, caps.clone(), json!({}))).await;
    assert_eq!(res["result"]["resultType"], "complete");
    assert_eq!(res["result"]["content"][0]["text"], "confirmed with state s1");

    // ctx.elicit(): one question per round trip; earlier answers ride along
    // in requestState.
    let res = c.call(4, "tools/call", with_meta(json!({"name":"ask"}), caps.clone(), json!({}))).await;
    let r = &res["result"];
    assert_eq!(r["resultType"], "input_required");
    let (key, req) = r["inputRequests"].as_object().unwrap().iter().next().unwrap();
    assert_eq!(req["params"]["message"], "first?");
    let retry = json!({"name":"ask","inputResponses":{key.clone():{"action":"accept","content":{"v":"one"}}}});
    let res = c.call(5, "tools/call", with_meta(retry, caps.clone(), json!({}))).await;
    let r = &res["result"];
    assert_eq!(r["resultType"], "input_required");
    let (key, req) = r["inputRequests"].as_object().unwrap().iter().next().unwrap();
    assert_eq!(req["params"]["message"], "second?");
    let retry = json!({
        "name": "ask",
        "inputResponses": {key.clone(): {"action":"accept","content":{"v":"two"}}},
        "requestState": r["requestState"],
    });
    let res = c.call(6, "tools/call", with_meta(retry, caps, json!({}))).await;
    assert_eq!(res["result"]["content"][0]["text"], "one two");
}

#[tokio::test]
async fn explicit_input_on_handshake_sessions() {
    // The same handler works for handshake clients: mcptk asks the client.
    let mut c = Client::connect(&server());
    c.call(
        0,
        "initialize",
        json!({"protocolVersion":"2025-11-25","capabilities":{"elicitation":{}},"clientInfo":{"name":"old","version":"1"}}),
    )
    .await;
    c.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"})).await;
    c.send(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"confirm"}})).await;
    let req = c.recv().await;
    assert_eq!(req["method"], "elicitation/create");
    assert_eq!(req["params"]["message"], "Sure?");
    c.send(json!({"jsonrpc":"2.0","id":req["id"],"result":{"action":"accept","content":{}}})).await;
    let res = c.recv().await;
    assert_eq!(res["id"], 1);
    assert_eq!(res["result"]["content"][0]["text"], "confirmed with state s1");
    assert!(res["result"].get("resultType").is_none(), "handshake results are unchanged");

    // Non-object structured content isn't sent to them.
    let res = c.call(2, "tools/call", json!({"name":"list"})).await;
    assert!(res["result"].get("structuredContent").is_none());
    assert_eq!(res["result"]["content"][0]["text"], "[1,2]");
}

#[tokio::test]
async fn listen_over_stdio() {
    let server = server();
    let mut c = Client::connect(&server);
    let params = with_meta(
        json!({"notifications":{"toolsListChanged":true,"promptsListChanged":true,"resourceSubscriptions":["mem://a"]}}),
        json!({}),
        json!({}),
    );
    c.send(json!({"jsonrpc":"2.0","id":"sub","method":"subscriptions/listen","params":params})).await;
    let ack = c.recv().await;
    assert_eq!(ack["method"], "notifications/subscriptions/acknowledged");
    assert_eq!(ack["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"], "sub");
    assert_eq!(
        ack["params"]["notifications"],
        json!({"toolsListChanged":true,"promptsListChanged":true,"resourceSubscriptions":["mem://a"]})
    );

    server.add_tool(Tool::new("new", "New"), |_c, _a| async move { Ok::<_, ToolError>("") });
    let n = c.recv().await;
    assert_eq!(n["method"], "notifications/tools/list_changed");
    assert_eq!(n["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"], "sub");
    server.notify_resource_updated("mem://other");
    server.notify_resource_updated("mem://a");
    let n = c.recv().await;
    assert_eq!(n["params"]["uri"], "mem://a");

    // Session-wide notifications aren't sent to this uninitialized
    // connection.
    assert!(server.sessions().is_empty());

    // Cancelling ends the subscription without a response.
    c.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"sub"}})).await;
    let d = c.call(1, "server/discover", with_meta(json!({}), json!({}), json!({}))).await;
    assert_eq!(d["id"], 1);
    server.add_tool(Tool::new("newer", "Newer"), |_c, _a| async move { Ok::<_, ToolError>("") });
    let d = c.call(2, "server/discover", with_meta(json!({}), json!({}), json!({}))).await;
    assert_eq!(d["id"], 2, "no notification after cancel");
}

#[tokio::test]
async fn private_cache_scope() {
    let server = Server::builder("s", "1")
        .cache_scope(CacheScope::Private)
        .tool(Tool::new("t", "T"), |_c, _a| async move { Ok::<_, ToolError>("") })
        .build();
    let mut c = Client::connect(&server);
    let list = c.call(1, "tools/list", with_meta(json!({}), json!({}), json!({}))).await;
    assert_eq!(list["result"]["cacheScope"], "private");
    assert_eq!(list["result"]["ttlMs"], 0);
}

// --- Streamable HTTP ---

const BOTH: &str = "application/json, text/event-stream";

fn post(method: &str, name: Option<&str>, accept: &str, body: Value) -> Request<Full<Bytes>> {
    let mut req = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", accept)
        .header("mcp-protocol-version", V)
        .header("mcp-method", method);
    if let Some(name) = name {
        req = req.header("mcp-name", name);
    }
    req.body(Full::new(Bytes::from(body.to_string()))).unwrap()
}

fn rpc(id: i64, method: &str, params: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}

async fn json_of(res: ::http::Response<McpBody>) -> Value {
    serde_json::from_slice(&res.into_body().collect_bytes().await).unwrap()
}

fn sse_messages(body: &[u8]) -> Vec<Value> {
    std::str::from_utf8(body)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|d| serde_json::from_str(d).unwrap())
        .collect()
}

#[tokio::test]
async fn http_stateless_requests() {
    let server = server();
    let http = StreamableHttp::new(server.clone());

    let body = rpc(1, "server/discover", with_meta(json!({}), json!({}), json!({})));
    let res = http.handle(post("server/discover", None, "application/json", body)).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers().get("mcp-session-id").is_none());
    let d = json_of(res).await;
    assert_eq!(d["result"]["supportedVersions"][0], V);

    // SSE: progress, then the response.
    let params = with_meta(json!({"name":"echo","arguments":{"text":"x"}}), json!({}), json!({"progressToken":"p"}));
    let res = http.handle(post("tools/call", Some("echo"), BOTH, rpc(2, "tools/call", params))).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "text/event-stream");
    let body = tokio::time::timeout(Duration::from_secs(5), res.into_body().collect_bytes()).await.unwrap();
    let msgs = sse_messages(&body);
    assert_eq!(msgs[0]["method"], "notifications/progress");
    assert_eq!(msgs[1]["result"]["content"][0]["text"], "x (from modern, 2026-07-28)");
    assert!(server.sessions().is_empty(), "stateless requests leave no session");

    // JSON mode works too.
    let json_http = StreamableHttp::new(server.clone()).json_response(true);
    let body = rpc(3, "tools/list", with_meta(json!({}), json!({}), json!({})));
    let res = json_http.handle(post("tools/list", None, BOTH, body)).await;
    assert_eq!(json_of(res).await["result"]["ttlMs"], 60000);

    // Errors that have an HTTP status.
    let body = rpc(4, "nope/nothing", with_meta(json!({}), json!({}), json!({})));
    let res = http.handle(post("nope/nothing", None, BOTH, body)).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_of(res).await["error"]["code"], -32601);

    let mut m = meta(json!({}));
    m["io.modelcontextprotocol/protocolVersion"] = "1900-01-01".into();
    let mut req = post("tools/list", None, BOTH, rpc(5, "tools/list", json!({"_meta": m})));
    req.headers_mut().insert("mcp-protocol-version", "1900-01-01".parse().unwrap());
    let res = http.handle(req).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_of(res).await["error"]["code"], -32022);

    let res = http
        .handle(post(
            "tools/call",
            Some("confirm"),
            BOTH,
            rpc(6, "tools/call", with_meta(json!({"name":"confirm"}), json!({}), json!({}))),
        ))
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_of(res).await["error"]["code"], -32021);

    let res = http.handle(post("tools/list", None, BOTH, rpc(7, "tools/list", json!({})))).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST, "no session id and no _meta");

    // Batches aren't allowed.
    let body = json!([rpc(8, "tools/list", with_meta(json!({}), json!({}), json!({})))]);
    assert_eq!(http.handle(post("tools/list", None, BOTH, body)).await.status(), StatusCode::BAD_REQUEST);

    // Notifications are accepted.
    let n = json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}});
    assert_eq!(http.handle(post("notifications/cancelled", None, BOTH, n)).await.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn http_header_validation() {
    let http = StreamableHttp::new(server()).json_response(true);
    let call =
        |args: Value| rpc(1, "tools/call", with_meta(json!({"name":"region","arguments":args}), json!({}), json!({})));
    let code = |res: ::http::Response<McpBody>| async move {
        let status = res.status();
        (status, json_of(res).await)
    };

    // Missing / mismatched Mcp-Method and Mcp-Name.
    let mut req = post("tools/call", Some("region"), BOTH, call(json!({})));
    req.headers_mut().remove("mcp-method");
    let (status, body) = code(http.handle(req).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);
    assert_eq!(body["id"], 1);
    let (status, _) = code(http.handle(post("tools/list", Some("region"), BOTH, call(json!({})))).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = code(http.handle(post("tools/call", Some("other"), BOTH, call(json!({})))).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = code(http.handle(post("tools/call", None, BOTH, call(json!({})))).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Base64-encoded names are decoded ("region" = cmVnaW9u).
    let (status, body) =
        code(http.handle(post("tools/call", Some("=?base64?cmVnaW9u?="), BOTH, call(json!({})))).await).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The protocol version header must match _meta.
    let mut req = post("tools/call", Some("region"), BOTH, call(json!({})));
    req.headers_mut().insert("mcp-protocol-version", "2025-11-25".parse().unwrap());
    assert_eq!(http.handle(req).await.status(), StatusCode::BAD_REQUEST);

    // x-mcp-header parameters.
    let (status, _) =
        code(http.handle(post("tools/call", Some("region"), BOTH, call(json!({"region":"eu"})))).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "missing Mcp-Param-Region");
    let mut req = post("tools/call", Some("region"), BOTH, call(json!({"region":"eu","n":42,"q":"x"})));
    req.headers_mut().insert("mcp-param-region", "eu".parse().unwrap());
    req.headers_mut().insert("mcp-param-n", "42.0".parse().unwrap());
    let (status, body) = code(http.handle(req).await).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["content"][0]["text"], "ok");
    let mut req = post("tools/call", Some("region"), BOTH, call(json!({"region":"eu"})));
    req.headers_mut().insert("mcp-param-region", "us".parse().unwrap());
    assert_eq!(http.handle(req).await.status(), StatusCode::BAD_REQUEST);
    let mut req = post("tools/call", Some("region"), BOTH, call(json!({})));
    req.headers_mut().insert("mcp-param-region", "us".parse().unwrap());
    assert_eq!(http.handle(req).await.status(), StatusCode::BAD_REQUEST, "header without a value");
}

#[tokio::test]
async fn http_listen_receives_list_changes() {
    let server = server();
    let http = StreamableHttp::new(server.clone()).json_response(true);
    let params = with_meta(json!({"notifications":{"toolsListChanged":true}}), json!({}), json!({}));
    let res = http.handle(post("subscriptions/listen", None, BOTH, rpc(9, "subscriptions/listen", params))).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "text/event-stream", "listen always streams");
    let mut body = res.into_body();
    let mut next = async || -> Value {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), body.frame()).await.unwrap().unwrap().unwrap();
            if let Some(msg) = sse_messages(&frame.into_data().unwrap()).into_iter().next() {
                return msg;
            }
        }
    };
    let ack = next().await;
    assert_eq!(ack["method"], "notifications/subscriptions/acknowledged");
    assert_eq!(ack["params"]["notifications"], json!({"toolsListChanged":true}));

    server.add_prompt(Prompt::new("ignored", "Not subscribed"), |_c, _a| async move { Ok("".to_string()) });
    server.add_tool(Tool::new("late", "Late"), |_c, _a| async move { Ok::<_, ToolError>("") });
    let n = next().await;
    assert_eq!(n["method"], "notifications/tools/list_changed");
    assert_eq!(n["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"], 9);
}

#[tokio::test]
async fn http_both_eras_on_one_endpoint() {
    let http = StreamableHttp::new(server()).json_response(true);
    let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}
    }});
    let req = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", BOTH)
        .body(Full::new(Bytes::from(init.to_string())))
        .unwrap();
    let res = http.handle(req).await;
    let sid = res.headers()["mcp-session-id"].to_str().unwrap().to_string();
    assert_eq!(json_of(res).await["result"]["protocolVersion"], "2025-06-18");

    // A stateless request ignores the session id.
    let mut req = post("tools/list", None, BOTH, rpc(2, "tools/list", with_meta(json!({}), json!({}), json!({}))));
    req.headers_mut().insert("mcp-session-id", sid.parse().unwrap());
    let res = http.handle(req).await;
    assert!(res.headers().get("mcp-session-id").is_none());
    assert_eq!(json_of(res).await["result"]["resultType"], "complete");

    // The legacy session goes on as before.
    let req = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", BOTH)
        .header("mcp-session-id", &sid)
        .body(Full::new(Bytes::from(rpc(3, "tools/list", json!({})).to_string())))
        .unwrap();
    let list = json_of(http.handle(req).await).await;
    assert!(list["result"].get("resultType").is_none());
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 5);
}
