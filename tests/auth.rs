//! Authorization (OAuth protected resource) tests over Streamable HTTP.
#![cfg(feature = "http")]

use ::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::Full;
use mcptk::auth::{AuthError, AuthInfo, ProtectedResource, StaticTokens, validator_fn};
use mcptk::http::{McpBody, StreamableHttp};
use mcptk::*;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};

const RESOURCE: &str = "https://mcp.example.com/mcp";
const METADATA: &str = "https://mcp.example.com/.well-known/oauth-protected-resource/mcp";

fn server() -> Server {
    Server::builder("auth-test", "1")
        .tool(Tool::new("whoami", "Who am I"), |ctx, _args| async move {
            let auth = ctx.auth().ok_or_else(|| ToolError::msg("anonymous"))?;
            Ok::<_, ToolError>(format!("{} {}", auth.subject, auth.client_id.as_deref().unwrap_or("-")))
        })
        .tool(Tool::new("write", "Write something"), |_ctx, _args| async move { Ok::<_, ToolError>("written") })
        .tool(Tool::new("admin", "Admin only"), |ctx, _args| async move {
            ctx.require_scope("admin")?;
            Ok::<_, ToolError>("ok")
        })
        .build()
}

fn resource() -> ProtectedResource {
    let tokens = StaticTokens::new()
        .token("alice-token", AuthInfo::new("alice").scopes(["mcp:read"]).client_id("cli"))
        .token("alice-rw", AuthInfo::new("alice").scopes(["mcp:read", "mcp:write"]))
        .token("bob-token", AuthInfo::new("bob").scopes(["mcp:read", "mcp:write"]))
        .token("no-scope", AuthInfo::new("carol"))
        .token(
            "expired",
            AuthInfo::new("dave").scopes(["mcp:read"]).expires_at(SystemTime::now() - Duration::from_secs(1)),
        );
    ProtectedResource::new(RESOURCE, tokens)
        .authorization_server("https://auth.example.com")
        .scopes_supported(["mcp:read"])
        .require_scopes(["mcp:read"])
        .tool_scopes("write", ["mcp:write"])
        .resource_name("Auth test")
}

fn http() -> StreamableHttp {
    StreamableHttp::new(server()).json_response(true).auth(resource())
}

fn post(token: Option<&str>, session: Option<&str>, body: Value) -> Request<Full<Bytes>> {
    let mut req = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    if let Some(s) = session {
        req = req.header("mcp-session-id", s);
    }
    req.body(Full::new(Bytes::from(body.to_string()))).unwrap()
}

fn get(path: &str) -> Request<Full<Bytes>> {
    Request::get(path).body(Full::new(Bytes::new())).unwrap()
}

fn init_body() -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}
    }})
}

fn call(name: &str) -> Value {
    json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":name,"arguments":{}}})
}

async fn json_of(res: ::http::Response<McpBody>) -> Value {
    serde_json::from_slice(&res.into_body().collect_bytes().await).unwrap()
}

fn challenge(res: &::http::Response<McpBody>) -> &str {
    res.headers()["www-authenticate"].to_str().unwrap()
}

async fn initialize(http: &StreamableHttp, token: &str) -> String {
    let res = http.handle(post(Some(token), None, init_body())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let id = res.headers()["mcp-session-id"].to_str().unwrap().to_string();
    let notif = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
    assert_eq!(http.handle(post(Some(token), Some(&id), notif)).await.status(), StatusCode::ACCEPTED);
    id
}

#[tokio::test]
async fn serves_metadata_on_both_well_known_paths() {
    let http = http();
    for path in ["/.well-known/oauth-protected-resource/mcp", "/.well-known/oauth-protected-resource"] {
        let res = http.handle(get(path)).await;
        assert_eq!(res.status(), StatusCode::OK, "{path}");
        assert_eq!(res.headers()["content-type"], "application/json");
        assert_eq!(res.headers()["access-control-allow-origin"], "*");
        assert_eq!(
            json_of(res).await,
            json!({
                "resource": RESOURCE,
                "authorization_servers": ["https://auth.example.com"],
                "scopes_supported": ["mcp:read"],
                "bearer_methods_supported": ["header"],
                "resource_name": "Auth test",
            })
        );
    }
    // Also when the handler answers on any path.
    let http = http.path(None);
    assert_eq!(http.handle(get("/.well-known/oauth-protected-resource/mcp")).await.status(), StatusCode::OK);
    let pr = http.protected_resource().unwrap();
    assert_eq!(pr.metadata_location(), METADATA);
    assert_eq!(pr.metadata_response().status(), StatusCode::OK);
    // Unrelated well-known paths still get the MCP endpoint's answer.
    assert_eq!(
        StreamableHttp::new(server()).auth(resource()).handle(get("/.well-known/other")).await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn missing_or_invalid_token_gets_401() {
    let http = http();
    let res = http.handle(post(None, None, init_body())).await;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(challenge(&res), format!("Bearer resource_metadata=\"{METADATA}\", scope=\"mcp:read\""));

    let res = http.handle(post(Some("nope"), None, init_body())).await;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        challenge(&res),
        format!(
            "Bearer error=\"invalid_token\", error_description=\"unknown token\", \
             resource_metadata=\"{METADATA}\", scope=\"mcp:read\""
        )
    );

    let res = http.handle(post(Some("expired"), None, init_body())).await;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert!(challenge(&res).contains("token expired"));

    // Another scheme.
    let mut req = post(None, None, init_body());
    req.headers_mut().insert("authorization", "Basic YTpi".parse().unwrap());
    assert_eq!(http.handle(req).await.status(), StatusCode::UNAUTHORIZED);

    // GET and DELETE need a token too.
    let req = Request::delete("/mcp").header("mcp-session-id", "x").body(Full::new(Bytes::new())).unwrap();
    assert_eq!(http.handle(req).await.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn valid_token_reaches_handlers() {
    let http = http();
    let id = initialize(&http, "alice-token").await;
    let res = http.handle(post(Some("alice-token"), Some(&id), call("whoami"))).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(json_of(res).await["result"]["content"][0]["text"], "alice cli");

    // In SSE mode too, and with the identity of this request's token.
    let http = StreamableHttp::new(server()).auth(resource());
    let id = initialize(&http, "alice-token").await;
    let res = http.handle(post(Some("alice-rw"), Some(&id), call("whoami"))).await;
    assert_eq!(res.headers()["content-type"], "text/event-stream");
    let body = String::from_utf8(res.into_body().collect_bytes().await.to_vec()).unwrap();
    assert!(body.contains("alice -"), "{body}");
}

#[tokio::test]
async fn insufficient_scope_gets_403() {
    let http = http();
    // Below the scopes every request needs.
    let res = http.handle(post(Some("no-scope"), None, init_body())).await;
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(challenge(&res).starts_with("Bearer error=\"insufficient_scope\""));
    assert!(challenge(&res).contains("scope=\"mcp:read\""));

    // A tool needing more.
    let id = initialize(&http, "alice-token").await;
    let res = http.handle(post(Some("alice-token"), Some(&id), call("write"))).await;
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let header = challenge(&res);
    assert!(header.starts_with("Bearer error=\"insufficient_scope\""), "{header}");
    assert!(header.contains(&format!("resource_metadata=\"{METADATA}\"")), "{header}");
    assert!(header.contains("scope=\"mcp:read mcp:write\""), "{header}");

    // Step-up: same user, more scopes, same session.
    let res = http.handle(post(Some("alice-rw"), Some(&id), call("write"))).await;
    assert_eq!(json_of(res).await["result"]["content"][0]["text"], "written");

    // In-handler checks give a tool error.
    let res = http.handle(post(Some("alice-rw"), Some(&id), call("admin"))).await;
    let res = json_of(res).await;
    assert_eq!(res["result"]["isError"], true);
    assert!(res["result"]["content"][0]["text"].as_str().unwrap().contains("admin"));
}

#[tokio::test]
async fn session_is_bound_to_its_subject() {
    let http = http();
    let id = initialize(&http, "alice-token").await;
    let res = http.handle(post(Some("bob-token"), Some(&id), call("whoami"))).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let get = Request::get("/mcp")
        .header("accept", "text/event-stream")
        .header("authorization", "Bearer bob-token")
        .header("mcp-session-id", &id)
        .body(Full::new(Bytes::new()))
        .unwrap();
    assert_eq!(http.handle(get).await.status(), StatusCode::NOT_FOUND);
    let del = Request::delete("/mcp")
        .header("authorization", "Bearer bob-token")
        .header("mcp-session-id", &id)
        .body(Full::new(Bytes::new()))
        .unwrap();
    assert_eq!(http.handle(del).await.status(), StatusCode::NOT_FOUND);
    // The owner still can.
    let res = http.handle(post(Some("alice-token"), Some(&id), call("whoami"))).await;
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn closure_validator_and_unavailable() {
    let validator = validator_fn(|token: String| async move {
        match token.as_str() {
            "down" => Err(AuthError::Unavailable("introspection timed out".into())),
            _ => Ok(AuthInfo::new(token)),
        }
    });
    let http = StreamableHttp::new(server())
        .json_response(true)
        .auth(ProtectedResource::new(RESOURCE, validator).authorization_server("https://auth.example.com"));
    assert_eq!(http.handle(post(Some("down"), None, init_body())).await.status(), StatusCode::SERVICE_UNAVAILABLE);
    let id = initialize(&http, "zed").await;
    let res = http.handle(post(Some("zed"), Some(&id), call("whoami"))).await;
    assert_eq!(json_of(res).await["result"]["content"][0]["text"], "zed -");
    // No scopes configured: no scope in the challenge.
    let res = http.handle(post(None, None, init_body())).await;
    assert_eq!(challenge(&res), format!("Bearer resource_metadata=\"{METADATA}\""));
}
