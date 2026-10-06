# mcptk — MCP Toolkit

Build [Model Context Protocol](https://modelcontextprotocol.io) servers in Rust,
served over stdio (or any byte stream) and Streamable HTTP, with first-class
support for newer features such as [Claude Code channels](#channels).

mcptk implements the protocol itself, on tokio and serde. It does not wrap
another MCP SDK.

- **Protocol**: revision [2026-07-28](#protocol-2026-07-28) (stateless,
  per-request metadata), and the handshake-based revisions 2025-11-25,
  2025-06-18, 2025-03-26 and 2024-11-05, negotiated per session. One server
  serves both kinds of clients, on any transport.
- **Server features**: tools (raw or typed with JSON Schema derived by
  `schemars`, structured output), resources, resource templates and
  subscriptions, prompts, completions, logging, progress, cancellation,
  per-session tool filtering, and tools/resources/prompts you can add or
  remove at runtime (sessions get `list_changed`).
- **Extensions**: [tasks](#tasks) (`io.modelcontextprotocol/tasks`):
  long-running tool calls answered with a task handle the client polls.
- **Requests to the client**: sampling, elicitation, roots, ping.
- **Transports**: stdio, any `AsyncRead`/`AsyncWrite` pair (unix sockets,
  pipes...), and Streamable HTTP (sessions, SSE or JSON responses, `GET`
  streams, origin checks, idle expiry, OAuth authorization). The HTTP handler
  can also be mounted in your own hyper/axum server. WebSocket (`ws` feature)
  for Claude Code's `"type": "ws"` servers, optionally on the same port and
  path as HTTP.
- **Channels**: push events into a Claude Code session, and relay its
  permission prompts.
- **MCP Apps**: serve interactive HTML views for tool results
  (`io.modelcontextprotocol/ui`).

Requires Rust 1.89+ (edition 2024).

## Quick start

```toml
[dependencies]
mcptk = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
schemars = "1"
```

```rust
use mcptk::{Server, Tool, ToolError};

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Greet {
    /// Who to greet.
    name: String,
}

#[tokio::main]
async fn main() -> mcptk::Result<()> {
    Server::builder("greeter", "0.1.0")
        .instructions("Greets people.")
        .typed_tool(Tool::new("greet", "Greet someone").read_only(), |_ctx, args: Greet| async move {
            Ok::<_, ToolError>(format!("Hello, {}!", args.name))
        })
        .build()
        .serve_stdio()
        .await
}
```

Register it with Claude Code using `claude mcp add greeter -- /path/to/greeter`.

### Tool results and errors

A tool handler returns `Result<R, ToolError>`, where `R` is anything
`IntoToolResult`: `String`, `&'static str`, `Content`, `Vec<Content>`,
`Json<T>` (text plus `structuredContent`), `()` or a full `CallToolResult`.

A `ToolError` becomes an `isError: true` result, so the model sees what went
wrong and can retry. Any `std::error::Error` converts into one, so `?` works.
Use `ToolError::protocol(...)` when you need a JSON-RPC error instead.

### The request context

Every handler receives a `RequestContext`. Use it to:

- report progress: `ctx.progress(done, Some(total), None)`
- send logs: `ctx.log(LoggingLevel::Info, Some("db"), "connected")`
- ask the client: `ctx.create_message(...)` (sampling, optionally with
  `tools`), `ctx.elicit(ElicitParams::form(...))` or
  `ctx.elicit(ElicitParams::url(...))` (then
  `session.notify_elicitation_complete(id)`), `ctx.list_roots()`
- check for cancellation: `ctx.is_cancelled()`. A handler's future is dropped
  when the client cancels the request.
- reach the `Session`: `ctx.session()`, which also holds per-session state via
  `set_data` / `data`

### Streamable HTTP

```rust
use mcptk::http::StreamableHttp;

StreamableHttp::new(server)
    .allow_origin("https://app.example") // localhost origins are always allowed
    .serve("127.0.0.1:8080")             // endpoint: /mcp
    .await?;
```

To mount it in your own server instead, call `StreamableHttp::handle(request)`
from any hyper-compatible stack and use `.path(None)` if your router already
matched the route.

### WebSocket

With the `ws` feature, each WebSocket connection is one MCP session, with one
JSON-RPC message per text frame and the `mcp` subprotocol, as Claude Code's
`"type": "ws"` servers and the TypeScript SDK's `WebSocketClientTransport`
expect. The upgrade runs on hyper with the same origin checks as HTTP.

```rust
use mcptk::ws::WebSocketServer;

WebSocketServer::new(server.clone())
    .max_message_size(4 << 20)              // larger messages close the socket (1009)
    .with_http(StreamableHttp::new(server)) // optional: HTTP on the same port and path
    .serve("127.0.0.1:8080")                // ws://127.0.0.1:8080/mcp
    .await?;
```

```json
{ "mcpServers": { "events": { "type": "ws", "url": "ws://127.0.0.1:8080/mcp",
  "headers": { "Authorization": "Bearer TOKEN" } } } }
```

Mount it in your own hyper/axum server with `WebSocketServer::handle(request)`
(the connection must be served `with_upgrades()`), or serve an already
upgraded `WebSocketStream` with `Server::connect_ws`.

`WebSocketServer::auth(ProtectedResource)` requires an OAuth access token on
the upgrade request, like [Streamable HTTP](#authorization-oauth) does:
handlers get the identity from `ctx.auth()`, requests lacking a tool's scopes
get a JSON-RPC error, and the socket closes when the token expires.

### Authorization (OAuth)

A Streamable HTTP server can require OAuth access tokens, following the MCP
authorization spec: the server is an OAuth 2.1 *resource server*, and your
identity provider is the authorization server that issues tokens.

```rust
use mcptk::auth::{AuthError, AuthInfo, ProtectedResource, validator_fn};

let validator = validator_fn(|token: String| async move {
    // Verify the JWT (or introspect the token) with the library of your
    // choice. Check its audience is this server's resource URL!
    let claims = verify(&token).map_err(|e| AuthError::invalid_token(e.to_string()))?;
    Ok(AuthInfo::new(claims.sub).scopes(claims.scopes))
});

StreamableHttp::new(server)
    .auth(
        ProtectedResource::new("https://mcp.example.com/mcp", validator)
            .authorization_server("https://auth.example.com")
            .scopes_supported(["notes:read"])
            .require_scopes(["notes:read"])        // every request
            .tool_scopes("add_note", ["notes:write"]), // step-up for one tool
    )
    .serve("127.0.0.1:8080")
    .await?;
```

With that, mcptk:

- serves the Protected Resource Metadata (RFC 9728) at
  `/.well-known/oauth-protected-resource/mcp` and
  `/.well-known/oauth-protected-resource`, which is how clients such as
  Claude Code find where to log in;
- answers requests with no valid `Authorization: Bearer` token with `401`
  and `WWW-Authenticate: Bearer resource_metadata="...", scope="..."`;
- answers tokens lacking scopes with `403` and
  `error="insufficient_scope"`, naming the scopes needed, so the client can
  re-authorize with more (tool scopes are checked before the tool runs);
- gives handlers the caller's identity, per request: `ctx.auth()` returns
  the `AuthInfo` (subject, scopes, client id, expiry, extra claims), and
  `ctx.require_scope("x")?` checks a scope inside a handler;
- binds each session to the subject that created it: another user's token
  can't use it.

mcptk ships no JWT or crypto code: implement `TokenValidator` (or use
`validator_fn`). The validator must reject tokens not issued for this server
(audience) and expired ones. Never pass the client's token on to other APIs.
`StaticTokens` maps fixed tokens to identities for tests and development.

When mounting `handle` in your own router, route the paths from
`ProtectedResource::metadata_paths()` to it too (it answers them whatever
`.path` is), or serve `ProtectedResource::metadata_response()` yourself.

## Tasks

mcptk implements the server side of the official Tasks extension
(`io.modelcontextprotocol/tasks`, protocol revision 2026-07-28, SEP-2663).
A slow tool call can answer right away with a task handle
(`resultType: "task"`). The client then polls `tasks/get` until the task is
`completed`, `failed` or `cancelled`, answers its input requests with
`tasks/update`, and can stop it with `tasks/cancel`.

```rust
use mcptk::tasks::{TaskConfig, TaskContext};

Server::builder("jobs", "1.0")
    .tasks(TaskConfig::new().ttl(Some(Duration::from_secs(600))))
    .task_tool(Tool::new("crunch", "Crunch numbers"), |ctx: TaskContext, _args| async move {
        ctx.progress(0.0, None, Some("warming up")).await?; // becomes the task's statusMessage
        let answer = ctx.elicit(params).await?;             // becomes one of the task's inputRequests
        Ok::<_, ToolError>("crunched")
    })
    .build()
```

- **The server decides which calls become tasks.** A call becomes a task when
  the tool was registered with `task_tool` / `typed_task_tool` and the
  request declares the extension in its own client capabilities
  (`_meta["io.modelcontextprotocol/clientCapabilities"].extensions`).
  Otherwise the handler runs inline like any tool. To refuse inline calls
  instead (error `-32021`), use `.task_mode(name, TaskMode::Required)`.
- **`TaskContext`** works in both modes. When the call is a task,
  `elicit` / `create_message` / `list_roots` become `inputRequests` on the
  task, and the handler resumes when `tasks/update` answers them. `progress`
  and `set_status_message` update `statusMessage`. Log notifications are
  dropped, since the extension doesn't support them on tasks. When the call
  runs inline, these are ordinary client requests and notifications.
- **Results.** A `ToolError::protocol` error ends the task as `failed`. Any
  other result, including `isError` results, ends it as `completed`.
  `tasks/cancel` drops the handler's future and the task ends as
  `cancelled`.
- **Storage.** Tasks live in a `TaskStore` owned by the `Server`, so a task
  created over one HTTP request or session can be polled from another.
  `InMemoryTaskStore` is the default. Implement the trait to store tasks
  elsewhere. Running handlers stay in the process that started them.
- **Expiry.** Each task expires `ttl` after it is created (one hour by
  default). A task still running at that point is cancelled. After expiry,
  the task is forgotten and `tasks/get` returns an error.
- **Access.** Task ids are 128-bit random values. With [OAuth](#authorization-oauth),
  a task belongs to the subject whose request created it, and other subjects
  get "task not found". Without OAuth, the task id is the only credential a
  client needs. The handler keeps the creating request's `ctx.auth()` while
  it runs in the background.

The server doesn't support the experimental tasks from 2025-11-25
(`capabilities.tasks`, the `task` parameter, `tasks/result`, `tasks/list`).
They aren't wire-compatible with the extension. Task status notifications
(`notifications/tasks`) aren't sent either, so clients have to poll.

## Protocol 2026-07-28

Clients on revision 2026-07-28 skip the `initialize` handshake: every request
carries its protocol version, client capabilities and client info in
`_meta`. mcptk answers them statelessly next to handshake sessions, on stdio,
WebSocket and HTTP alike, and implements `server/discover`,
`subscriptions/listen`, `resultType`, `ttlMs`/`cacheScope`, per-request log
levels, the `Mcp-Method`/`Mcp-Name`/`Mcp-Param-*` HTTP headers, and the new
error codes. Fields that only exist in 2026-07-28 are only sent to those
clients, so handshake clients see exactly what they did before.

Handlers mostly don't need to care which revision a client speaks:

- `ctx.client_info()` and `ctx.client_capabilities()` come from the request
  or from the session; `ctx.is_stateless()` tells which.
- `ctx.elicit()`, `ctx.create_message()` and `ctx.list_roots()` send a request
  to handshake clients. For 2026-07-28 clients they use multi round-trip
  requests: the call fails with `Error::InputRequired`; let it propagate with
  `?`, the client retries with the answer, the handler runs again and the call
  returns it. Make these calls in the same order on every run.
- To ask several things at once, or to keep your own state, return an
  `InputRequired` and read the answers with `ctx.input_response(key)` and
  `ctx.request_state()`. For handshake clients mcptk sends the requests itself
  and runs the handler again, so the same code serves both.

```rust
.tool(Tool::new("delete_all", "Delete everything"), |ctx, _args| async move {
    match ctx.input_response::<ElicitResult>("confirm")? {
        Some(r) if r.action == ElicitAction::Accept => Ok("deleted".to_string()),
        Some(_) => Ok("kept".to_string()),
        None => Err(InputRequired::new()
            .elicit("confirm", ElicitParams::form("Really?", json!({"type": "object"})))
            .into()),
    }
})
```

Results carry `ttlMs` and `cacheScope` hints: set them with
`ServerBuilder::cache_ttl` (default 0) and `cache_scope` (default public;
`tools/list` is private with a `tool_filter`). There are no sessions in this
revision: logs, channel events and list changes only go to initialized
sessions, while 2026-07-28 clients get list changes and resource updates on
`subscriptions/listen` streams. Over HTTP each of their requests gets a
fresh `Session`, so per-session data doesn't carry over.

**Channels need a handshake revision.** Claude Code only delivers channel
messages from servers it talks to over the handshake revisions, so keep
channel servers on those (mcptk does, since Claude Code initializes them).

## Channels

A channel is an MCP server, spawned by Claude Code over stdio, that pushes
events into the session: chat messages, CI failures, alerts. Claude sees each
event as `<channel source="your-server" key="value">content</channel>`.

```rust
use mcptk::{ChannelEvent, Server};

let server = Server::builder("alerts", "0.1.0")
    .channel()
    .instructions("Alerts arrive as <channel source=\"alerts\" severity=\"...\">. Investigate them.")
    .build();

let conn = server.connect_stdio();
let session = conn.session().clone();
tokio::spawn(async move {
    // ...when something happens:
    let _ = session.channel_event(&ChannelEvent::new("disk full on db1").meta("severity", "high"));
});
conn.wait().await?;
```

Meta keys must be identifiers (letters, digits, underscores). Claude Code drops
other keys silently, so `ChannelEvent::meta` drops them too and logs a warning;
`try_meta` returns an error instead.

**Permission relay.** A two-way channel whose senders are authenticated can
opt in to receiving Claude Code's tool approval prompts, and answer them:

```rust
.channel_permission(|session, req| async move {
    send_to_phone(req.prompt_text()); // "...Reply "yes abcde" or "no abcde""
})
// later, in your inbound handler:
if let Some(verdict) = mcptk::channel::parse_permission_reply(&text) {
    session.permission_verdict(&verdict)?;
}
```

Anyone who can send a verdict can approve tool use in the session, so gate
inbound messages on the sender's identity first.

While channels are in research preview, test custom channels with
`claude --dangerously-load-development-channels server:<name>`. See
[`examples/webhook_channel.rs`](examples/webhook_channel.rs) for a complete
two-way channel with a reply tool and permission relay.

## MCP Apps

[MCP Apps](https://github.com/modelcontextprotocol/ext-apps) (the
`io.modelcontextprotocol/ui` extension, SEP-1865) let a tool's result render
as an interactive HTML view in hosts that support it. The page is a `ui://`
resource with the MIME type `text/html;profile=mcp-app`; a tool links to it
with `_meta.ui.resourceUri`.

```rust
use mcptk::apps::{AppsBuilderExt, ToolUiExt, UiResource, UiSupport};

let view = UiResource::new("ui://weather/view", "weather_view", include_str!("view.html"))
    .connect_domain("https://api.weather.example") // CSP: fetch/XHR/WebSocket
    .resource_domain("https://cdn.jsdelivr.net")   // CSP: scripts, styles, images, fonts
    .prefers_border(true);

Server::builder("weather", "1.0")
    .ui_resource(view) // serves the page and declares the extension
    .tool(Tool::new("forecast", "Show the forecast").ui_resource("ui://weather/view"), |ctx, args| async move {
        // Text for the model and text-only hosts; structuredContent for the view.
        Ok::<_, ToolError>(CallToolResult::text("Sunny, 22°C").structured(json!({"temp": 22})))
    })
    .tool(Tool::new("refresh", "Refresh the view").ui_resource("ui://weather/view").app_only(), refresh)
    .tool_filter(mcptk::apps::hide_app_only_tools_without_ui)
```

- `app_only()` sets `_meta.ui.visibility: ["app"]`: the view can call the tool
  (through the host) but the model doesn't see it. Hosts without MCP Apps
  would list it anyway; `hide_app_only_tools_without_ui` hides it there.
- `ctx.supports_ui()` tells whether the client declared the extension with
  `text/html;profile=mcp-app`, from the request's per-call client
  capabilities (2026-07-28) or the session's. Always return useful text too.
- The view talks to the host over `postMessage` (`ui/initialize`,
  `ui/notifications/tool-result`, `tools/call`...). The server only sees
  ordinary `resources/read` and `tools/call` requests.
- `openai_compat()` on tools and `UiResource` also emits the OpenAI Apps SDK
  keys (`openai/outputTemplate`, `openai/widgetCSP`...). ChatGPT now reads
  the standard keys, so this is only for older hosts.
- Claude Code renders no views: it hides UI resources from `@` mentions and
  its resource list, but reading one by URI works.

See [`examples/ui_app.rs`](examples/ui_app.rs) for a complete app.

## Examples

| Example | What it shows |
| --- | --- |
| [`echo`](examples/echo.rs) | stdio server: typed tools, progress, structured output, resources, templates, prompts |
| [`http_server`](examples/http_server.rs) | Streamable HTTP server with logging and resource subscriptions |
| [`webhook_channel`](examples/webhook_channel.rs) | Two-way Claude Code channel with permission relay |
| [`ui_app`](examples/ui_app.rs) | MCP App: a tool whose result renders as an interactive HTML view |
| [`tasks`](examples/tasks.rs) | Tools that run as tasks (polling, input requests) for clients with the tasks extension |

Run one with `cargo run --example echo`.

## Cargo features

| Feature | Default | Enables |
| --- | --- | --- |
| `stdio` | yes | `serve_stdio` / `connect_stdio` (`connect_io` is always available) |
| `http` | yes | the `http` module: Streamable HTTP server transport |
| `ws` | no | the `ws` module: WebSocket server transport (`tokio-tungstenite`) |
| `schemars` | yes | `typed_tool` and `Tool::output_schema_for`, with schemas derived from types |

## Not yet supported

- Resuming HTTP streams with `Last-Event-ID`
- Pagination cursors (lists are returned whole)
- Task status notifications on `subscriptions/listen` (tasks extension)

## License

MIT
