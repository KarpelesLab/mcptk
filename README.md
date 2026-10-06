# mcptk — MCP Toolkit

Build [Model Context Protocol](https://modelcontextprotocol.io) servers in Rust,
served over stdio (or any byte stream) and Streamable HTTP, with first-class
support for newer features such as [Claude Code channels](#channels).

mcptk implements the protocol itself, on tokio and serde. It does not wrap
another MCP SDK.

- **Protocol**: revisions 2025-11-25, 2025-06-18, 2025-03-26 and 2024-11-05,
  negotiated per session.
- **Server features**: tools (raw or typed with JSON Schema derived by
  `schemars`, structured output), resources, resource templates and
  subscriptions, prompts, completions, logging, progress, cancellation,
  per-session tool filtering, and tools/resources/prompts you can add or
  remove at runtime (sessions get `list_changed`).
- **Requests to the client**: sampling, elicitation, roots, ping.
- **Transports**: stdio, any `AsyncRead`/`AsyncWrite` pair (unix sockets,
  pipes...), and Streamable HTTP (sessions, SSE or JSON responses, `GET`
  streams, origin checks, idle expiry). The HTTP handler can also be mounted
  in your own hyper/axum server. WebSocket (`ws` feature) for Claude Code's
  `"type": "ws"` servers, optionally on the same port and path as HTTP.
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
upgraded `WebSocketStream` with `Server::connect_ws`. Authentication is up to
you: check the request's headers before handing it over.

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

## License

MIT
