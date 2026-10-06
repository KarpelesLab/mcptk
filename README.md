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
  in your own hyper/axum server.
- **Channels**: push events into a Claude Code session, and relay its
  permission prompts.

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
- ask the client: `ctx.create_message(...)` (sampling),
  `ctx.elicit(...)`, `ctx.list_roots()`
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

## Examples

| Example | What it shows |
| --- | --- |
| [`echo`](examples/echo.rs) | stdio server: typed tools, progress, structured output, resources, templates, prompts |
| [`http_server`](examples/http_server.rs) | Streamable HTTP server with logging and resource subscriptions |
| [`webhook_channel`](examples/webhook_channel.rs) | Two-way Claude Code channel with permission relay |

Run one with `cargo run --example echo`.

## Cargo features

| Feature | Default | Enables |
| --- | --- | --- |
| `stdio` | yes | `serve_stdio` / `connect_stdio` (`connect_io` is always available) |
| `http` | yes | the `http` module: Streamable HTTP server transport |
| `schemars` | yes | `typed_tool` and `Tool::output_schema_for`, with schemas derived from types |

## Not yet supported

- Resuming HTTP streams with `Last-Event-ID`
- Pagination cursors (lists are returned whole)

## License

MIT
