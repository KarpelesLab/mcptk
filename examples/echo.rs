//! A stdio MCP server with a few tools, a resource, a resource template and
//! a prompt.
//!
//! Try it with Claude Code:
//!     claude mcp add echo -- cargo run --example echo

use mcptk::types::ResourceContents;
use mcptk::{Json, Prompt, Resource, ResourceTemplate, Server, Tool, ToolError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, schemars::JsonSchema)]
struct EchoArgs {
    /// The text to send back.
    text: String,
    /// How many times to repeat it.
    #[serde(default)]
    times: Option<u32>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CountArgs {
    /// Count up to this number, one step per 100ms.
    to: u32,
}

#[derive(Serialize, schemars::JsonSchema)]
struct Stats {
    chars: usize,
    words: usize,
    lines: usize,
}

#[tokio::main]
async fn main() -> mcptk::Result<()> {
    // Stdout carries the protocol: logs go to stderr.
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_env_filter("info").init();

    Server::builder("echo", env!("CARGO_PKG_VERSION"))
        .title("Echo")
        .instructions("A demo server: echo text, count with progress, compute text stats.")
        .typed_tool(Tool::new("echo", "Echo text back").read_only(), |_ctx, args: EchoArgs| async move {
            Ok::<_, ToolError>(args.text.repeat(args.times.unwrap_or(1) as usize))
        })
        .typed_tool(Tool::new("count", "Count slowly, reporting progress"), |ctx, args: CountArgs| async move {
            for i in 1..=args.to {
                tokio::time::sleep(Duration::from_millis(100)).await;
                ctx.progress(i as f64, Some(args.to as f64), None)?;
            }
            Ok::<_, ToolError>(format!("counted to {}", args.to))
        })
        .typed_tool(
            Tool::new("stats", "Count characters, words and lines").read_only().output_schema_for::<Stats>(),
            |_ctx, args: EchoArgs| async move {
                let t = &args.text;
                Ok::<_, ToolError>(Json(Stats {
                    chars: t.chars().count(),
                    words: t.split_whitespace().count(),
                    lines: t.lines().count(),
                }))
            },
        )
        .resource(Resource::new("echo://about", "about").mime_type("text/plain"), |_ctx, _uri| async move {
            Ok("This server is an mcptk example.")
        })
        .resource_template(
            ResourceTemplate::new("echo://upper/{text}", "uppercase").description("Any text, uppercased"),
            |_ctx, uri, vars| async move {
                Ok(ResourceContents::text(uri, Some("text/plain"), vars["text"].to_uppercase()))
            },
        )
        .prompt(
            Prompt::new("haiku", "Write a haiku").argument("topic", "What the haiku is about", true),
            |_ctx, args| async move { Ok(format!("Write a haiku about {}.", args["topic"])) },
        )
        .build()
        .serve_stdio()
        .await
}
