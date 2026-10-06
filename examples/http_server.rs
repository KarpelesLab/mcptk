//! An MCP server over Streamable HTTP, at http://127.0.0.1:8080/mcp.
//!
//! Try it with Claude Code:
//!     claude mcp add --transport http notes http://127.0.0.1:8080/mcp

use mcptk::http::StreamableHttp;
use mcptk::{LoggingLevel, Resource, Server, Tool, ToolError};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[derive(Deserialize, schemars::JsonSchema)]
struct AddNote {
    /// The note's text.
    text: String,
}

#[tokio::main]
async fn main() -> mcptk::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let notes = Arc::new(Mutex::new(Vec::<String>::new()));

    let (add, read) = (notes.clone(), notes.clone());
    let server = Server::builder("notes", env!("CARGO_PKG_VERSION"))
        .instructions("Keeps notes shared by every client of this server.")
        .typed_tool(Tool::new("add_note", "Save a note"), move |ctx, args: AddNote| {
            let notes = add.clone();
            async move {
                let count = {
                    let mut notes = notes.lock().unwrap();
                    notes.push(args.text);
                    notes.len()
                };
                ctx.log(LoggingLevel::Info, Some("notes"), format!("now {count} notes"))?;
                // Subscribed clients learn the resource changed.
                ctx.session().server().notify_resource_updated("notes://all");
                Ok::<_, ToolError>(format!("saved note #{count}"))
            }
        })
        .resource(Resource::new("notes://all", "notes").mime_type("text/plain"), move |_ctx, _uri| {
            let notes = read.clone();
            async move { Ok(notes.lock().unwrap().join("\n")) }
        })
        .build();

    tracing::info!("listening on http://127.0.0.1:8080/mcp");
    StreamableHttp::new(server).serve("127.0.0.1:8080").await
}
