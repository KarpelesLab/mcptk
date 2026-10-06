//! # mcptk — MCP Toolkit
//!
//! Build [Model Context Protocol](https://modelcontextprotocol.io) servers in
//! Rust: tools, resources, prompts, completions, logging, progress,
//! cancellation, sampling, elicitation and roots, served over stdio (or any
//! byte stream) and Streamable HTTP. Includes first-class support for
//! [Claude Code channels](channel), which let a server push events into a
//! session and relay its permission prompts.
//!
//! ```no_run
//! use mcptk::{Server, Tool, ToolError};
//!
//! #[derive(serde::Deserialize, schemars::JsonSchema)]
//! struct Greet {
//!     /// Who to greet.
//!     name: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> mcptk::Result<()> {
//!     Server::builder("greeter", "0.1.0")
//!         .typed_tool(Tool::new("greet", "Greet someone").read_only(), |_ctx, args: Greet| async move {
//!             Ok::<_, ToolError>(format!("Hello, {}!", args.name))
//!         })
//!         .build()
//!         .serve_stdio()
//!         .await
//! }
//! ```

pub mod channel;
mod error;
#[cfg(feature = "http")]
pub mod http;
mod io;
pub mod jsonrpc;
pub mod server;
pub mod types;

pub use channel::{Behavior, ChannelEvent, PermissionRequest, PermissionVerdict};
pub use error::{Error, Result, ToolError};
pub use io::Connection;
pub use server::{Json, RequestContext, Server, ServerBuilder, Session};
pub use types::{
    CallToolResult, Content, LoggingLevel, Prompt, PromptMessage, Resource, ResourceContents, ResourceTemplate, Tool,
};
