//! # mcptk — MCP Toolkit
//!
//! Build [Model Context Protocol](https://modelcontextprotocol.io) servers in
//! Rust: tools, resources, prompts, completions, logging, progress,
//! cancellation, sampling, elicitation and roots, served over stdio (or any
//! byte stream) and Streamable HTTP. Includes first-class support for
//! [Claude Code channels](channel), which let a server push events into a
//! session and relay its permission prompts.
//!
//! Servers speak protocol revision 2026-07-28 (stateless: each request
//! carries its metadata) and the handshake-based revisions before it, at the
//! same time. See [`RequestContext`] for writing handlers that work with
//! both, and [`InputRequired`] for multi round-trip requests.
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

pub mod apps;
pub mod auth;
pub mod channel;
mod error;
#[cfg(feature = "http")]
pub mod http;
mod io;
pub mod jsonrpc;
pub mod server;
pub mod types;
#[cfg(feature = "ws")]
pub mod ws;

pub use channel::{Behavior, ChannelEvent, PermissionRequest, PermissionVerdict};
pub use error::{Error, Result, ToolError};
pub use io::Connection;
pub use server::{InputRequired, Json, RequestContext, Server, ServerBuilder, Session, tasks};
pub use types::{
    CallToolResult, Content, LoggingLevel, Prompt, PromptMessage, Resource, ResourceContents, ResourceTemplate, Tool,
};
