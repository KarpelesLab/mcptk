//! A stdio server whose slow tools run as tasks (the
//! `io.modelcontextprotocol/tasks` extension) for clients that support them,
//! and inline for the others.
//!
//!     cargo run --example tasks

use mcptk::tasks::{TaskConfig, TaskContext};
use mcptk::types::{ElicitAction, ElicitParams};
use mcptk::{Server, Tool, ToolError};
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, schemars::JsonSchema)]
struct BuildArgs {
    /// How many steps the build takes, one per second.
    steps: u32,
}

#[tokio::main]
async fn main() -> mcptk::Result<()> {
    // Stdout carries the protocol: logs go to stderr.
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_env_filter("info").init();

    Server::builder("builder", env!("CARGO_PKG_VERSION"))
        .tasks(TaskConfig::new().ttl(Some(Duration::from_secs(600))).poll_interval(Some(Duration::from_secs(2))))
        .typed_task_tool(Tool::new("build", "Run a (pretend) build"), |ctx: TaskContext, args: BuildArgs| async move {
            for step in 1..=args.steps {
                // The task's status message, or a progress notification inline.
                let message = format!("step {step}/{}", args.steps);
                ctx.progress(step as f64, Some(args.steps as f64), Some(&message)).await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok::<_, ToolError>(format!("built in {} steps", args.steps))
        })
        .task_tool(Tool::new("deploy", "Deploy, after asking for confirmation"), |ctx: TaskContext, _args| async move {
            // As a task, this is an `inputRequests` entry that the client
            // answers with `tasks/update`; inline, an elicitation request.
            let answer = ctx
                .elicit(ElicitParams::form(
                    "Deploy to production?",
                    serde_json::json!({"type": "object", "properties": {}}),
                ))
                .await
                .map_err(ToolError::msg)?;
            if answer.action != ElicitAction::Accept {
                return Ok("deployment cancelled".to_string());
            }
            ctx.set_status_message("deploying").await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok("deployed".to_string())
        })
        .build()
        .serve_stdio()
        .await
}
