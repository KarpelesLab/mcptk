//! An MCP App: a dice roller whose results render as an interactive HTML
//! view in hosts that support the `io.modelcontextprotocol/ui` extension,
//! and as plain text everywhere else.
//!
//! - `roll_dice` (visible to the model) is linked to the `ui://dice/view` page.
//! - `reroll` is app-only: the page calls it through the host when you click
//!   "Roll again"; the model never sees it.
//!
//! The page speaks the host's JSON-RPC-over-postMessage protocol by hand
//! (`ui/initialize`, `ui/notifications/tool-result`, `tools/call`), with no
//! SDK and no external resources, so it runs under the default CSP.
//!
//! Run it with an MCP Apps host over stdio:
//!     cargo run --example ui_app

use mcptk::apps::{AppsBuilderExt, ToolUiExt, UiResource, UiSupport};
use mcptk::{CallToolResult, Server, Tool, ToolError};
use serde::{Deserialize, Serialize};

const VIEW_URI: &str = "ui://dice/view";

const VIEW_HTML: &str = r#"<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  body { font-family: system-ui, sans-serif; margin: 0; padding: 16px; }
  #dice { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
  .die { width: 44px; height: 44px; border-radius: 8px; border: 2px solid currentColor;
         display: grid; place-items: center; font-size: 20px; font-weight: 600; }
  button { font: inherit; padding: 6px 14px; cursor: pointer; }
</style>
</head>
<body>
<div id="dice">Waiting for the roll...</div>
<p id="total"></p>
<button id="again" disabled>Roll again</button>
<script>
  let nextId = 1;
  const pending = new Map();
  let last = { count: 2, sides: 6 };

  function request(method, params) {
    const id = nextId++;
    window.parent.postMessage({ jsonrpc: "2.0", id, method, params }, "*");
    return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
  }
  function notify(method, params) {
    window.parent.postMessage({ jsonrpc: "2.0", method, params }, "*");
  }

  function render(result) {
    const data = result && result.structuredContent;
    if (!data) return;
    last = { count: data.rolls.length, sides: data.sides };
    const dice = document.getElementById("dice");
    dice.replaceChildren(...data.rolls.map((n) => {
      const d = document.createElement("div");
      d.className = "die";
      d.textContent = n;
      return d;
    }));
    document.getElementById("total").textContent = `Total: ${data.total} (${data.rolls.length}d${data.sides})`;
    document.getElementById("again").disabled = false;
    notify("ui/notifications/size-changed", { width: document.body.scrollWidth, height: document.body.scrollHeight });
  }

  window.addEventListener("message", (event) => {
    const msg = event.data;
    if (!msg || msg.jsonrpc !== "2.0") return;
    if (msg.id !== undefined && pending.has(msg.id) && !msg.method) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result);
    } else if (msg.method === "ui/notifications/tool-result") {
      render(msg.params);
    }
  });

  document.getElementById("again").addEventListener("click", async () => {
    // An app-only tool on this server, proxied by the host.
    render(await request("tools/call", { name: "reroll", arguments: last }));
  });

  request("ui/initialize", {
    appInfo: { name: "dice-view", version: "1.0.0" },
    appCapabilities: {},
    protocolVersion: "2026-01-26",
  }).then(() => notify("ui/notifications/initialized", {}));
</script>
</body>
</html>
"#;

#[derive(Deserialize, schemars::JsonSchema)]
struct RollArgs {
    /// How many dice to roll (1 to 20).
    #[serde(default = "two")]
    count: u32,
    /// Faces per die (2 to 100).
    #[serde(default = "six")]
    sides: u32,
}

fn two() -> u32 {
    2
}

fn six() -> u32 {
    6
}

#[derive(Serialize, schemars::JsonSchema)]
struct Roll {
    rolls: Vec<u32>,
    sides: u32,
    total: u32,
}

fn roll(args: &RollArgs) -> Result<Roll, ToolError> {
    if !(1..=20).contains(&args.count) || !(2..=100).contains(&args.sides) {
        return Err(ToolError::msg("count must be 1-20 and sides 2-100"));
    }
    let mut bytes = vec![0u8; args.count as usize * 4];
    getrandom::fill(&mut bytes).map_err(|e| ToolError::msg(e.to_string()))?;
    let rolls: Vec<u32> = bytes.as_chunks::<4>().0.iter().map(|c| u32::from_le_bytes(*c) % args.sides + 1).collect();
    Ok(Roll { total: rolls.iter().sum(), rolls, sides: args.sides })
}

/// Text for the model and text-only hosts, data for the view.
fn result(roll: Roll) -> CallToolResult {
    let text = format!("Rolled {:?} (total {})", roll.rolls, roll.total);
    CallToolResult::text(text).structured(serde_json::to_value(roll).unwrap())
}

#[tokio::main]
async fn main() -> mcptk::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_env_filter("info").init();

    let view = UiResource::new(VIEW_URI, "dice_view", VIEW_HTML)
        .title("Dice")
        .description("Interactive view of a dice roll")
        .prefers_border(true);

    Server::builder("dice", env!("CARGO_PKG_VERSION"))
        .instructions("Rolls dice. Results show as an interactive view where the host supports MCP Apps.")
        .ui_resource(view)
        .typed_tool(
            Tool::new("roll_dice", "Roll dice and show the result").output_schema_for::<Roll>().ui_resource(VIEW_URI),
            |ctx, args: RollArgs| async move {
                if !ctx.supports_ui() {
                    tracing::info!("client can't render MCP Apps; it gets the text content only");
                }
                Ok::<_, ToolError>(result(roll(&args)?))
            },
        )
        .typed_tool(
            Tool::new("reroll", "Roll the same dice again")
                .output_schema_for::<Roll>()
                .ui_resource(VIEW_URI)
                .app_only(),
            |_ctx, args: RollArgs| async move { Ok::<_, ToolError>(result(roll(&args)?)) },
        )
        .tool_filter(mcptk::apps::hide_app_only_tools_without_ui)
        .build()
        .serve_stdio()
        .await
}
