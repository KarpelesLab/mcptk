//! Claude Code channels: push events into a Claude Code session, and relay
//! its tool permission prompts.
//!
//! A server becomes a channel by declaring the `claude/channel` experimental
//! capability ([`ServerBuilder::channel`](crate::ServerBuilder::channel)),
//! then sends [`ChannelEvent`]s with
//! [`Session::channel_event`](crate::Session::channel_event). Claude sees each
//! one as `<channel source="<server>" key="value"...>content</channel>`.
//!
//! A two-way channel with authenticated senders can also opt in to permission
//! relay ([`ServerBuilder::channel_permission`](crate::ServerBuilder::channel_permission)):
//! Claude Code then sends a [`PermissionRequest`] whenever a tool approval
//! dialog opens, and the server answers with
//! [`Session::permission_verdict`](crate::Session::permission_verdict).
//! Anyone who can send a verdict can approve tool use in the session: only
//! enable this behind a real sender check.
//!
//! Clients that didn't load the server as a channel drop these events
//! silently; sending one never fails because of that.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Experimental capability that makes a server a channel.
pub const CHANNEL_CAPABILITY: &str = "claude/channel";
/// Experimental capability that opts a channel in to permission relay.
pub const PERMISSION_CAPABILITY: &str = "claude/channel/permission";
/// Server → client: a channel event.
pub const CHANNEL_NOTIFICATION: &str = "notifications/claude/channel";
/// Client → server: a tool permission prompt opened.
pub const PERMISSION_REQUEST_NOTIFICATION: &str = "notifications/claude/channel/permission_request";
/// Server → client: the verdict for a permission prompt.
pub const PERMISSION_NOTIFICATION: &str = "notifications/claude/channel/permission";

/// An event pushed into the session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelEvent {
    /// The body of the `<channel>` tag.
    pub content: String,
    /// Attributes of the `<channel>` tag, for routing (chat id, sender,
    /// severity...). Keys must pass [`is_valid_meta_key`]; Claude Code drops
    /// others silently.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, String>,
}

impl ChannelEvent {
    pub fn new(content: impl Into<String>) -> Self {
        ChannelEvent { content: content.into(), meta: BTreeMap::new() }
    }

    /// Add an attribute. Invalid keys are dropped (with a warning), as
    /// Claude Code would drop them anyway; see [`ChannelEvent::try_meta`].
    pub fn meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        if is_valid_meta_key(&key) {
            self.meta.insert(key, value.into());
        } else {
            tracing::warn!(key, "dropping channel meta key: only letters, digits and underscores are allowed");
        }
        self
    }

    /// Add an attribute, failing on an invalid key.
    pub fn try_meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Result<Self, InvalidMetaKey> {
        let key = key.into();
        if !is_valid_meta_key(&key) {
            return Err(InvalidMetaKey(key));
        }
        self.meta.insert(key, value.into());
        Ok(self)
    }
}

/// A meta key Claude Code would drop.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid channel meta key {0:?}: only letters, digits and underscores are allowed")]
pub struct InvalidMetaKey(pub String);

/// Whether `key` is usable as a `<channel>` tag attribute: an identifier made
/// of ASCII letters, digits and underscores, not starting with a digit.
pub fn is_valid_meta_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A tool approval prompt Claude Code relays to the channel.
///
/// `description` and `input_preview` come from the model's tool call: treat
/// them as untrusted text when rendering.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRequest {
    /// Five lowercase letters (`a`-`z` without `l`). The verdict must echo it.
    pub request_id: String,
    /// The tool to approve, e.g. `Bash` or `Write`.
    pub tool_name: String,
    /// A human-readable summary of the call.
    #[serde(default)]
    pub description: String,
    /// The tool's arguments as JSON-shaped display text.
    #[serde(default)]
    pub input_preview: String,
}

impl PermissionRequest {
    /// A prompt to send to the approver, telling them how to answer.
    pub fn prompt_text(&self) -> String {
        let mut text = format!("Claude wants to run {}: {}\n", self.tool_name, self.description);
        if !self.input_preview.is_empty() {
            text.push_str(&self.input_preview);
            text.push('\n');
        }
        text.push_str(&format!("\nReply \"yes {0}\" or \"no {0}\"", self.request_id));
        text
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Behavior {
    Allow,
    Deny,
}

/// The answer to a [`PermissionRequest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionVerdict {
    pub request_id: String,
    pub behavior: Behavior,
}

/// Whether `id` looks like a request id Claude Code issues.
pub fn is_valid_request_id(id: &str) -> bool {
    id.len() == 5 && id.bytes().all(|b| b.is_ascii_lowercase() && b != b'l')
}

/// Parse an approver's reply such as `yes abcde`, `n abcde` (any case).
/// Returns `None` for anything else, which should go to Claude as a normal
/// message instead.
pub fn parse_permission_reply(text: &str) -> Option<PermissionVerdict> {
    let mut words = text.split_whitespace();
    let (word, id) = (words.next()?, words.next()?);
    if words.next().is_some() {
        return None;
    }
    let behavior = match word.to_ascii_lowercase().as_str() {
        "y" | "yes" => Behavior::Allow,
        "n" | "no" => Behavior::Deny,
        _ => return None,
    };
    let id = id.to_ascii_lowercase();
    is_valid_request_id(&id).then_some(PermissionVerdict { request_id: id, behavior })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_keys() {
        assert!(is_valid_meta_key("chat_id"));
        assert!(is_valid_meta_key("_x9"));
        assert!(!is_valid_meta_key("chat-id"));
        assert!(!is_valid_meta_key("9lives"));
        assert!(!is_valid_meta_key(""));
        let ev = ChannelEvent::new("hi").meta("ok", "1").meta("not-ok", "2");
        assert_eq!(ev.meta.len(), 1);
        assert!(ChannelEvent::new("x").try_meta("a b", "c").is_err());
    }

    #[test]
    fn event_json() {
        let ev = ChannelEvent::new("build failed").meta("severity", "high");
        assert_eq!(
            serde_json::to_value(&ev).unwrap(),
            serde_json::json!({"content":"build failed","meta":{"severity":"high"}})
        );
        assert_eq!(serde_json::to_value(ChannelEvent::new("x")).unwrap(), serde_json::json!({"content":"x"}));
    }

    #[test]
    fn replies() {
        let v = parse_permission_reply("  Yes ABCDE ").unwrap();
        assert_eq!(v, PermissionVerdict { request_id: "abcde".into(), behavior: Behavior::Allow });
        assert_eq!(parse_permission_reply("n qwert").unwrap().behavior, Behavior::Deny);
        assert!(parse_permission_reply("yes").is_none());
        assert!(parse_permission_reply("yes abcdl").is_none()); // no 'l' in ids
        assert!(parse_permission_reply("yes abcd").is_none());
        assert!(parse_permission_reply("approve abcde").is_none());
        assert!(parse_permission_reply("yes abcde please").is_none());
    }
}
