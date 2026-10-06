# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/KarpelesLab/mcptk/releases/tag/v0.1.0) - 2026-10-06

### Added

- MCP server toolkit with its own protocol implementation on tokio and serde:
  tools (raw or typed, structured output), resources and templates, prompts,
  completions, logging, progress, cancellation, paginated lists, per-session
  tool filtering and runtime registration.
- Protocol revisions 2026-07-28 (stateless requests, `server/discover`,
  `subscriptions/listen`, multi round-trip requests) and the handshake
  revisions 2025-11-25, 2025-06-18, 2025-03-26 and 2024-11-05, served side by
  side.
- Transports: stdio and any byte stream, Streamable HTTP, and WebSocket (`ws`
  feature).
- OAuth 2.1 resource server support for HTTP and WebSocket (protected resource
  metadata, bearer challenges, pluggable token validation).
- Extensions: tasks (`io.modelcontextprotocol/tasks`) and MCP Apps
  (`io.modelcontextprotocol/ui`).
- Claude Code channels (events and permission relay) and `anthropic/*` tool
  metadata helpers.
