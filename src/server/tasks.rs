//! The Tasks extension (`io.modelcontextprotocol/tasks`, protocol revision
//! 2026-07-28, SEP-2663).
//!
//! A tool call that takes a while can answer right away with a *task handle*
//! (`resultType: "task"`). The client then polls `tasks/get` until the task
//! reaches a terminal status, answers the task's input requests (elicitation,
//! sampling, roots) with `tasks/update`, and may cancel it with
//! `tasks/cancel`.
//!
//! The server decides which calls become tasks. With mcptk, register a tool
//! with [`ServerBuilder::task_tool`] (or [`ServerBuilder::typed_task_tool`]):
//! when the request declares the extension in its client capabilities
//! (`_meta["io.modelcontextprotocol/clientCapabilities"].extensions`), the
//! call is answered with a task and the handler runs in the background;
//! otherwise it runs inline like any tool (see [`TaskMode`]).
//!
//! ```no_run
//! use mcptk::tasks::{TaskConfig, TaskContext};
//! use mcptk::{Server, Tool, ToolError};
//! use std::time::Duration;
//!
//! let server = Server::builder("jobs", "1.0")
//!     .tasks(TaskConfig::new().ttl(Some(Duration::from_secs(600))))
//!     .task_tool(Tool::new("crunch", "Crunch numbers for a while"), |ctx: TaskContext, _args| async move {
//!         for step in 1..=10 {
//!             ctx.progress(step as f64, Some(10.0), Some(&format!("step {step}/10"))).await?;
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!         }
//!         Ok::<_, ToolError>("crunched")
//!     })
//!     .build();
//! ```
//!
//! Task state lives in a [`TaskStore`] owned by the [`Server`], not by a
//! session: a task created over one HTTP request (or session) can be polled
//! from another. Task ids are 128-bit random values, the only thing a client
//! needs to access a task, so treat them as secrets. Running handlers and
//! their input requests live in the process that started them.
//!
//! The experimental tasks of revision 2025-11-25 (`capabilities.tasks`, the
//! `task` parameter of `tools/call`, `tasks/result`, `tasks/list`) are not
//! supported: they are not wire-compatible with the extension.

use super::session::random_id;
use super::{BoxFuture, IntoToolResult, RequestContext, RequestFn, Server, ServerBuilder, Session};
use crate::error::{Error, Result, ToolError};
use crate::jsonrpc::ErrorObject;
use crate::types::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// The extension identifier, as declared in `capabilities.extensions`.
pub const EXTENSION_ID: &str = "io.modelcontextprotocol/tasks";

/// `_meta` key carrying the client's capabilities on each request
/// (2026-07-28).
pub const CLIENT_CAPABILITIES_META: &str = crate::types::META_CLIENT_CAPABILITIES;

/// `_meta` key carrying the request's protocol revision (2026-07-28).
pub const PROTOCOL_VERSION_META: &str = crate::types::META_PROTOCOL_VERSION;

/// JSON-RPC error code: the request needs a capability the client didn't
/// declare (2026-07-28).
pub const MISSING_REQUIRED_CLIENT_CAPABILITY: i64 = crate::jsonrpc::MISSING_REQUIRED_CLIENT_CAPABILITY;

/// Revisions where the extension is not defined.
const LEGACY_VERSIONS: &[&str] = crate::types::HANDSHAKE_PROTOCOL_VERSIONS;

/// The status of a task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// The request is being processed.
    Working,
    /// The task waits for the client to answer its `inputRequests`.
    InputRequired,
    /// Done; the result is in `result` (tool errors with `isError` too).
    Completed,
    /// The request failed with a JSON-RPC error, in `error`.
    Failed,
    /// The request was cancelled.
    Cancelled,
}

impl TaskStatus {
    /// Whether the task can no longer change.
    pub fn is_terminal(self) -> bool {
        matches!(self, TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled)
    }
}

/// A task, as `tasks/get` returns it (a `DetailedTask`): status-specific
/// fields (`inputRequests`, `result`, `error`) are set for that status only.
///
/// This is also what a [`TaskStore`] keeps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub task_id: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// ISO 8601 creation time.
    pub created_at: String,
    /// ISO 8601 time of the last change.
    pub last_updated_at: String,
    /// Time to live from creation, in milliseconds; `None` (null) for
    /// unlimited.
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    /// Suggested polling interval, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    /// Outstanding requests to the client (`input_required`), by key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_requests: Option<JsonObject>,
    /// The final result (`completed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// The JSON-RPC error (`failed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    /// Who created the task: the OAuth subject of the `tools/call` request
    /// (see [`RequestContext::auth`]), if authenticated. Only that subject
    /// can then access the task. Kept by the store, never sent to clients.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

impl Task {
    /// When the task's time to live runs out, if it has one.
    pub fn expires_at(&self) -> Option<SystemTime> {
        let ttl = self.ttl_ms?;
        Some(parse_time(&self.created_at)? + Duration::from_millis(ttl))
    }

    /// Whether the task's time to live ran out.
    pub fn is_expired(&self) -> bool {
        self.expires_at().is_some_and(|t| t <= SystemTime::now())
    }

    /// The task as clients see it.
    fn to_wire(&self) -> Result<Value, ErrorObject> {
        let mut v = serde_json::to_value(self).map_err(|e| ErrorObject::internal(e.to_string()))?;
        if let Value::Object(map) = &mut v {
            map.remove("owner");
        }
        Ok(v)
    }

    fn to_result(&self, result_type: &str) -> Result<Value, ErrorObject> {
        let mut v = self.to_wire()?;
        v["resultType"] = result_type.into();
        Ok(v)
    }
}

/// A future returned by [`TaskStore`] methods.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Where task state is kept. The default, [`InMemoryTaskStore`], keeps it
/// in the process; implement this to keep it elsewhere (a database...).
///
/// `put` must not return before a `get` of the same task would see it: the
/// server answers with a task handle only once it is stored. Stores may drop
/// tasks once [`Task::is_expired`]; the server also checks on every read.
pub trait TaskStore: Send + Sync + 'static {
    /// Insert or replace a task.
    fn put(&self, task: Task) -> StoreFuture<'_, ()>;
    fn get<'a>(&'a self, task_id: &'a str) -> StoreFuture<'a, Option<Task>>;
    fn delete<'a>(&'a self, task_id: &'a str) -> StoreFuture<'a, ()>;
}

/// A [`TaskStore`] in memory. Expired tasks are purged as new ones are
/// stored.
#[derive(Default)]
pub struct InMemoryTaskStore {
    tasks: Mutex<HashMap<String, Task>>,
}

impl InMemoryTaskStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many tasks are stored (expired ones included until purged).
    pub fn len(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl TaskStore for InMemoryTaskStore {
    fn put(&self, task: Task) -> StoreFuture<'_, ()> {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|_, t| !t.is_expired());
        tasks.insert(task.task_id.clone(), task);
        Box::pin(std::future::ready(Ok(())))
    }

    fn get<'a>(&'a self, task_id: &'a str) -> StoreFuture<'a, Option<Task>> {
        let task = self.tasks.lock().unwrap().get(task_id).cloned();
        Box::pin(std::future::ready(Ok(task)))
    }

    fn delete<'a>(&'a self, task_id: &'a str) -> StoreFuture<'a, ()> {
        self.tasks.lock().unwrap().remove(task_id);
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Settings of the tasks extension, for [`ServerBuilder::tasks`].
#[derive(Clone)]
pub struct TaskConfig {
    store: Arc<dyn TaskStore>,
    ttl: Option<Duration>,
    poll_interval: Option<Duration>,
}

impl Default for TaskConfig {
    fn default() -> Self {
        TaskConfig {
            store: Arc::new(InMemoryTaskStore::new()),
            ttl: Some(Duration::from_secs(3600)),
            poll_interval: Some(Duration::from_secs(1)),
        }
    }
}

impl TaskConfig {
    /// Tasks in memory, kept for an hour, polled every second.
    pub fn new() -> Self {
        Self::default()
    }

    /// Where tasks are kept.
    pub fn store(mut self, store: impl TaskStore) -> Self {
        self.store = Arc::new(store);
        self
    }

    /// Where tasks are kept, shared with other code.
    pub fn shared_store(mut self, store: Arc<dyn TaskStore>) -> Self {
        self.store = store;
        self
    }

    /// How long a task lives from its creation (`None`: forever). A task
    /// still running then is cancelled; either way it is then forgotten and
    /// `tasks/get` reports it expired.
    pub fn ttl(mut self, ttl: Option<Duration>) -> Self {
        self.ttl = ttl;
        self
    }

    /// How often clients should poll (`pollIntervalMs`).
    pub fn poll_interval(mut self, interval: Option<Duration>) -> Self {
        self.poll_interval = interval;
        self
    }
}

/// When a task tool's calls become tasks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TaskMode {
    /// Never: run inline like any tool.
    Never,
    /// When the request declares the extension; inline otherwise.
    #[default]
    Optional,
    /// Always: requests that don't declare the extension get a
    /// "missing required client capability" (-32021) error.
    Required,
}

/// Whether the request declared the tasks extension in its per-request
/// client capabilities (`_meta["io.modelcontextprotocol/clientCapabilities"]`),
/// which is what allows answering it with a task.
///
/// Capabilities declared at `initialize` don't count: the extension is not
/// defined for revisions up to 2025-11-25, and the spec requires the
/// declaration on the request itself.
pub fn client_supports_tasks(ctx: &RequestContext) -> bool {
    if ctx.is_stateless() {
        let extensions = ctx.client_capabilities().and_then(|c| c.extensions.as_ref());
        return extensions.and_then(|e| e.get(EXTENSION_ID)).is_some_and(Value::is_object);
    }
    declares_tasks(ctx.meta())
}

fn declares_tasks(meta: Option<&JsonObject>) -> bool {
    let Some(meta) = meta else { return false };
    if let Some(v) = meta.get(PROTOCOL_VERSION_META).and_then(Value::as_str)
        && LEGACY_VERSIONS.contains(&v)
    {
        return false;
    }
    meta.get(CLIENT_CAPABILITIES_META)
        .and_then(|c| c.get("extensions"))
        .and_then(|e| e.get(EXTENSION_ID))
        .is_some_and(Value::is_object)
}

/// The error for a request that needs the tasks extension.
pub fn missing_capability_error() -> ErrorObject {
    ErrorObject::new(MISSING_REQUIRED_CLIENT_CAPABILITY, "Missing required client capability")
        .with_data(json!({ "requiredCapabilities": { "extensions": { EXTENSION_ID: {} } } }))
}

fn not_found(what: &str) -> Error {
    Error::invalid_params(format!("Failed to {what} task: Task not found"))
}

fn expired(what: &str) -> Error {
    Error::invalid_params(format!("Failed to {what} task: Task has expired"))
}

type TaskToolFn = Arc<dyn Fn(TaskContext, JsonObject) -> BoxFuture<Result<CallToolResult, ToolError>> + Send + Sync>;

/// The extension's state in a [`Server`].
pub(crate) struct TaskManager {
    config: RwLock<TaskConfig>,
    modes: RwLock<HashMap<String, TaskMode>>,
    running: Mutex<HashMap<String, Arc<RunningTask>>>,
}

tokio::task_local! {
    /// The task a tool handler is running as.
    static CURRENT_TASK: Arc<RunningTask>;
}

impl TaskManager {
    fn new() -> Self {
        TaskManager {
            config: RwLock::new(TaskConfig::default()),
            modes: RwLock::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
        }
    }

    fn store(&self) -> Arc<dyn TaskStore> {
        self.config.read().unwrap().store.clone()
    }

    fn mode(&self, tool: &str) -> TaskMode {
        self.modes.read().unwrap().get(tool).copied().unwrap_or(TaskMode::Never)
    }

    fn running(&self, task_id: &str) -> Option<Arc<RunningTask>> {
        self.running.lock().unwrap().get(task_id).cloned()
    }

    /// A stored task, if it exists, hasn't expired (expired ones are
    /// forgotten) and, when `requester` is given, belongs to it. Tasks
    /// created by an authenticated request belong to its subject; others
    /// to whoever knows their id.
    async fn lookup(&self, task_id: &str, what: &str, requester: Option<&RequestContext>) -> Result<Task> {
        let store = self.store();
        let Some(task) = store.get(task_id).await? else {
            return Err(not_found(what));
        };
        if let (Some(owner), Some(ctx)) = (&task.owner, requester)
            && ctx.auth().map(|a| &a.subject) != Some(owner)
        {
            // Same answer as for an unknown id: don't reveal it exists.
            return Err(not_found(what));
        }
        if task.is_expired() {
            let running = self.running.lock().unwrap().remove(task_id);
            if let Some(rt) = running {
                rt.expire().await;
            }
            store.delete(task_id).await?;
            return Err(expired(what));
        }
        Ok(task)
    }

    /// The tasks among `ids` that `ctx` may watch (they exist and belong
    /// to it), for `subscriptions/listen`.
    async fn watchable(&self, ctx: &RequestContext, ids: Vec<String>) -> Vec<String> {
        let mut out = Vec::new();
        for id in ids {
            if self.lookup(&id, "watch", Some(ctx)).await.is_ok() {
                out.push(id);
            }
        }
        out
    }

    /// Create a task for a tool call, start it, and return the
    /// `CreateTaskResult`.
    async fn start(
        self: &Arc<Self>,
        server: Server,
        ctx: RequestContext,
        params: CallToolParams,
    ) -> Result<Value, ErrorObject> {
        let (store, ttl, poll) = {
            let c = self.config.read().unwrap();
            (c.store.clone(), c.ttl, c.poll_interval)
        };
        let now = format_time(SystemTime::now());
        let task = Task {
            task_id: random_id(),
            status: TaskStatus::Working,
            status_message: None,
            created_at: now.clone(),
            last_updated_at: now,
            ttl_ms: ttl.map(|d| d.as_millis() as u64),
            poll_interval_ms: poll.map(|d| d.as_millis() as u64),
            input_requests: None,
            result: None,
            error: None,
            owner: ctx.auth().map(|a| a.subject.clone()),
        };
        store.put(task.clone()).await.map_err(|e| e.to_error_object())?;
        let rt = Arc::new(RunningTask {
            id: task.task_id.clone(),
            state: tokio::sync::Mutex::new(task.clone()),
            store,
            server: Arc::downgrade(&server.inner),
            cancel: CancellationToken::new(),
            inputs: Mutex::new(HashMap::new()),
            next_key: AtomicU64::new(1),
        });
        self.running.lock().unwrap().insert(task.task_id.clone(), rt.clone());
        let deadline = ttl.map(|ttl| tokio::time::Instant::now() + ttl);
        let manager = self.clone();
        tokio::spawn(async move {
            let expiry = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            };
            let run = CURRENT_TASK.scope(rt.clone(), server.call_tool(ctx, params));
            tokio::select! {
                r = run => match r {
                    Ok(result) => {
                        let result = serde_json::to_value(result).unwrap_or_else(|e| json!({
                            "content": [{"type": "text", "text": format!("failed to serialize result: {e}")}],
                            "isError": true,
                        }));
                        rt.finish(TaskStatus::Completed, None, |t| t.result = Some(result)).await;
                    }
                    Err(e) => {
                        let e = e.to_error_object();
                        let message = e.message.clone();
                        rt.finish(TaskStatus::Failed, Some(message), |t| t.error = Some(e)).await;
                    }
                },
                _ = rt.cancel.cancelled() => {
                    rt.finish(TaskStatus::Cancelled, Some("Cancelled by the client".into()), |_| {}).await;
                }
                _ = expiry => {
                    rt.expire().await;
                    if let Err(e) = rt.store.delete(&rt.id).await {
                        tracing::warn!(task = rt.id, "failed to delete expired task: {e}");
                    }
                }
            }
            manager.running.lock().unwrap().remove(&rt.id);
        });
        task.to_result("task")
    }

    async fn get(self: Arc<Self>, ctx: RequestContext, params: Option<Value>) -> Result<Value> {
        let p: TaskIdParams = require_tasks(&ctx, params)?;
        Ok(self.lookup(&p.task_id, "retrieve", Some(&ctx)).await?.to_result("complete")?)
    }

    async fn update(self: Arc<Self>, ctx: RequestContext, params: Option<Value>) -> Result<Value> {
        let p: UpdateTaskParams = require_tasks(&ctx, params)?;
        self.lookup(&p.task_id, "update", Some(&ctx)).await?;
        if let Some(rt) = self.running(&p.task_id) {
            rt.answer(p.input_responses).await;
        }
        Ok(json!({ "resultType": "complete" }))
    }

    async fn cancel(self: Arc<Self>, ctx: RequestContext, params: Option<Value>) -> Result<Value> {
        let p: TaskIdParams = require_tasks(&ctx, params)?;
        self.lookup(&p.task_id, "cancel", Some(&ctx)).await?;
        if let Some(rt) = self.running(&p.task_id) {
            rt.cancel.cancel();
        }
        Ok(json!({ "resultType": "complete" }))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskIdParams {
    task_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateTaskParams {
    task_id: String,
    #[serde(default)]
    input_responses: JsonObject,
}

fn require_tasks<T: DeserializeOwned>(ctx: &RequestContext, params: Option<Value>) -> Result<T> {
    if !client_supports_tasks(ctx) {
        return Err(missing_capability_error().into());
    }
    serde_json::from_value(params.unwrap_or_else(|| json!({})))
        .map_err(|e| Error::invalid_params(format!("invalid params: {e}")))
}

/// A task whose handler runs in this process.
struct RunningTask {
    id: String,
    /// The latest state, also in the store. Locked across store writes so
    /// they happen in order.
    state: tokio::sync::Mutex<Task>,
    store: Arc<dyn TaskStore>,
    /// To tell `subscriptions/listen` streams watching the task.
    server: std::sync::Weak<super::ServerInner>,
    cancel: CancellationToken,
    /// Outstanding input requests, by key.
    inputs: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    next_key: AtomicU64,
}

impl RunningTask {
    /// Change the task, unless it is already terminal, and store it.
    async fn update(&self, f: impl FnOnce(&mut Task)) -> Result<()> {
        let mut task = self.state.lock().await;
        if task.status.is_terminal() {
            return Ok(());
        }
        f(&mut task);
        task.last_updated_at = format_time(SystemTime::now());
        self.store.put(task.clone()).await?;
        if let Some(server) = self.server.upgrade()
            && let Ok(wire) = task.to_wire()
        {
            server.listeners.notify_task(&self.id, &wire);
        }
        Ok(())
    }

    /// The task ran out of time: make it terminal (so nothing stores it
    /// again) and stop it.
    async fn expire(&self) {
        self.state.lock().await.status = TaskStatus::Cancelled;
        self.cancel.cancel();
        self.inputs.lock().unwrap().clear();
    }

    async fn finish(&self, status: TaskStatus, message: Option<String>, f: impl FnOnce(&mut Task)) {
        let r = self
            .update(|t| {
                t.status = status;
                if message.is_some() {
                    t.status_message = message;
                }
                t.input_requests = None;
                f(t);
            })
            .await;
        if let Err(e) = r {
            tracing::warn!("failed to store task: {e}");
        }
        self.inputs.lock().unwrap().clear();
    }

    /// Ask the client something through `inputRequests`, and wait for the
    /// answer.
    async fn input(&self, method: &str, params: Value) -> Result<Value> {
        let key = format!("input-{}", self.next_key.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        self.inputs.lock().unwrap().insert(key.clone(), tx);
        let mut request = json!({ "method": method, "params": params });
        super::input::for_stateless(&mut request);
        let stored = self
            .update(|t| {
                t.status = TaskStatus::InputRequired;
                t.input_requests.get_or_insert_with(JsonObject::new).insert(key.clone(), request);
            })
            .await;
        if let Err(e) = stored {
            self.inputs.lock().unwrap().remove(&key);
            return Err(e);
        }
        rx.await.map_err(|_| Error::Cancelled)
    }

    /// Deliver `tasks/update` responses to the requests waiting for them.
    /// Keys not outstanding are ignored.
    async fn answer(&self, responses: JsonObject) {
        let mut answered = Vec::new();
        {
            let mut inputs = self.inputs.lock().unwrap();
            for (key, value) in responses {
                if let Some(tx) = inputs.remove(&key) {
                    let _ = tx.send(value);
                    answered.push(key);
                }
            }
        }
        if answered.is_empty() {
            return;
        }
        let r = self
            .update(|t| {
                if let Some(requests) = &mut t.input_requests {
                    for key in &answered {
                        requests.remove(key);
                    }
                    if requests.is_empty() {
                        t.input_requests = None;
                    }
                }
                if t.input_requests.is_none() && t.status == TaskStatus::InputRequired {
                    t.status = TaskStatus::Working;
                }
            })
            .await;
        if let Err(e) = r {
            tracing::warn!("failed to store task: {e}");
        }
    }
}

/// What a task tool's handler gets: the [`RequestContext`] of the
/// `tools/call` request, plus the task it runs as, if any.
///
/// Its methods work either way: run as a task, input requests go through the
/// task's `inputRequests` (answered by `tasks/update`) and progress becomes
/// the task's `statusMessage`; run inline, they are ordinary requests to the
/// client and progress notifications.
///
/// Run as a task, the handler outlives the `tools/call` request (and the
/// session): don't send notifications through [`TaskContext::request`],
/// whose stream may be gone. Progress and log notifications aren't
/// supported for tasks; [`TaskContext::log`] drops them.
#[derive(Clone)]
pub struct TaskContext {
    request: RequestContext,
    task: Option<Arc<RunningTask>>,
}

impl TaskContext {
    /// The context of the `tools/call` request that started this.
    pub fn request(&self) -> &RequestContext {
        &self.request
    }

    pub fn session(&self) -> &Session {
        self.request.session()
    }

    /// The task id, when running as a task.
    pub fn task_id(&self) -> Option<&str> {
        self.task.as_ref().map(|t| t.id.as_str())
    }

    pub fn is_task(&self) -> bool {
        self.task.is_some()
    }

    /// Whether the task (or inline, the request) was cancelled. The
    /// handler's future is dropped when that happens; this is for work done
    /// outside it.
    pub fn is_cancelled(&self) -> bool {
        match &self.task {
            Some(t) => t.cancel.is_cancelled(),
            None => self.request.is_cancelled(),
        }
    }

    pub async fn cancelled(&self) {
        match &self.task {
            Some(t) => t.cancel.cancelled().await,
            None => self.request.cancelled().await,
        }
    }

    /// Set the task's `statusMessage` (a no-op inline).
    pub async fn set_status_message(&self, message: impl Into<String>) -> Result<()> {
        match &self.task {
            Some(t) => {
                let message = message.into();
                t.update(|task| task.status_message = Some(message)).await
            }
            None => Ok(()),
        }
    }

    /// Report progress. Inline, this is a progress notification (if the
    /// client asked for them); as a task, `message` becomes the task's
    /// `statusMessage`.
    pub async fn progress(&self, progress: f64, total: Option<f64>, message: Option<&str>) -> Result<()> {
        match (&self.task, message) {
            (Some(_), Some(m)) => self.set_status_message(m).await,
            (Some(_), None) => Ok(()),
            (None, _) => self.request.progress(progress, total, message),
        }
    }

    /// Send a log message. Inline only: log notifications aren't supported
    /// for tasks, so they are dropped.
    pub fn log(&self, level: LoggingLevel, logger: Option<&str>, data: impl Into<Value>) -> Result<()> {
        match &self.task {
            Some(_) => Ok(()),
            None => self.request.log(level, logger, data),
        }
    }

    /// Send the client a request (`elicitation/create`,
    /// `sampling/createMessage`, `roots/list`) and wait for its result. As a
    /// task, it becomes one of the task's `inputRequests`.
    pub async fn input(&self, method: &str, params: Value) -> Result<Value> {
        match &self.task {
            Some(t) => t.input(method, params).await,
            None => self.request.ask(method, params).await,
        }
    }

    /// The client's capabilities: those sent with the request (2026-07-28),
    /// if any, else those declared at initialize.
    fn client_capabilities(&self) -> ClientCapabilities {
        if self.request.is_stateless() {
            return self.request.client_capabilities().cloned().unwrap_or_default();
        }
        let per_request = self.request.meta().and_then(|m| m.get(CLIENT_CAPABILITIES_META));
        match per_request {
            Some(caps) => serde_json::from_value(caps.clone()).unwrap_or_default(),
            None => self.session().client_capabilities().cloned().unwrap_or_default(),
        }
    }

    fn require(&self, what: &'static str, has: impl Fn(&ClientCapabilities) -> bool) -> Result<()> {
        if has(&self.client_capabilities()) { Ok(()) } else { Err(Error::Unsupported(what)) }
    }

    async fn task_input<T: DeserializeOwned>(&self, method: &str, params: impl Serialize) -> Result<T> {
        Ok(serde_json::from_value(self.input(method, serde_json::to_value(params)?).await?)?)
    }

    /// Ask the user for input (elicitation). URL mode needs the client's
    /// `elicitation.url` capability.
    pub async fn elicit(&self, params: ElicitParams) -> Result<ElicitResult> {
        if self.task.is_none() {
            return self.request.elicit(params).await;
        }
        match &params {
            ElicitParams::Form(_) => self.require("elicitation", ClientCapabilities::supports_elicitation_form)?,
            ElicitParams::Url(_) => self.require("URL elicitation", ClientCapabilities::supports_elicitation_url)?,
        }
        self.task_input("elicitation/create", params).await
    }

    /// Ask the client's LLM for a completion (sampling). Requests with tools
    /// need the client's `sampling.tools` capability.
    pub async fn create_message(&self, params: CreateMessageParams) -> Result<CreateMessageResult> {
        if self.task.is_none() {
            return self.request.create_message(params).await;
        }
        self.require("sampling", |c| c.sampling.is_some())?;
        if params.uses_tools() {
            self.require("sampling with tools", ClientCapabilities::supports_sampling_tools)?;
        }
        self.task_input("sampling/createMessage", params).await
    }

    /// The client's roots.
    pub async fn list_roots(&self) -> Result<ListRootsResult> {
        if self.task.is_none() {
            return self.request.list_roots().await;
        }
        self.require("roots", |c| c.roots.is_some())?;
        self.task_input("roots/list", json!({})).await
    }
}

fn task_context(request: RequestContext) -> TaskContext {
    TaskContext { request, task: CURRENT_TASK.try_with(Arc::clone).ok() }
}

fn task_tool_fn<F, Fut, R>(f: F) -> TaskToolFn
where
    F: Fn(TaskContext, JsonObject) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
    R: IntoToolResult,
{
    Arc::new(move |ctx, args| {
        let fut = f(ctx, args);
        Box::pin(async move { fut.await.map(IntoToolResult::into_tool_result) })
    })
}

/// A plain tool handler running `f` with a [`TaskContext`].
fn as_tool_handler(
    f: TaskToolFn,
) -> impl Fn(RequestContext, JsonObject) -> BoxFuture<Result<CallToolResult, ToolError>> + Send + Sync + 'static {
    move |ctx, args| f(task_context(ctx), args)
}

impl ServerBuilder {
    fn task_manager(&mut self) -> Arc<TaskManager> {
        if let Some(m) = &self.config.tasks {
            return m.clone();
        }
        let manager = Arc::new(TaskManager::new());
        self.config.tasks = Some(manager.clone());
        self.config.extensions.insert(EXTENSION_ID.to_string(), Value::Object(JsonObject::new()));
        type Method = fn(Arc<TaskManager>, RequestContext, Option<Value>) -> BoxFuture<Result<Value>>;
        let methods: [(&str, Method); 3] = [
            ("tasks/get", |m, c, p| Box::pin(m.get(c, p))),
            ("tasks/update", |m, c, p| Box::pin(m.update(c, p))),
            ("tasks/cancel", |m, c, p| Box::pin(m.cancel(c, p))),
        ];
        for (name, method) in methods {
            let m = manager.clone();
            let handler: RequestFn = Arc::new(move |ctx, params| method(m.clone(), ctx, params));
            self.config.requests.insert(name.to_string(), handler);
        }
        manager
    }

    /// Enable the tasks extension (`io.modelcontextprotocol/tasks`): declare
    /// it, and answer `tasks/get`, `tasks/update` and `tasks/cancel`.
    /// Registering a task tool does this too, with the default settings.
    pub fn tasks(mut self, config: TaskConfig) -> Self {
        *self.task_manager().config.write().unwrap() = config;
        self
    }

    /// Add a tool whose calls become tasks when the client supports them
    /// ([`TaskMode::Optional`]; change it with [`ServerBuilder::task_mode`]).
    /// `handler` gets a [`TaskContext`] and the raw arguments object, and
    /// returns what a tool handler does: a [`ToolError::protocol`] fails the
    /// task, other errors complete it with an `isError` result.
    pub fn task_tool<F, Fut, R>(mut self, tool: Tool, handler: F) -> Self
    where
        F: Fn(TaskContext, JsonObject) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        self.task_manager().modes.write().unwrap().insert(tool.name.clone(), TaskMode::Optional);
        self.tool(tool, as_tool_handler(task_tool_fn(handler)))
    }

    /// Add a task tool taking typed arguments (see
    /// [`ServerBuilder::typed_tool`] and [`ServerBuilder::task_tool`]).
    /// Arguments that don't deserialize complete the task with an `isError`
    /// result.
    #[cfg(feature = "schemars")]
    pub fn typed_task_tool<A, F, Fut, R>(mut self, tool: Tool, handler: F) -> Self
    where
        A: DeserializeOwned + schemars::JsonSchema + Send + 'static,
        F: Fn(TaskContext, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        self.task_manager().modes.write().unwrap().insert(tool.name.clone(), TaskMode::Optional);
        self.typed_tool(tool, move |ctx, args: A| handler(task_context(ctx), args))
    }

    /// Set when calls of the task tool `name` become tasks.
    pub fn task_mode(mut self, name: impl Into<String>, mode: TaskMode) -> Self {
        self.task_manager().modes.write().unwrap().insert(name.into(), mode);
        self
    }
}

impl Server {
    fn tasks_or_panic(&self) -> &Arc<TaskManager> {
        self.inner.config.tasks.as_ref().expect("the tasks extension is not enabled: use ServerBuilder::tasks")
    }

    /// Add a task tool, or replace the tool with the same name. See
    /// [`ServerBuilder::task_tool`].
    ///
    /// # Panics
    ///
    /// If the server was built without the tasks extension.
    pub fn add_task_tool<F, Fut, R>(&self, tool: Tool, handler: F)
    where
        F: Fn(TaskContext, JsonObject) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
        R: IntoToolResult,
    {
        self.tasks_or_panic().modes.write().unwrap().insert(tool.name.clone(), TaskMode::Optional);
        self.add_tool(tool, as_tool_handler(task_tool_fn(handler)));
    }

    /// Set when calls of the tool `name` become tasks. Only for tools added
    /// as task tools: others don't get a [`TaskContext`].
    ///
    /// # Panics
    ///
    /// If the server was built without the tasks extension.
    pub fn set_task_mode(&self, name: impl Into<String>, mode: TaskMode) {
        self.tasks_or_panic().modes.write().unwrap().insert(name.into(), mode);
    }

    /// Remove a task tool. Returns whether it existed.
    pub fn remove_task_tool(&self, name: &str) -> bool {
        if let Some(m) = &self.inner.config.tasks {
            m.modes.write().unwrap().remove(name);
        }
        self.remove_tool(name)
    }

    /// A task's current state, if it exists and hasn't expired.
    pub async fn task(&self, task_id: &str) -> Option<Task> {
        self.inner.config.tasks.as_ref()?.lookup(task_id, "retrieve", None).await.ok()
    }

    /// Ask a running task to stop (it ends `cancelled`). Returns whether it
    /// was running in this process.
    pub fn cancel_task(&self, task_id: &str) -> bool {
        let Some(rt) = self.inner.config.tasks.as_ref().and_then(|m| m.running(task_id)) else {
            return false;
        };
        rt.cancel.cancel();
        true
    }

    /// Answer a `tools/call`: with a task handle when the tool is a task tool
    /// and the request allows it, else with the tool's result.
    pub(crate) async fn route_tool_call(&self, ctx: RequestContext, params: CallToolParams) -> Result<Value> {
        if let Some(manager) = &self.inner.config.tasks {
            let mode = manager.mode(&params.name);
            if mode != TaskMode::Never && self.tool_exists_for(ctx.session(), &params.name) {
                if client_supports_tasks(&ctx) {
                    return Ok(manager.start(self.clone(), ctx, params).await?);
                }
                if mode == TaskMode::Required {
                    return Err(missing_capability_error().into());
                }
            }
        }
        Ok(serde_json::to_value(self.call_tool(ctx, params).await?)?)
    }

    fn tool_exists_for(&self, session: &Session, name: &str) -> bool {
        let entry = self.inner.tools.read().unwrap().iter().find(|e| e.tool.name == name).cloned();
        entry.is_some_and(|e| self.tool_visible(session, &e.tool))
    }
}

/// Format a time as ISO 8601 UTC, with milliseconds.
fn format_time(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (y, m, day) = civil_from_days(secs.div_euclid(86400));
    let s = secs.rem_euclid(86400);
    format!("{y:04}-{m:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z", s / 3600, s / 60 % 60, s % 60, d.subsec_millis())
}

/// Parse an ISO 8601 UTC time: `YYYY-MM-DDTHH:MM:SS[.fff]` then `Z` or
/// `+00:00`.
fn parse_time(s: &str) -> Option<SystemTime> {
    let num = |from: usize, to: usize| s.get(from..to)?.parse::<u32>().ok();
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't') || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (y, mo, d, h, mi, sec) = (num(0, 4)?, num(5, 7)?, num(8, 10)?, num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let mut rest = &s[19..];
    let mut millis = 0u64;
    if let Some(frac) = rest.strip_prefix('.') {
        let digits = frac.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let padded = format!("{:0<3}", &frac[..digits.min(3)]);
        millis = padded.parse().ok()?;
        rest = &frac[digits..];
    }
    if !matches!(rest, "Z" | "z" | "+00:00") {
        return None;
    }
    let days = days_from_civil(y as i64, mo, d);
    let secs = days * 86400 + (h * 3600 + mi * 60 + sec) as i64;
    let secs = u64::try_from(secs).ok()?;
    Some(UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis))
}

// Date algorithms from Howard Hinnant's "chrono-Compatible Low-Level Date
// Algorithms".
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

impl Server {
    /// The part of a `subscriptions/listen` request's `taskIds` to honor.
    pub(crate) async fn watchable_tasks(&self, ctx: &RequestContext, ids: Option<Vec<String>>) -> Option<Vec<String>> {
        let manager = self.inner.config.tasks.as_ref()?;
        let ids = manager.watchable(ctx, ids?).await;
        (!ids.is_empty()).then_some(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_round_trip() {
        assert_eq!(format_time(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        let t = UNIX_EPOCH + Duration::from_millis(1_764_066_600_123);
        assert_eq!(format_time(t), "2025-11-25T10:30:00.123Z");
        assert_eq!(parse_time("2025-11-25T10:30:00.123Z"), Some(t));
        assert_eq!(parse_time("2025-11-25T10:30:00.1234567Z"), Some(t));
        assert_eq!(parse_time("2025-11-25T10:30:00Z"), Some(t - Duration::from_millis(123)));
        assert_eq!(parse_time("2024-02-29T00:00:00+00:00"), Some(UNIX_EPOCH + Duration::from_secs(1_709_164_800)));
        assert_eq!(parse_time("2025-11-25 10:30:00Z"), None);
        assert_eq!(parse_time("2025-11-25T10:30:00+02:00"), None);
        let now = SystemTime::now();
        let rounded = parse_time(&format_time(now)).unwrap();
        assert!(now.duration_since(rounded).unwrap() < Duration::from_millis(1));
    }

    #[test]
    fn task_wire_format() {
        let task = Task {
            task_id: "t1".into(),
            status: TaskStatus::InputRequired,
            status_message: None,
            created_at: "2025-11-25T10:30:00.000Z".into(),
            last_updated_at: "2025-11-25T10:30:00.000Z".into(),
            ttl_ms: None,
            poll_interval_ms: Some(500),
            input_requests: Some(JsonObject::new()),
            result: None,
            error: None,
            owner: Some("alice".into()),
        };
        assert_eq!(
            task.to_result("complete").unwrap(),
            json!({
                "resultType": "complete", "taskId": "t1", "status": "input_required",
                "createdAt": "2025-11-25T10:30:00.000Z", "lastUpdatedAt": "2025-11-25T10:30:00.000Z",
                "ttlMs": null, "pollIntervalMs": 500, "inputRequests": {}
            })
        );
        assert!(!task.is_expired());
        let expired = Task { ttl_ms: Some(1000), ..task };
        assert!(expired.is_expired());
    }

    #[test]
    fn declaration() {
        let meta = |v: Value| v.as_object().cloned();
        let caps = json!({ CLIENT_CAPABILITIES_META: { "extensions": { EXTENSION_ID: {} } } });
        assert!(declares_tasks(meta(caps.clone()).as_ref()));
        assert!(!declares_tasks(None));
        assert!(!declares_tasks(meta(json!({ CLIENT_CAPABILITIES_META: { "extensions": {} } })).as_ref()));
        let mut legacy = caps;
        legacy[PROTOCOL_VERSION_META] = "2025-11-25".into();
        assert!(!declares_tasks(meta(legacy.clone()).as_ref()));
        legacy[PROTOCOL_VERSION_META] = "2026-07-28".into();
        assert!(declares_tasks(meta(legacy).as_ref()));
    }
}
