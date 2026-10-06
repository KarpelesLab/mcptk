//! Multi round-trip requests (MRTR, protocol 2026-07-28): asking the client
//! for input by answering a request with `resultType: "input_required"`, and
//! reading the answers when it retries.

use crate::error::{Error, Result};
use crate::types::{ClientCapabilities, CreateMessageParams, ElicitParams, JsonObject};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Prefix of the `requestState` mcptk writes for [`RequestContext::elicit`]
/// and friends.
///
/// [`RequestContext::elicit`]: crate::RequestContext::elicit
const STATE_PREFIX: &str = "mcptk1:";

/// A request for more input from the client: the answer to a `tools/call`,
/// `prompts/get` or `resources/read` that can't complete yet.
///
/// Return it from a handler (`Err(InputRequired::new()...)?`, or
/// [`ToolError::input_required`](crate::ToolError::input_required) in tools).
/// mcptk then:
///
/// - on 2026-07-28 requests, answers with an `input_required` result. The
///   client fulfils `inputRequests` and retries the request with
///   `inputResponses` and `requestState`, which the handler reads with
///   [`RequestContext::input_response`](crate::RequestContext::input_response)
///   and [`RequestContext::request_state`](crate::RequestContext::request_state).
/// - on handshake sessions, sends each input request to the client itself
///   (`elicitation/create`, `sampling/createMessage`, `roots/list`) and runs
///   the handler again with the answers, the same way.
///
/// Either way the handler runs again from the start, so it must not have
/// done anything it can't repeat before asking. The client must have
/// declared the capability for each request (elicitation, sampling, roots),
/// or the request fails with a `MissingRequiredClientCapability` error.
///
/// `requestState` goes through the client: if it matters for authorization
/// or business logic, protect it (e.g. with an HMAC) and check it.
///
/// For one question at a time, [`RequestContext::elicit`](crate::RequestContext::elicit),
/// `create_message` and `list_roots` do all of this for you.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InputRequired {
    /// Requests for the client, by key: `{ "method": ..., "params": ... }`.
    pub input_requests: JsonObject,
    /// Opaque state the client sends back on its retry.
    pub request_state: Option<String>,
}

impl InputRequired {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask the user for input (`elicitation/create`). The answer comes back
    /// as an [`ElicitResult`](crate::types::ElicitResult) under `key`.
    pub fn elicit(self, key: impl Into<String>, params: ElicitParams) -> Self {
        self.request(key, "elicitation/create", serde_json::to_value(params).unwrap_or_default())
    }

    /// Ask the client's LLM for a completion (`sampling/createMessage`). The
    /// answer comes back as a [`CreateMessageResult`](crate::types::CreateMessageResult).
    pub fn create_message(self, key: impl Into<String>, params: CreateMessageParams) -> Self {
        self.request(key, "sampling/createMessage", serde_json::to_value(params).unwrap_or_default())
    }

    /// Ask for the client's roots (`roots/list`). The answer comes back as a
    /// [`ListRootsResult`](crate::types::ListRootsResult).
    pub fn list_roots(self, key: impl Into<String>) -> Self {
        self.request(key, "roots/list", json!({}))
    }

    /// Add an input request of any method.
    pub fn request(mut self, key: impl Into<String>, method: &str, params: Value) -> Self {
        self.input_requests.insert(key.into(), json!({ "method": method, "params": params }));
        self
    }

    /// Set the `requestState` the client sends back.
    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.request_state = Some(state.into());
        self
    }

    /// The capabilities the input requests need that `caps` lacks, as a
    /// `ClientCapabilities` object, if any.
    pub(crate) fn missing_capabilities(&self, caps: Option<&ClientCapabilities>) -> Option<Value> {
        let mut missing = JsonObject::new();
        for req in self.input_requests.values() {
            let (name, has) = match req.get("method").and_then(Value::as_str) {
                Some("elicitation/create") => ("elicitation", caps.is_some_and(|c| c.elicitation.is_some())),
                Some("sampling/createMessage") => ("sampling", caps.is_some_and(|c| c.sampling.is_some())),
                Some("roots/list") => ("roots", caps.is_some_and(|c| c.roots.is_some())),
                _ => continue,
            };
            if !has {
                missing.insert(name.into(), json!({}));
            }
        }
        (!missing.is_empty()).then_some(Value::Object(missing))
    }

    /// The `input_required` result (without `_meta`).
    pub(crate) fn into_result(mut self) -> Value {
        let mut result = json!({ "resultType": "input_required" });
        self.input_requests.values_mut().for_each(for_stateless);
        if !self.input_requests.is_empty() {
            result["inputRequests"] = Value::Object(self.input_requests);
        }
        if let Some(state) = self.request_state {
            result["requestState"] = state.into();
        }
        result
    }
}

/// Adapt an input request (`{ "method", "params" }`) to 2026-07-28: URL
/// mode elicitations lost their `elicitationId` there, as the client learns
/// the outcome by retrying, not from a completion notification.
pub(crate) fn for_stateless(request: &mut Value) {
    if request["method"] == "elicitation/create"
        && let Some(params) = request.get_mut("params").and_then(Value::as_object_mut)
    {
        params.remove("elicitationId");
    }
}

/// The client's answers available to one run of a handler.
#[derive(Default)]
pub(crate) struct InputState {
    /// `inputResponses`, as sent.
    responses: Option<JsonObject>,
    /// `requestState`, as sent.
    state: Option<String>,
    /// Answers to automatic input requests: those carried in our
    /// `requestState`, plus `inputResponses`.
    known: JsonObject,
    /// Automatic input requests made so far in this run.
    next: AtomicUsize,
}

impl InputState {
    pub(crate) fn new(responses: Option<JsonObject>, state: Option<String>) -> Self {
        let mut known = state
            .as_deref()
            .and_then(|s| s.strip_prefix(STATE_PREFIX))
            .and_then(|s| serde_json::from_str::<JsonObject>(s).ok())
            .unwrap_or_default();
        known.extend(responses.iter().flatten().map(|(k, v)| (k.clone(), v.clone())));
        InputState { responses, state, known, next: AtomicUsize::new(0) }
    }

    /// Read `inputResponses` and `requestState` from request params.
    pub(crate) fn from_params(params: Option<&Value>) -> Self {
        let responses = params.and_then(|p| p.get("inputResponses")).and_then(Value::as_object).cloned();
        let state = params.and_then(|p| p.get("requestState")).and_then(Value::as_str).map(str::to_string);
        Self::new(responses, state)
    }

    pub(crate) fn responses(&self) -> Option<&JsonObject> {
        self.responses.as_ref()
    }

    pub(crate) fn state(&self) -> Option<&str> {
        self.state.as_deref()
    }

    pub(crate) fn response<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.responses.as_ref().and_then(|r| r.get(key)) {
            Some(v) => Ok(Some(serde_json::from_value(v.clone())?)),
            None => Ok(None),
        }
    }

    /// The answer to the next automatic input request, or the
    /// [`InputRequired`] that asks for it. Keys are numbered in call order,
    /// so a handler must make the same requests in the same order each run.
    pub(crate) fn next<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let key = format!("mcptk-{}", self.next.fetch_add(1, Ordering::Relaxed));
        if let Some(answer) = self.known.get(&key) {
            return serde_json::from_value(answer.clone())
                .map_err(|e| Error::invalid_params(format!("invalid input response {key:?}: {e}")));
        }
        let mut input = InputRequired::new().request(key, method, params);
        if !self.known.is_empty() {
            input = input.state(format!("{STATE_PREFIX}{}", Value::Object(self.known.clone())));
        }
        Err(Error::InputRequired(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_requests_carry_earlier_answers() {
        let first = InputState::new(None, None);
        let Err(Error::InputRequired(ask)) = first.next::<Value>("roots/list", json!({})) else { panic!() };
        assert!(ask.request_state.is_none());
        assert!(ask.input_requests.contains_key("mcptk-0"));

        let answers = json!({"mcptk-0": {"roots": []}}).as_object().cloned();
        let second = InputState::new(answers, None);
        assert_eq!(second.next::<Value>("roots/list", json!({})).unwrap(), json!({"roots": []}));
        let Err(Error::InputRequired(ask)) = second.next::<Value>("roots/list", json!({})) else { panic!() };
        assert!(ask.input_requests.contains_key("mcptk-1"));

        let third = InputState::new(json!({"mcptk-1": 2}).as_object().cloned(), ask.request_state);
        assert_eq!(third.next::<Value>("x", json!({})).unwrap(), json!({"roots": []}));
        assert_eq!(third.next::<i32>("x", json!({})).unwrap(), 2);
    }

    #[test]
    fn url_elicitations_lose_their_id_on_stateless_requests() {
        let ask = InputRequired::new().elicit("go", ElicitParams::url("Sign in", "https://example.com/in", "e1"));
        assert_eq!(
            ask.into_result()["inputRequests"]["go"],
            json!({"method": "elicitation/create", "params": {"mode": "url", "message": "Sign in", "url": "https://example.com/in"}})
        );
    }

    #[test]
    fn missing_capabilities() {
        let ask = InputRequired::new().elicit("a", ElicitParams::form("hi", json!({"type": "object"}))).list_roots("b");
        let caps = ClientCapabilities { roots: Some(Default::default()), ..Default::default() };
        assert_eq!(ask.missing_capabilities(Some(&caps)), Some(json!({"elicitation": {}})));
        assert_eq!(ask.missing_capabilities(None), Some(json!({"elicitation": {}, "roots": {}})));
    }
}
