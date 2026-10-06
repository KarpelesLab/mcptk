//! JSON-RPC 2.0 messages, the wire format of MCP.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt;

/// Invalid JSON was received.
pub const PARSE_ERROR: i64 = -32700;
/// The JSON sent is not a valid request object.
pub const INVALID_REQUEST: i64 = -32600;
/// The method does not exist or is not available.
pub const METHOD_NOT_FOUND: i64 = -32601;
/// Invalid method parameters.
pub const INVALID_PARAMS: i64 = -32602;
/// Internal JSON-RPC error.
pub const INTERNAL_ERROR: i64 = -32603;
/// MCP: the requested resource does not exist.
pub const RESOURCE_NOT_FOUND: i64 = -32002;
/// MCP (2025-11-25+): the request needs URL mode elicitations completed first.
pub const URL_ELICITATION_REQUIRED: i64 = -32042;

/// A request id: a string or an integer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Number(n) => write!(f, "{n}"),
            RequestId::String(s) => f.write_str(s),
        }
    }
}

impl From<i64> for RequestId {
    fn from(n: i64) -> Self {
        RequestId::Number(n)
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        RequestId::String(s)
    }
}

impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        RequestId::String(s.to_string())
    }
}

/// The `error` member of an error response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl ErrorObject {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        ErrorObject { code, message: message.into(), data: None }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::new(PARSE_ERROR, message)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(INVALID_REQUEST, message)
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(METHOD_NOT_FOUND, format!("method not found: {method}"))
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(INTERNAL_ERROR, message)
    }

    pub fn resource_not_found(uri: &str) -> Self {
        Self::new(RESOURCE_NOT_FOUND, "resource not found").with_data(serde_json::json!({ "uri": uri }))
    }

    /// The request can't go on until the user completes these URL mode
    /// elicitations; the client may retry it after that (2025-11-25+).
    pub fn url_elicitation_required(
        message: impl Into<String>,
        elicitations: Vec<crate::types::ElicitUrlParams>,
    ) -> Self {
        Self::new(URL_ELICITATION_REQUIRED, message).with_data(serde_json::json!({ "elicitations": elicitations }))
    }
}

impl fmt::Display for ErrorObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for ErrorObject {}

/// A request, which expects a response.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub id: RequestId,
    pub method: String,
    pub params: Option<Value>,
}

/// A notification, which gets no response.
#[derive(Clone, Debug, PartialEq)]
pub struct Notification {
    pub method: String,
    pub params: Option<Value>,
}

/// A successful response.
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    pub id: RequestId,
    pub result: Value,
}

/// An error response. `id` is `None` when the request id could not be read.
#[derive(Clone, Debug, PartialEq)]
pub struct ErrorResponse {
    pub id: Option<RequestId>,
    pub error: ErrorObject,
}

/// Any JSON-RPC message.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    Request(Request),
    Notification(Notification),
    Response(Response),
    Error(ErrorResponse),
}

impl Message {
    pub fn request(id: impl Into<RequestId>, method: impl Into<String>, params: Option<Value>) -> Self {
        Message::Request(Request { id: id.into(), method: method.into(), params })
    }

    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        Message::Notification(Notification { method: method.into(), params })
    }

    pub fn response(id: RequestId, result: Value) -> Self {
        Message::Response(Response { id, result })
    }

    pub fn error(id: Option<RequestId>, error: ErrorObject) -> Self {
        Message::Error(ErrorResponse { id, error })
    }

    /// The method, for requests and notifications.
    pub fn method(&self) -> Option<&str> {
        match self {
            Message::Request(r) => Some(&r.method),
            Message::Notification(n) => Some(&n.method),
            _ => None,
        }
    }

    /// The id of a response or error response.
    pub fn response_id(&self) -> Option<&RequestId> {
        match self {
            Message::Response(r) => Some(&r.id),
            Message::Error(e) => e.id.as_ref(),
            _ => None,
        }
    }
}

/// One or several messages, as found in a single JSON text.
#[derive(Clone, Debug, PartialEq)]
pub enum Payload {
    Single(Message),
    Batch(Vec<Message>),
}

impl Payload {
    /// Parse a JSON text holding a message or a batch of messages.
    pub fn parse(text: &[u8]) -> Result<Payload, serde_json::Error> {
        let value: Value = serde_json::from_slice(text)?;
        match value {
            Value::Array(items) => {
                Ok(Payload::Batch(items.into_iter().map(serde_json::from_value).collect::<Result<_, _>>()?))
            }
            other => Ok(Payload::Single(serde_json::from_value(other)?)),
        }
    }

    pub fn into_vec(self) -> Vec<Message> {
        match self {
            Payload::Single(m) => vec![m],
            Payload::Batch(v) => v,
        }
    }
}

/// Decode incoming JSON text: each message, or the error response to send
/// back for it (invalid JSON, or a value that isn't a message).
pub(crate) fn decode(text: &[u8]) -> Vec<Result<Message, Message>> {
    let value: Value = match serde_json::from_slice(text) {
        Ok(v) => v,
        Err(e) => return vec![Err(Message::error(None, ErrorObject::parse_error(e.to_string())))],
    };
    let one = |v: Value| {
        let id = v.get("id").cloned().and_then(|id| serde_json::from_value(id).ok());
        serde_json::from_value(v).map_err(|e| Message::error(id, ErrorObject::invalid_request(e.to_string())))
    };
    match value {
        Value::Array(items) if items.is_empty() => {
            vec![Err(Message::error(None, ErrorObject::invalid_request("empty batch")))]
        }
        Value::Array(items) => items.into_iter().map(one).collect(),
        other => vec![one(other)],
    }
}

#[derive(Serialize, Deserialize)]
struct Raw {
    jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "nullable")]
    id: Option<Option<RequestId>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<ErrorObject>,
}

// Tells a missing `id` (None) from `"id": null` (Some(None)).
fn nullable<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<RequestId>>, D::Error> {
    Option::<RequestId>::deserialize(d).map(Some)
}

impl Serialize for Message {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut raw = Raw { jsonrpc: "2.0".into(), id: None, method: None, params: None, result: None, error: None };
        match self {
            Message::Request(r) => {
                raw.id = Some(Some(r.id.clone()));
                raw.method = Some(r.method.clone());
                raw.params = r.params.clone();
            }
            Message::Notification(n) => {
                raw.method = Some(n.method.clone());
                raw.params = n.params.clone();
            }
            Message::Response(r) => {
                raw.id = Some(Some(r.id.clone()));
                raw.result = Some(r.result.clone());
            }
            Message::Error(e) => {
                raw.id = Some(e.id.clone());
                raw.error = Some(e.error.clone());
            }
        }
        raw.serialize(s)
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Raw::deserialize(d)?;
        if raw.jsonrpc != "2.0" {
            return Err(D::Error::custom("unsupported jsonrpc version"));
        }
        match (raw.id, raw.method, raw.result, raw.error) {
            (Some(Some(id)), Some(method), None, None) => {
                Ok(Message::Request(Request { id, method, params: raw.params }))
            }
            (None, Some(method), None, None) => Ok(Message::Notification(Notification { method, params: raw.params })),
            (Some(Some(id)), None, Some(result), None) => Ok(Message::Response(Response { id, result })),
            (id, None, None, Some(error)) => Ok(Message::Error(ErrorResponse { id: id.flatten(), error })),
            _ => Err(D::Error::custom("not a valid JSON-RPC message")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips() {
        let cases = [
            json!({"jsonrpc":"2.0","id":1,"method":"ping"}),
            json!({"jsonrpc":"2.0","id":"a","method":"tools/call","params":{"name":"x"}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"result":{}}),
            json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"bad"}}),
        ];
        for case in cases {
            let msg: Message = serde_json::from_value(case.clone()).unwrap();
            assert_eq!(serde_json::to_value(&msg).unwrap(), case);
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(serde_json::from_value::<Message>(json!({"jsonrpc":"2.0"})).is_err());
        assert!(serde_json::from_value::<Message>(json!({"jsonrpc":"1.0","method":"x"})).is_err());
        assert!(serde_json::from_value::<Message>(json!({"jsonrpc":"2.0","id":null,"method":"x"})).is_err());
    }

    #[test]
    fn parses_batches() {
        let p = Payload::parse(br#"[{"jsonrpc":"2.0","method":"a"},{"jsonrpc":"2.0","id":1,"method":"b"}]"#).unwrap();
        assert_eq!(p.into_vec().len(), 2);
    }
}
