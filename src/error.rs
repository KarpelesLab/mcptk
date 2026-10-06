use crate::jsonrpc::ErrorObject;
use std::fmt;

/// Errors from mcptk.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A JSON-RPC error, sent by the peer or to be sent to it.
    #[error("{0}")]
    Rpc(ErrorObject),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// The session or its transport is gone.
    #[error("session closed")]
    Closed,
    /// The request was cancelled.
    #[error("request cancelled")]
    Cancelled,
    /// The client didn't declare the capability this needs.
    #[error("client does not support {0}")]
    Unsupported(&'static str),
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Error::Rpc(ErrorObject::invalid_params(message))
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Error::Rpc(ErrorObject::internal(message))
    }

    pub fn resource_not_found(uri: &str) -> Self {
        Error::Rpc(ErrorObject::resource_not_found(uri))
    }

    /// The JSON-RPC error to answer a request with.
    pub fn to_error_object(&self) -> ErrorObject {
        match self {
            Error::Rpc(e) => e.clone(),
            Error::Json(e) => ErrorObject::invalid_params(e.to_string()),
            other => ErrorObject::internal(other.to_string()),
        }
    }
}

impl From<ErrorObject> for Error {
    fn from(e: ErrorObject) -> Self {
        Error::Rpc(e)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error a tool handler returns.
///
/// By default it is a tool execution error: the call "succeeds" at the
/// protocol level with `isError: true` and the message as content, so the
/// model can see what went wrong and adjust. Use [`ToolError::protocol`] for
/// a JSON-RPC error instead.
///
/// Any `std::error::Error` converts into it, so `?` works in handlers.
pub struct ToolError(ToolErrorKind);

enum ToolErrorKind {
    Execution(String),
    Protocol(ErrorObject),
}

impl ToolError {
    /// A tool execution error with this message.
    pub fn msg(message: impl fmt::Display) -> Self {
        ToolError(ToolErrorKind::Execution(message.to_string()))
    }

    /// A JSON-RPC error instead of an `isError` result.
    pub fn protocol(error: impl Into<ErrorObject>) -> Self {
        ToolError(ToolErrorKind::Protocol(error.into()))
    }

    pub(crate) fn into_result(self) -> std::result::Result<crate::types::CallToolResult, ErrorObject> {
        match self.0 {
            ToolErrorKind::Execution(msg) => Ok(crate::types::CallToolResult::error(msg)),
            ToolErrorKind::Protocol(e) => Err(e),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for ToolError {
    fn from(e: E) -> Self {
        ToolError::msg(e)
    }
}

impl fmt::Debug for ToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ToolErrorKind::Execution(m) => write!(f, "ToolError({m:?})"),
            ToolErrorKind::Protocol(e) => write!(f, "ToolError::Protocol({e:?})"),
        }
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ToolErrorKind::Execution(m) => f.write_str(m),
            ToolErrorKind::Protocol(e) => e.fmt(f),
        }
    }
}
