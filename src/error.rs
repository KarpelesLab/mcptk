use crate::jsonrpc::ErrorObject;
use crate::server::InputRequired;
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
    /// The request needs more input from the client (multi round-trip
    /// requests). Returned by `RequestContext::elicit` and friends on
    /// 2026-07-28 requests, and by handlers to ask for input; see
    /// [`InputRequired`].
    #[error("client input required")]
    InputRequired(InputRequired),
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
            Error::InputRequired(_) => {
                ErrorObject::internal("client input required, but this request can't ask for it")
            }
            other => ErrorObject::internal(other.to_string()),
        }
    }
}

impl From<ErrorObject> for Error {
    fn from(e: ErrorObject) -> Self {
        Error::Rpc(e)
    }
}

impl From<InputRequired> for Error {
    fn from(input: InputRequired) -> Self {
        Error::InputRequired(input)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error a tool handler returns.
///
/// By default it is a tool execution error: the call "succeeds" at the
/// protocol level with `isError: true` and the message as content, so the
/// model can see what went wrong and adjust. Use [`ToolError::protocol`] for
/// a JSON-RPC error instead, and [`ToolError::input_required`] to ask the
/// client for input (multi round-trip requests).
///
/// Any `std::error::Error` converts into it, so `?` works in handlers. An
/// [`Error::InputRequired`] (from `ctx.elicit(...)?` on a 2026-07-28 request)
/// stays an input request.
pub struct ToolError(ToolErrorKind);

enum ToolErrorKind {
    Execution(String),
    Protocol(ErrorObject),
    InputRequired(InputRequired),
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

    /// Ask the client for input, then run the tool again with the answers
    /// (see [`InputRequired`]).
    pub fn input_required(input: InputRequired) -> Self {
        ToolError(ToolErrorKind::InputRequired(input))
    }

    pub(crate) fn into_result(self) -> std::result::Result<crate::types::CallToolResult, Error> {
        match self.0 {
            ToolErrorKind::Execution(msg) => Ok(crate::types::CallToolResult::error(msg)),
            ToolErrorKind::Protocol(e) => Err(Error::Rpc(e)),
            ToolErrorKind::InputRequired(input) => Err(Error::InputRequired(input)),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for ToolError {
    fn from(e: E) -> Self {
        // Keep input requests (`ctx.elicit(...)?`) as such.
        let mut slot = Some(e);
        if let Some(err) = (&mut slot as &mut dyn std::any::Any).downcast_mut::<Option<Error>>()
            && let Some(Error::InputRequired(input)) = err.take_if(|e| matches!(e, Error::InputRequired(_)))
        {
            return ToolError::input_required(input);
        }
        match slot {
            Some(e) => ToolError::msg(e),
            None => unreachable!("only taken for input requests"),
        }
    }
}

impl From<InputRequired> for ToolError {
    fn from(input: InputRequired) -> Self {
        ToolError::input_required(input)
    }
}

impl fmt::Debug for ToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ToolErrorKind::Execution(m) => write!(f, "ToolError({m:?})"),
            ToolErrorKind::Protocol(e) => write!(f, "ToolError::Protocol({e:?})"),
            ToolErrorKind::InputRequired(i) => write!(f, "ToolError::InputRequired({i:?})"),
        }
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ToolErrorKind::Execution(m) => f.write_str(m),
            ToolErrorKind::Protocol(e) => e.fmt(f),
            ToolErrorKind::InputRequired(_) => f.write_str("client input required"),
        }
    }
}
