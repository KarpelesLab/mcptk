//! Authorization: serve an MCP server as an OAuth 2.1 protected resource.
//!
//! Over Streamable HTTP, an MCP server can require an OAuth access token on
//! every request, as described by the MCP authorization specification. The
//! server is a *resource server*: it does not issue tokens. An authorization
//! server (your identity provider) does, and the MCP server only checks them.
//!
//! What mcptk does, once [`StreamableHttp::auth`](crate::http::StreamableHttp::auth)
//! is given a [`ProtectedResource`]:
//!
//! - Serves the OAuth 2.0 Protected Resource Metadata document (RFC 9728) at
//!   `/.well-known/oauth-protected-resource` and at the path-suffixed variant
//!   for the endpoint (`/.well-known/oauth-protected-resource/mcp` for
//!   `https://host/mcp`). It names your authorization servers, so clients
//!   such as Claude Code can find where to log in.
//! - Answers requests without a valid `Authorization: Bearer` token with
//!   `401` and a `WWW-Authenticate` challenge pointing at that document.
//! - Hands each token to your [`TokenValidator`], and makes the resulting
//!   [`AuthInfo`] available to handlers through
//!   [`RequestContext::auth`](crate::RequestContext::auth).
//! - Checks scopes (for every request, per tool, or per method) and answers
//!   `403` with `error="insufficient_scope"` when the token lacks some, so the
//!   client can ask the user for more (step-up authorization).
//! - Binds each protocol session to the subject that created it: a request
//!   carrying another subject's token gets `404 Session not found`.
//!
//! # Validating tokens
//!
//! mcptk ships no JWT or crypto code. Implement [`TokenValidator`] with the
//! library and policy of your choice: verify a JWT's signature against the
//! issuer's JWKS, or call the authorization server's introspection endpoint
//! (RFC 7662). Whatever the method, the validator **must**:
//!
//! - check the token is current (signature, expiry, revocation);
//! - check it was issued **for this server**: its audience (`aud`, or the
//!   introspection `aud`/resource) must be this server's resource URL
//!   ([`ProtectedResource::resource`]). Accepting tokens minted for other
//!   services lets one service's token be replayed against yours;
//! - return the scopes the token grants, expanded through any hierarchy your
//!   scopes have (if `files:write` implies `files:read`, return both), as
//!   mcptk compares scope strings exactly.
//!
//! Never forward the client's token to other services (token passthrough).
//! When a tool calls an upstream API, use a token issued for that API.
//!
//! [`StaticTokens`] maps fixed tokens to identities, for tests and
//! development.
//!
//! # Example
//!
//! ```no_run
//! use mcptk::auth::{AuthInfo, ProtectedResource, StaticTokens};
//! use mcptk::http::StreamableHttp;
//!
//! # async fn run(server: mcptk::Server) -> mcptk::Result<()> {
//! let tokens = StaticTokens::new().token("dev-token", AuthInfo::new("alice").scopes(["notes:read", "notes:write"]));
//! let resource = ProtectedResource::new("https://notes.example.com/mcp", tokens)
//!     .authorization_server("https://auth.example.com")
//!     .scopes_supported(["notes:read"])
//!     .require_scopes(["notes:read"])
//!     .tool_scopes("add_note", ["notes:write"]);
//! StreamableHttp::new(server).auth(resource).serve("127.0.0.1:8080").await
//! # }
//! ```

use crate::RequestContext;
use crate::error::ToolError;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

/// The well-known path of protected resource metadata (RFC 9728).
pub const METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// Who a request is authenticated as: what a [`TokenValidator`] makes of a
/// valid access token.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct AuthInfo {
    /// The resource owner (the token's `sub`). For tokens obtained with
    /// client credentials, use the client id.
    pub subject: String,
    /// The scopes the token grants, hierarchies expanded.
    pub scopes: Vec<String>,
    /// The OAuth client the token was issued to.
    pub client_id: Option<String>,
    /// When the token expires. Requests with an expired token get `401`.
    pub expires_at: Option<SystemTime>,
    /// Anything else the validator wants handlers to see (claims, tenant...).
    pub extra: Value,
}

impl AuthInfo {
    pub fn new(subject: impl Into<String>) -> Self {
        AuthInfo { subject: subject.into(), scopes: Vec::new(), client_id: None, expires_at: None, extra: Value::Null }
    }

    pub fn scopes<S: Into<String>>(mut self, scopes: impl IntoIterator<Item = S>) -> Self {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn expires_at(mut self, at: SystemTime) -> Self {
        self.expires_at = Some(at);
        self
    }

    pub fn extra(mut self, extra: Value) -> Self {
        self.extra = extra;
        self
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }

    /// Whether the token is past its expiry time.
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|at| at <= SystemTime::now())
    }
}

/// Why a token was refused.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthError {
    /// Unknown, expired, revoked, malformed, or issued for another resource:
    /// `401` with `error="invalid_token"`.
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// Valid, but it lacks these scopes: `403` with
    /// `error="insufficient_scope"`.
    #[error("insufficient scope: needs {}", .scopes.join(" "))]
    InsufficientScope { scopes: Vec<String>, description: Option<String> },
    /// The token couldn't be checked (introspection endpoint down...): `503`.
    #[error("token validation unavailable: {0}")]
    Unavailable(String),
}

impl AuthError {
    pub fn invalid_token(description: impl Into<String>) -> Self {
        AuthError::InvalidToken(description.into())
    }
}

/// Checks access tokens. See the [module docs](self) for what a validator
/// must verify, audience above all.
///
/// Implement it with an `async fn`:
///
/// ```
/// use mcptk::auth::{AuthError, AuthInfo, TokenValidator};
///
/// struct Introspect;
///
/// impl TokenValidator for Introspect {
///     async fn validate(&self, token: &str) -> Result<AuthInfo, AuthError> {
///         // Ask the authorization server, check `active` and `aud`...
///         Err(AuthError::invalid_token("unknown token"))
///     }
/// }
/// ```
///
/// Or wrap a closure with [`validator_fn`].
pub trait TokenValidator: Send + Sync + 'static {
    fn validate(&self, token: &str) -> impl Future<Output = Result<AuthInfo, AuthError>> + Send;
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`TokenValidator`], object safe.
trait DynValidator: Send + Sync {
    fn validate_dyn<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Result<AuthInfo, AuthError>>;
}

impl<T: TokenValidator> DynValidator for T {
    fn validate_dyn<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Result<AuthInfo, AuthError>> {
        Box::pin(self.validate(token))
    }
}

/// A [`TokenValidator`] made from a closure; see [`validator_fn`].
pub struct FnValidator<F>(F);

/// Make a [`TokenValidator`] from an async closure taking the token.
///
/// ```
/// use mcptk::auth::{AuthError, AuthInfo, validator_fn};
///
/// let validator = validator_fn(|token: String| async move {
///     match token.as_str() {
///         "letmein" => Ok(AuthInfo::new("alice")),
///         _ => Err(AuthError::invalid_token("unknown token")),
///     }
/// });
/// ```
pub fn validator_fn<F, Fut>(f: F) -> FnValidator<F>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<AuthInfo, AuthError>> + Send + 'static,
{
    FnValidator(f)
}

impl<F, Fut> TokenValidator for FnValidator<F>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<AuthInfo, AuthError>> + Send + 'static,
{
    fn validate(&self, token: &str) -> impl Future<Output = Result<AuthInfo, AuthError>> + Send {
        (self.0)(token.to_string())
    }
}

/// A validator accepting a fixed set of tokens, for tests and development.
/// Don't use it in production: such tokens are not bound to an audience and
/// never expire.
#[derive(Clone, Debug, Default)]
pub struct StaticTokens {
    tokens: Vec<(String, AuthInfo)>,
}

impl StaticTokens {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept `token`, as `info`.
    pub fn token(mut self, token: impl Into<String>, info: AuthInfo) -> Self {
        self.tokens.push((token.into(), info));
        self
    }
}

/// Compare without stopping at the first difference.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl TokenValidator for StaticTokens {
    async fn validate(&self, token: &str) -> Result<AuthInfo, AuthError> {
        let mut found = None;
        for (t, info) in &self.tokens {
            if constant_time_eq(t.as_bytes(), token.as_bytes()) {
                found = Some(info);
            }
        }
        found.cloned().ok_or_else(|| AuthError::invalid_token("unknown token"))
    }
}

type ScopePolicy = Arc<dyn Fn(&crate::jsonrpc::Request) -> Vec<String> + Send + Sync>;

/// How an MCP server is protected: its identity as an OAuth resource, its
/// authorization servers, the scopes it wants, and the token validator.
/// Give it to [`StreamableHttp::auth`](crate::http::StreamableHttp::auth).
#[derive(Clone)]
pub struct ProtectedResource {
    resource: String,
    metadata_url: String,
    authorization_servers: Vec<String>,
    scopes_supported: Vec<String>,
    required_scopes: Vec<String>,
    tool_scopes: Vec<(String, Vec<String>)>,
    method_scopes: Vec<(String, Vec<String>)>,
    policy: Option<ScopePolicy>,
    metadata_fields: serde_json::Map<String, Value>,
    validator: Arc<dyn DynValidator>,
}

impl std::fmt::Debug for ProtectedResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtectedResource")
            .field("resource", &self.resource)
            .field("metadata_url", &self.metadata_url)
            .field("authorization_servers", &self.authorization_servers)
            .field("required_scopes", &self.required_scopes)
            .finish_non_exhaustive()
    }
}

/// Split an absolute URL into its origin and path, dropping the query, the
/// fragment and a trailing slash.
fn split_url(url: &str) -> Option<(&str, &str)> {
    let url = url.split(['#', '?']).next().unwrap_or(url);
    let scheme_end = url.find("://")? + 3;
    let path_start = url[scheme_end..].find('/').map_or(url.len(), |i| scheme_end + i);
    Some((&url[..path_start], url[path_start..].trim_end_matches('/')))
}

impl ProtectedResource {
    /// `resource` is the server's canonical URL, the one clients connect to
    /// and ask tokens for (e.g. `https://mcp.example.com/mcp`, without a
    /// trailing slash). Tokens must be issued for it.
    ///
    /// # Panics
    ///
    /// If `resource` is not an absolute URL.
    pub fn new(resource: impl Into<String>, validator: impl TokenValidator) -> Self {
        let resource = resource.into();
        let Some((origin, path)) = split_url(&resource) else {
            panic!("ProtectedResource: {resource:?} is not an absolute URL");
        };
        let metadata_url = format!("{origin}{METADATA_PATH}{path}");
        ProtectedResource {
            resource,
            metadata_url,
            authorization_servers: Vec::new(),
            scopes_supported: Vec::new(),
            required_scopes: Vec::new(),
            tool_scopes: Vec::new(),
            method_scopes: Vec::new(),
            policy: None,
            metadata_fields: serde_json::Map::new(),
            validator: Arc::new(validator),
        }
    }

    /// Add an authorization server, by its issuer URL. At least one is
    /// required.
    pub fn authorization_server(mut self, issuer: impl Into<String>) -> Self {
        self.authorization_servers.push(issuer.into());
        self
    }

    /// The scopes advertised in the metadata (`scopes_supported`): the
    /// minimum for basic use, which clients request when not told otherwise.
    /// Also sent in `401` challenges when no [`require_scopes`](Self::require_scopes)
    /// are set. Don't include `offline_access`.
    pub fn scopes_supported<S: Into<String>>(mut self, scopes: impl IntoIterator<Item = S>) -> Self {
        self.scopes_supported = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// Scopes every request needs. Tokens lacking one get `403
    /// insufficient_scope`.
    pub fn require_scopes<S: Into<String>>(mut self, scopes: impl IntoIterator<Item = S>) -> Self {
        self.required_scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// Scopes needed to call the tool `name`, checked before the request is
    /// handled, so the client gets an HTTP `403 insufficient_scope` it can
    /// act on (step-up authorization).
    pub fn tool_scopes<S: Into<String>>(
        mut self,
        name: impl Into<String>,
        scopes: impl IntoIterator<Item = S>,
    ) -> Self {
        self.tool_scopes.push((name.into(), scopes.into_iter().map(Into::into).collect()));
        self
    }

    /// Scopes needed for requests of `method` (e.g. `resources/read`).
    pub fn method_scopes<S: Into<String>>(
        mut self,
        method: impl Into<String>,
        scopes: impl IntoIterator<Item = S>,
    ) -> Self {
        self.method_scopes.push((method.into(), scopes.into_iter().map(Into::into).collect()));
        self
    }

    /// Compute the scopes a request needs from its method and params, on top
    /// of the static ones.
    pub fn scope_policy<F>(mut self, policy: F) -> Self
    where
        F: Fn(&crate::jsonrpc::Request) -> Vec<String> + Send + Sync + 'static,
    {
        self.policy = Some(Arc::new(policy));
        self
    }

    /// A human-readable name for the metadata (`resource_name`).
    pub fn resource_name(self, name: impl Into<String>) -> Self {
        self.metadata_field("resource_name", name.into())
    }

    /// A page documenting the server (`resource_documentation`).
    pub fn resource_documentation(self, url: impl Into<String>) -> Self {
        self.metadata_field("resource_documentation", url.into())
    }

    /// Set any other field of the metadata document.
    pub fn metadata_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.metadata_fields.insert(key.into(), value.into());
        self
    }

    /// Where the metadata document is published, if not at the URL derived
    /// from the resource (e.g. behind a proxy rewriting paths).
    pub fn metadata_url(mut self, url: impl Into<String>) -> Self {
        self.metadata_url = url.into();
        self
    }

    /// The resource URL; tokens must be issued for it.
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// The authorization servers' issuer URLs.
    pub fn authorization_servers(&self) -> &[String] {
        &self.authorization_servers
    }

    /// The metadata URL sent in `WWW-Authenticate` challenges.
    pub fn metadata_location(&self) -> &str {
        &self.metadata_url
    }

    /// The paths [`StreamableHttp::handle`](crate::http::StreamableHttp::handle)
    /// serves the metadata on: the one from [`metadata_location`](Self::metadata_location)
    /// (e.g. `/.well-known/oauth-protected-resource/mcp`) and the root one.
    /// Route them to the handler if you mount it in your own router.
    pub fn metadata_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        if let Some((_, path)) = split_url(&self.metadata_url) {
            paths.push(if path.is_empty() { "/".to_string() } else { path.to_string() });
        }
        if !paths.iter().any(|p| p == METADATA_PATH) {
            paths.push(METADATA_PATH.to_string());
        }
        paths
    }

    /// The protected resource metadata document (RFC 9728).
    pub fn metadata(&self) -> Value {
        let mut doc = json!({
            "resource": self.resource,
            "authorization_servers": self.authorization_servers,
            "bearer_methods_supported": ["header"],
        });
        if !self.scopes_supported.is_empty() {
            doc["scopes_supported"] = json!(self.scopes_supported);
        }
        if let Value::Object(map) = &mut doc {
            map.extend(self.metadata_fields.clone());
        }
        doc
    }

    /// Validate a bearer token, as the HTTP transport does. Also checks the
    /// expiry the validator reported, and the scopes every request needs.
    pub async fn validate(&self, token: &str) -> Result<AuthInfo, AuthError> {
        let info = self.validator.validate_dyn(token).await?;
        if info.is_expired() {
            return Err(AuthError::invalid_token("token expired"));
        }
        let required = self.scopes_needed(std::iter::empty());
        if required.iter().any(|s| !info.has_scope(s)) {
            return Err(AuthError::InsufficientScope { scopes: required, description: None });
        }
        Ok(info)
    }

    /// Everything these requests need, required scopes first, without
    /// duplicates.
    fn scopes_needed<'a>(&self, requests: impl Iterator<Item = &'a crate::jsonrpc::Request>) -> Vec<String> {
        let mut scopes = self.required_scopes.clone();
        let mut add = |list: &[String]| {
            for s in list {
                if !scopes.contains(s) {
                    scopes.push(s.clone());
                }
            }
        };
        for req in requests {
            for (method, list) in &self.method_scopes {
                if *method == req.method {
                    add(list);
                }
            }
            if req.method == "tools/call" {
                let name = req.params.as_ref().and_then(|p| p.get("name")).and_then(Value::as_str);
                for (tool, list) in &self.tool_scopes {
                    if Some(tool.as_str()) == name {
                        add(list);
                    }
                }
            }
            if let Some(policy) = &self.policy {
                add(&policy(req));
            }
        }
        scopes
    }
}

tokio::task_local! {
    /// The identity of the HTTP request whose messages are being handled.
    static CURRENT: Option<Arc<AuthInfo>>;
}

/// The identity the transport set for the messages being dispatched; read
/// when a [`RequestContext`] is created.
pub(crate) fn current() -> Option<Arc<AuthInfo>> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

impl RequestContext {
    /// Who the request is authenticated as: set when served over HTTP with
    /// [`StreamableHttp::auth`](crate::http::StreamableHttp::auth). Each HTTP
    /// request is authenticated on its own, so this is per request, not per
    /// session.
    pub fn auth(&self) -> Option<&AuthInfo> {
        self.auth.as_deref()
    }

    /// Fail unless the request's token grants `scope`. Unauthenticated
    /// requests (stdio, or HTTP without auth) fail too.
    ///
    /// The error is a tool execution error, seen by the model. To have the
    /// client re-authorize with more scopes, declare them with
    /// [`ProtectedResource::tool_scopes`] instead: they are checked before
    /// the request runs, and answered with HTTP `403 insufficient_scope`.
    pub fn require_scope(&self, scope: &str) -> Result<(), ToolError> {
        self.require_scopes([scope])
    }

    /// Fail unless the request's token grants all of `scopes`.
    pub fn require_scopes<'a>(&self, scopes: impl IntoIterator<Item = &'a str>) -> Result<(), ToolError> {
        let scopes: Vec<&str> = scopes.into_iter().collect();
        match self.auth() {
            Some(info) if scopes.iter().all(|s| info.has_scope(s)) => Ok(()),
            Some(_) => Err(ToolError::msg(format!("insufficient scope: this needs {}", scopes.join(" ")))),
            None => Err(ToolError::msg("this needs an authenticated request")),
        }
    }
}

#[cfg(feature = "http")]
pub(crate) use http_glue::*;

/// Hooks for the Streamable HTTP transport.
#[cfg(feature = "http")]
mod http_glue {
    use super::*;
    use crate::http::McpBody;
    use crate::jsonrpc::{ErrorObject, Message};
    use crate::server::Session;
    use http::{Extensions, HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};

    /// The session owner, kept in the session's data.
    #[derive(Clone)]
    struct SessionOwner(String);

    fn response(status: StatusCode, body: Option<&Value>) -> Response<McpBody> {
        let mut res = match body {
            Some(v) => {
                let mut res = Response::new(McpBody::full(serde_json::to_vec(v).unwrap_or_default()));
                res.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
                res
            }
            None => Response::new(McpBody::full(bytes::Bytes::new())),
        };
        *res.status_mut() = status;
        res
    }

    fn with_cors(mut res: Response<McpBody>) -> Response<McpBody> {
        let h = res.headers_mut();
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, OPTIONS"));
        h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
        res
    }

    /// Make `s` fit in a quoted auth-param (RFC 6750 restricts the characters).
    fn quote(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                '"' | '\\' => '\'',
                ' '..='~' => c,
                _ => '?',
            })
            .collect()
    }

    impl ProtectedResource {
        /// The scopes to suggest in a `401` challenge.
        fn challenge_scopes(&self) -> &[String] {
            if self.required_scopes.is_empty() { &self.scopes_supported } else { &self.required_scopes }
        }

        /// A `WWW-Authenticate` value.
        pub(super) fn challenge(&self, error: Option<&str>, description: Option<&str>, scopes: &[String]) -> String {
            let mut params = Vec::new();
            if let Some(error) = error {
                params.push(format!("error=\"{}\"", quote(error)));
            }
            if let Some(d) = description {
                params.push(format!("error_description=\"{}\"", quote(d)));
            }
            params.push(format!("resource_metadata=\"{}\"", quote(&self.metadata_url)));
            if !scopes.is_empty() {
                params.push(format!("scope=\"{}\"", quote(&scopes.join(" "))));
            }
            format!("Bearer {}", params.join(", "))
        }

        /// A response carrying the metadata document, for when you route
        /// the well-known path yourself.
        pub fn metadata_response(&self) -> Response<McpBody> {
            let mut res = with_cors(response(StatusCode::OK, Some(&self.metadata())));
            res.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("max-age=3600"));
            res
        }

        /// Answer `req` if it is for the metadata document.
        pub(crate) fn serve_metadata<B>(&self, req: &Request<B>) -> Option<Response<McpBody>> {
            let path = req.uri().path();
            if !self.metadata_paths().iter().any(|p| p == path) {
                return None;
            }
            Some(match *req.method() {
                Method::GET => self.metadata_response(),
                Method::OPTIONS => with_cors(response(StatusCode::NO_CONTENT, None)),
                _ => {
                    let mut res = response(StatusCode::METHOD_NOT_ALLOWED, None);
                    res.headers_mut().insert(header::ALLOW, HeaderValue::from_static("GET, OPTIONS"));
                    res
                }
            })
        }

        /// An error response with a `WWW-Authenticate` challenge.
        fn refuse(
            &self,
            status: StatusCode,
            error: Option<&str>,
            description: Option<&str>,
            scopes: &[String],
        ) -> Response<McpBody> {
            let message = match description {
                Some(d) => format!("{}: {d}", status.canonical_reason().unwrap_or("Error")),
                None => status.canonical_reason().unwrap_or("Error").to_string(),
            };
            let body = serde_json::to_value(Message::error(None, ErrorObject::new(-32000, message))).ok();
            let mut res = response(status, body.as_ref());
            if let Ok(v) = HeaderValue::from_str(&self.challenge(error, description, scopes)) {
                res.headers_mut().insert(header::WWW_AUTHENTICATE, v);
            }
            res
        }

        fn refuse_with(&self, error: AuthError) -> Response<McpBody> {
            match error {
                AuthError::InvalidToken(d) => {
                    self.refuse(StatusCode::UNAUTHORIZED, Some("invalid_token"), Some(&d), self.challenge_scopes())
                }
                AuthError::InsufficientScope { scopes, description } => self.refuse(
                    StatusCode::FORBIDDEN,
                    Some("insufficient_scope"),
                    Some(description.as_deref().unwrap_or("the token lacks required scopes")),
                    &scopes,
                ),
                AuthError::Unavailable(d) => {
                    tracing::warn!("token validation unavailable: {d}");
                    let body = serde_json::to_value(Message::error(
                        None,
                        ErrorObject::new(-32000, "Service Unavailable: cannot validate token"),
                    ))
                    .ok();
                    response(StatusCode::SERVICE_UNAVAILABLE, body.as_ref())
                }
            }
        }

        /// Authenticate a request from its `Authorization` header.
        #[allow(clippy::result_large_err)]
        pub(crate) async fn authenticate(&self, headers: &HeaderMap) -> Result<Arc<AuthInfo>, Response<McpBody>> {
            let mut values = headers.get_all(header::AUTHORIZATION).iter();
            let (Some(value), None) = (values.next(), values.next()) else {
                return Err(match headers.contains_key(header::AUTHORIZATION) {
                    true => self.refuse(
                        StatusCode::BAD_REQUEST,
                        Some("invalid_request"),
                        Some("several Authorization headers"),
                        &[],
                    ),
                    false => self.refuse(StatusCode::UNAUTHORIZED, None, None, self.challenge_scopes()),
                });
            };
            let value = value.to_str().unwrap_or("");
            let (scheme, token) = value.split_once(' ').unwrap_or((value, ""));
            if !scheme.eq_ignore_ascii_case("bearer") {
                return Err(self.refuse(StatusCode::UNAUTHORIZED, None, None, self.challenge_scopes()));
            }
            let token = token.trim();
            if token.is_empty() || token.contains(' ') {
                return Err(self.refuse(
                    StatusCode::BAD_REQUEST,
                    Some("invalid_request"),
                    Some("malformed bearer token"),
                    &[],
                ));
            }
            self.validate(token).await.map(Arc::new).map_err(|e| self.refuse_with(e))
        }

        /// Check the scopes needed by the requests in a POST.
        #[allow(clippy::result_large_err)]
        pub(crate) fn check_scopes(&self, messages: &[Message], ext: &Extensions) -> Result<(), Response<McpBody>> {
            let Some(info) = ext.get::<Arc<AuthInfo>>() else { return Ok(()) };
            let requests = messages.iter().filter_map(|m| match m {
                Message::Request(r) => Some(r),
                _ => None,
            });
            let needed = self.scopes_needed(requests);
            if needed.iter().all(|s| info.has_scope(s)) {
                return Ok(());
            }
            Err(self.refuse_with(AuthError::InsufficientScope { scopes: needed, description: None }))
        }
    }

    /// The identity [`ProtectedResource::authenticate`] stored in a request.
    pub(crate) fn from_extensions(ext: &Extensions) -> Option<Arc<AuthInfo>> {
        ext.get::<Arc<AuthInfo>>().cloned()
    }

    /// Run `f` (which dispatches messages to a session) with `auth` as the
    /// identity that [`RequestContext::auth`] reports.
    pub(crate) fn scope<R>(auth: Option<Arc<AuthInfo>>, f: impl FnOnce() -> R) -> R {
        CURRENT.sync_scope(auth, f)
    }

    /// Record the authenticated subject as the owner of a new session.
    pub(crate) fn bind_session(session: &Session, ext: &Extensions) {
        if let Some(info) = ext.get::<Arc<AuthInfo>>() {
            session.set_data(SessionOwner(info.subject.clone()));
        }
    }

    /// Whether a request may use `session`: it carries the same subject as
    /// the one that created it.
    pub(crate) fn owns_session(session: &Session, ext: &Extensions) -> bool {
        match (session.data::<SessionOwner>(), ext.get::<Arc<AuthInfo>>()) {
            (Some(owner), Some(info)) => owner.0 == info.subject,
            (Some(_), None) => false,
            (None, _) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_location() {
        let v = StaticTokens::new();
        let pr = ProtectedResource::new("https://example.com/public/mcp/", v.clone());
        assert_eq!(pr.metadata_location(), "https://example.com/.well-known/oauth-protected-resource/public/mcp");
        assert_eq!(
            pr.metadata_paths(),
            ["/.well-known/oauth-protected-resource/public/mcp", "/.well-known/oauth-protected-resource"]
        );
        let pr = ProtectedResource::new("https://example.com:8443", v);
        assert_eq!(pr.metadata_location(), "https://example.com:8443/.well-known/oauth-protected-resource");
        assert_eq!(pr.metadata_paths(), ["/.well-known/oauth-protected-resource"]);
    }

    #[cfg(feature = "http")]
    #[test]
    fn challenge_format() {
        let pr = ProtectedResource::new("https://h/mcp", StaticTokens::new());
        let scopes = vec!["a".to_string(), "b".to_string()];
        assert_eq!(
            pr.challenge(Some("insufficient_scope"), Some("say \"hi\"\n"), &scopes),
            "Bearer error=\"insufficient_scope\", error_description=\"say 'hi'?\", \
             resource_metadata=\"https://h/.well-known/oauth-protected-resource/mcp\", scope=\"a b\""
        );
    }
}
