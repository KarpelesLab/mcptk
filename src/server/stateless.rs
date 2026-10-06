//! Protocol revision 2026-07-28 and later: no handshake, per-request
//! metadata, `server/discover`, result decoration and `subscriptions/listen`.

use super::session::SessionInner;
use super::{Outbound, Outlet, Server, Session};
use crate::jsonrpc::{ErrorObject, INVALID_PARAMS, Message, RESOURCE_NOT_FOUND, RequestId};
use crate::types::*;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Results that carry `ttlMs` and `cacheScope`.
const CACHEABLE: &[&str] =
    &["server/discover", "tools/list", "prompts/list", "resources/list", "resources/templates/list", "resources/read"];

/// The protocol metadata a stateless request carries in `_meta`.
#[derive(Debug)]
pub(crate) struct RequestMeta {
    pub(crate) version: &'static str,
    pub(crate) client_info: Option<Implementation>,
    pub(crate) capabilities: ClientCapabilities,
    pub(crate) log_level: Option<LoggingLevel>,
}

/// Whether a request uses a stateless revision: it carries a protocol
/// version in `_meta`, or is `server/discover` (which only exists there).
pub(crate) fn is_stateless_request(method: &str, params: Option<&Value>) -> bool {
    method == "server/discover"
        || params.and_then(|p| p.get("_meta")).and_then(|m| m.get(META_PROTOCOL_VERSION)).is_some()
}

fn meta_field<T: DeserializeOwned>(meta: &JsonObject, key: &str) -> Result<Option<T>, ErrorObject> {
    match meta.get(key) {
        None => Ok(None),
        Some(v) => serde_json::from_value(v.clone())
            .map(Some)
            .map_err(|e| ErrorObject::invalid_params(format!("invalid {key}: {e}"))),
    }
}

/// Read and check the protocol metadata of a stateless request.
pub(crate) fn parse_meta(params: Option<&Value>) -> Result<RequestMeta, ErrorObject> {
    let meta = params.and_then(|p| p.get("_meta")).and_then(Value::as_object);
    let Some(meta) = meta else {
        return Err(ErrorObject::invalid_params(format!("missing _meta with {META_PROTOCOL_VERSION}")));
    };
    let Some(requested) = meta.get(META_PROTOCOL_VERSION).and_then(Value::as_str) else {
        return Err(ErrorObject::invalid_params(format!("missing {META_PROTOCOL_VERSION}")));
    };
    let Some(version) = STATELESS_PROTOCOL_VERSIONS.iter().find(|v| **v == requested).copied() else {
        return Err(ErrorObject::unsupported_protocol_version(requested));
    };
    let Some(capabilities) = meta_field(meta, META_CLIENT_CAPABILITIES)? else {
        return Err(ErrorObject::invalid_params(format!("missing {META_CLIENT_CAPABILITIES}")));
    };
    Ok(RequestMeta {
        version,
        client_info: meta_field(meta, META_CLIENT_INFO)?,
        capabilities,
        log_level: meta_field(meta, META_LOG_LEVEL)?,
    })
}

/// Adapt a result to the stateless revisions: `resultType`, caching hints,
/// `serverInfo`, and the resource-not-found error code.
pub(crate) fn finish(server: &Server, method: &str, result: Result<Value, ErrorObject>) -> Result<Value, ErrorObject> {
    match result {
        Ok(Value::Object(mut map)) => {
            let complete = map.get("resultType").is_none_or(|t| t == "complete");
            map.entry("resultType").or_insert_with(|| "complete".into());
            if complete && CACHEABLE.contains(&method) {
                let config = &server.inner.config;
                let ttl = u64::try_from(config.cache_ttl.as_millis()).unwrap_or(u64::MAX);
                map.entry("ttlMs").or_insert_with(|| ttl.into());
                let scope = match method {
                    "tools/list" if config.tool_filter.is_some() => CacheScope::Private,
                    _ => config.cache_scope,
                };
                map.entry("cacheScope").or_insert_with(|| json!(scope));
            }
            if let Some(meta) = map.entry("_meta").or_insert_with(|| json!({})).as_object_mut() {
                meta.entry(META_SERVER_INFO).or_insert_with(|| json!(server.info()));
            }
            Ok(Value::Object(map))
        }
        Ok(other) => Ok(other),
        Err(mut e) => {
            if e.code == RESOURCE_NOT_FOUND {
                e.code = INVALID_PARAMS;
            }
            Err(e)
        }
    }
}

pub(crate) fn discover(server: &Server) -> DiscoverResult {
    DiscoverResult {
        supported_versions: SUPPORTED_PROTOCOL_VERSIONS.iter().map(|v| v.to_string()).collect(),
        capabilities: server.capabilities(),
        instructions: server.instructions().map(str::to_string),
    }
}

/// The part of `requested` this server can honor.
pub(crate) fn honored(server: &Server, requested: &SubscriptionFilter) -> SubscriptionFilter {
    let c = &server.inner.config;
    let yes = |asked: Option<bool>, offered: bool| (asked == Some(true) && offered).then_some(true);
    SubscriptionFilter {
        tools_list_changed: yes(requested.tools_list_changed, c.tools),
        prompts_list_changed: yes(requested.prompts_list_changed, c.prompts),
        resources_list_changed: yes(requested.resources_list_changed, c.resources),
        resource_subscriptions: requested.resource_subscriptions.clone().filter(|uris| c.resources && !uris.is_empty()),
    }
}

fn wants(filter: &SubscriptionFilter, method: &str, uri: Option<&str>) -> bool {
    match method {
        "notifications/tools/list_changed" => filter.tools_list_changed == Some(true),
        "notifications/prompts/list_changed" => filter.prompts_list_changed == Some(true),
        "notifications/resources/list_changed" => filter.resources_list_changed == Some(true),
        "notifications/resources/updated" => {
            uri.is_some_and(|uri| filter.resource_subscriptions.iter().flatten().any(|u| u == uri))
        }
        _ => false,
    }
}

/// An open `subscriptions/listen` stream.
struct Listener {
    key: u64,
    id: RequestId,
    filter: SubscriptionFilter,
    outlet: Outlet,
    session: Weak<SessionInner>,
}

/// The server's open `subscriptions/listen` streams.
#[derive(Default)]
pub(crate) struct Listeners {
    next: AtomicU64,
    list: Mutex<Vec<Arc<Listener>>>,
}

/// Removes a listener when dropped (the listen request ended).
pub(crate) struct ListenerGuard {
    server: Server,
    key: u64,
}

impl Drop for ListenerGuard {
    fn drop(&mut self) {
        self.server.inner.listeners.list.lock().unwrap().retain(|l| l.key != self.key);
    }
}

fn notification_params(id: &RequestId, uri: Option<&str>) -> Value {
    let mut params = json!({ "_meta": { META_SUBSCRIPTION_ID: id } });
    if let Some(uri) = uri {
        params["uri"] = uri.into();
    }
    params
}

impl Listeners {
    /// Acknowledge a listen request and start delivering to it.
    pub(crate) fn add(
        &self,
        server: &Server,
        session: &Session,
        id: RequestId,
        filter: SubscriptionFilter,
        outlet: Outlet,
    ) -> ListenerGuard {
        let key = self.next.fetch_add(1, Ordering::Relaxed);
        let mut ack = notification_params(&id, None);
        ack["notifications"] = json!(filter);
        // Under the lock, so that nothing is delivered before the ack.
        let mut list = self.list.lock().unwrap();
        outlet.send(Outbound::Message(Message::notification("notifications/subscriptions/acknowledged", Some(ack))));
        list.push(Arc::new(Listener { key, id, filter, outlet, session: Arc::downgrade(&session.inner) }));
        ListenerGuard { server: server.clone(), key }
    }

    /// Deliver a change notification to the listeners that asked for it
    /// (only those opened over `only`'s connection, if given).
    pub(crate) fn notify(&self, method: &str, uri: Option<&str>, only: Option<&Session>) {
        let list = self.list.lock().unwrap();
        for l in list.iter() {
            if !wants(&l.filter, method, uri)
                || only.is_some_and(|s| !Weak::ptr_eq(&l.session, &Arc::downgrade(&s.inner)))
            {
                continue;
            }
            let params = notification_params(&l.id, uri);
            l.outlet.send(Outbound::Message(Message::notification(method, Some(params))));
        }
    }
}
