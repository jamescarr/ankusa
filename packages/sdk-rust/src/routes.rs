//! The route-management client: the route table, the global IP rules, and the
//! dry run.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use percent_encoding::utf8_percent_encode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::client::{
    ClientBuilder, ConfigError, Core, HeaderNames, Query, SEGMENT, UnavailableReason, decode,
    error_body, json_body,
};
use crate::transport::{ReqwestTransport, Transport};

/// Client for the route-management listener (`routes.admin.port`).
pub struct RoutesClient<T = ReqwestTransport> {
    core: Arc<Core<T>>,
}

impl<T> RoutesClient<T> {
    pub(crate) fn from_core(core: Arc<Core<T>>) -> Self {
        Self { core }
    }
}

impl<T> Clone for RoutesClient<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> fmt::Debug for RoutesClient<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutesClient")
            .field("base_url", &self.core.base_url)
            .field("header_names", &HeaderNames(&self.core.headers))
            .field("timeout", &self.core.timeout)
            .finish_non_exhaustive()
    }
}

impl RoutesClient {
    /// Builds a client for `base_url` (`http://host:port`), with the default
    /// 10-second timeout.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when `base_url` is not an absolute `http`/`https` URL,
    /// or the HTTP client cannot be built.
    pub fn new(base_url: impl Into<String>) -> Result<Self, ConfigError> {
        ClientBuilder::new(base_url).routes()
    }
}

impl<T: Transport> RoutesClient<T> {
    /// `GET /health`: how many routes this node enforces.
    ///
    /// # Errors
    ///
    /// [`RoutesError`]: [`RoutesError::Unavailable`] for anything but a `200`
    /// with a JSON body.
    pub async fn health(&self) -> Result<RoutesHealth, RoutesError> {
        self.get("/health").await
    }

    /// `GET /admin/routes`: one page of the route table.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn list_routes(&self, params: &ListRoutesParams) -> Result<RoutePage, RoutesError> {
        let mut query = Query::new();
        if let Some(enabled) = params.enabled {
            query.push_display("enabled", enabled);
        }
        if let Some(limit) = params.limit {
            query.push_display("limit", limit);
        }
        if let Some(cursor) = params.cursor.as_deref() {
            query.push_str("cursor", cursor);
        }
        self.get(&format!("/admin/routes{}", query.finish())).await
    }

    /// `POST /admin/routes`: adds a route definition.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document. A `path` the node refuses,
    /// or one another enabled route already captures, is
    /// [`RoutesError::Rejected`].
    pub async fn create_route(&self, input: &RouteInput) -> Result<Route, RoutesError> {
        self.send(http::Method::POST, "/admin/routes", input).await
    }

    /// `GET /admin/routes/{id}`.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn get_route(&self, id: &str) -> Result<Route, RoutesError> {
        self.get(&route_path(id)?).await
    }

    /// `PUT /admin/routes/{id}`: replaces the definition wholesale.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn replace_route(&self, id: &str, input: &RouteInput) -> Result<Route, RoutesError> {
        self.send(http::Method::PUT, &route_path(id)?, input).await
    }

    /// `PATCH /admin/routes/{id}`: changes only the fields that are set.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn update_route(&self, id: &str, patch: &RoutePatch) -> Result<Route, RoutesError> {
        self.send(http::Method::PATCH, &route_path(id)?, patch)
            .await
    }

    /// `DELETE /admin/routes/{id}`.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn delete_route(&self, id: &str) -> Result<(), RoutesError> {
        let response = self
            .core
            .send(http::Method::DELETE, &route_path(id)?, None)
            .await
            .map_err(RoutesError::Unavailable)?;
        // The body is not read: a successful delete has nothing to decode.
        check(response).map(|_| ())
    }

    /// `GET /admin/ip-rules`: the global rules every route inherits.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn get_ip_rules(&self) -> Result<IpRules, RoutesError> {
        self.get("/admin/ip-rules").await
    }

    /// `PUT /admin/ip-rules`: replaces the global rules.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document. An unparseable CIDR is
    /// [`RoutesError::Rejected`].
    pub async fn put_ip_rules(&self, rules: &IpRules) -> Result<IpRules, RoutesError> {
        self.send(http::Method::PUT, "/admin/ip-rules", rules).await
    }

    /// `POST /admin/routes/test`: which route a request would match, without
    /// capturing anything.
    ///
    /// # Errors
    ///
    /// [`RoutesError`], as its variants document.
    pub async fn test_route(&self, request: &DryRunRequest) -> Result<DryRunResult, RoutesError> {
        self.send(http::Method::POST, "/admin/routes/test", request)
            .await
    }

    async fn get<R: DeserializeOwned>(&self, path_and_query: &str) -> Result<R, RoutesError> {
        let response = self
            .core
            .send(http::Method::GET, path_and_query, None)
            .await
            .map_err(RoutesError::Unavailable)?;
        let response = check(response)?;
        decode(response.body()).map_err(RoutesError::Unavailable)
    }

    async fn send<R: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: http::Method,
        path_and_query: &str,
        body: &B,
    ) -> Result<R, RoutesError> {
        let body = json_body(body).map_err(RoutesError::Unavailable)?;
        let response = self
            .core
            .send(method, path_and_query, Some(body))
            .await
            .map_err(RoutesError::Unavailable)?;
        let response = check(response)?;
        decode(response.body()).map_err(RoutesError::Unavailable)
    }
}

/// `/admin/routes/{id}` for a valid id; an id that is empty, `.` or `..` is
/// refused here, before any request is built.
fn route_path(id: &str) -> Result<String, RoutesError> {
    if id.is_empty() || id == "." || id == ".." {
        return Err(RoutesError::InvalidRouteId { id: id.to_owned() });
    }
    Ok(format!(
        "/admin/routes/{}",
        utf8_percent_encode(id, SEGMENT)
    ))
}

/// Maps a response status onto the client's classification: `2xx` is the
/// caller's to decode, `404` is "no such route", another `4xx` is a rejection,
/// and everything else — `1xx`, `3xx`, `5xx` — means try again later.
fn check(response: http::Response<Bytes>) -> Result<http::Response<Bytes>, RoutesError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status == http::StatusCode::NOT_FOUND {
        return Err(RoutesError::NotFound);
    }
    if status.is_client_error() {
        return Err(RoutesError::Rejected(Box::new(rejection(&response))));
    }
    Err(RoutesError::Unavailable(UnavailableReason::Status(status)))
}

fn rejection(response: &http::Response<Bytes>) -> RoutesRejection {
    let body = error_body(response.body());
    // Read only from a JSON object: a proxy's HTML error page has no fields.
    let string = |key: &str| {
        body.get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    RoutesRejection {
        status: response.status(),
        code: string("error"),
        field: string("field"),
        message: string("message"),
        conflicting_id: string("conflicting_id"),
        max_routes: body.get("max_routes").and_then(serde_json::Value::as_u64),
    }
}

/// `GET /health` on the route-management listener.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RoutesHealth {
    /// Always `"ok"` from a healthy listener.
    pub status: String,
    /// Route definitions in the table, enabled or not.
    pub routes: u64,
}

/// One IP rule: a network and what to do with a client in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpRule {
    /// Allow or deny.
    pub action: Access,
    /// An IPv4 or IPv6 network; a bare address is a full-length prefix.
    pub cidr: String,
}

/// Allow or deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    /// Allow the request through.
    Allow,
    /// Refuse the request.
    Deny,
}

/// The global IP rules: the floor every route inherits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpRules {
    /// Applied when no rule in `rules` matched.
    pub default: Access,
    /// Ordered; the first match wins.
    pub rules: Vec<IpRule>,
}

/// A stored route definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Route {
    /// A lowercase slug, supplied by the caller or generated. Immutable after
    /// creation.
    pub id: String,
    /// A pattern, not a regex: `/`-separated segments, each a literal, a
    /// `:param` (exactly one segment), or a trailing `*` wildcard.
    pub path: String,
    /// The methods this route captures. Defaults to `["POST"]`.
    pub methods: Vec<String>,
    /// A disabled route captures nothing.
    pub enabled: bool,
    /// Empty (the default) means the global list applies; non-empty replaces
    /// it for this route.
    pub ip_rules: Vec<IpRule>,
    /// Free-form JSON for the operator.
    pub metadata: serde_json::Map<String, serde_json::Value>,
    /// Set once, at creation.
    pub inserted_at: String,
    /// When the definition was last written.
    pub updated_at: String,
}

/// A route definition as written. Only the fields that are set travel: the
/// node applies its own defaults to the rest.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RouteInput {
    /// Optional on create. On `PUT` the path's `id` wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The capture pattern.
    pub path: String,
    /// Defaults to `["POST"]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    /// Defaults to `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Defaults to `[]` (the global list applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_rules: Option<Vec<IpRule>>,
    /// Defaults to `{}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// The four fields `PATCH` may change. `path` and `id` are immutable: replace
/// the definition instead.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RoutePatch {
    /// Enables or disables capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Replaces the captured methods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    /// Replaces this route's own IP rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_rules: Option<Vec<IpRule>>,
    /// Replaces the metadata wholesale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// One page of the route table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RoutePage {
    /// The routes on this page.
    pub routes: Vec<Route>,
    /// The last id of this page when another page exists; pass it back as
    /// `cursor`. `null` on the last page.
    pub next_cursor: Option<String>,
}

/// The filters `GET /admin/routes` accepts.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ListRoutesParams {
    /// Only routes that are enabled (or disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// The last id of the previous page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// A request to dry-run against the route table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DryRunRequest {
    /// The method the sender would use.
    pub method: String,
    /// The path as the client would send it.
    pub path: String,
    /// The client address.
    pub ip: String,
}

/// What the dry run decided.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DryRunResult {
    /// Allow or deny.
    pub decision: Access,
    /// Why.
    pub reason: DryRunReason,
    /// The matched route, when one matched.
    pub route_id: Option<String>,
    /// The rule that decided the IP question, when one did.
    pub ip_rule: Option<DryRunIpRule>,
}

/// Why a dry run decided what it did. Only `Matched` allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DryRunReason {
    /// A route matched and its IP rules allowed the address.
    Matched,
    /// No route captures that path.
    NoRoute,
    /// A route captures the path but not that method.
    Method,
    /// A route matched, and the address was denied.
    IpDenied,
}

/// The rule that produced an IP decision, and the list it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DryRunIpRule {
    /// Allow or deny.
    pub action: Access,
    /// The matched network.
    pub cidr: String,
    /// Which list the rule came from.
    pub scope: IpRuleScope,
}

/// Where a deciding IP rule lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IpRuleScope {
    /// `routes.ip_rules`, the global list.
    Global,
    /// The matched route's own list.
    Route,
}

/// Something the route-management API would not do.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RoutesError {
    /// The route id cannot address a route: it is empty, or exactly `.` or
    /// `..`. Nothing was sent.
    #[error("invalid route id {id:?}: must be non-empty and not `.` or `..`")]
    InvalidRouteId {
        /// The rejected id.
        id: String,
    },
    /// The API has no such route.
    #[error("route not found")]
    NotFound,
    /// The API refused the request. Boxed: the rejection carries six fields,
    /// and a `Result` should stay small.
    #[error("route-management API rejected the request (HTTP {})", .0.status)]
    Rejected(Box<RoutesRejection>),
    /// No usable answer: a `5xx`, an unfollowed redirect, a timeout, a
    /// transport failure, or a success body that was not JSON.
    #[error("route-management API unavailable")]
    Unavailable(#[source] UnavailableReason),
}

impl RoutesError {
    /// Whether calling again later could succeed.
    ///
    /// Only [`RoutesError::Unavailable`] is retryable: a rejection and a
    /// missing route both need the operator to change something instead.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

/// A `4xx` from the route-management API, with every field the error shape can
/// carry.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RoutesRejection {
    /// The response status.
    pub status: http::StatusCode,
    /// The API's `error` code, e.g. `invalid_route` or `duplicate_route`.
    pub code: Option<String>,
    /// The offending field, when the API names one.
    pub field: Option<String>,
    /// The human-readable reason, when the API gives one.
    pub message: Option<String>,
    /// On `duplicate_route`: the route already holding that path and method.
    pub conflicting_id: Option<String>,
    /// On `too_many_routes`: the configured cap.
    pub max_routes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{ListRoutesParams, RoutesClient, RoutesError};
    use crate::client::{ClientBuilder, test_support::Recording};

    fn client(transport: Recording) -> RoutesClient<Recording> {
        ClientBuilder::with_transport("http://gateway.invalid", transport)
            .routes()
            .expect("valid client")
    }

    #[tokio::test]
    async fn list_routes_query_percent_encodes_cursor() {
        let transport = Recording::new(200, r#"{"routes":[],"next_cursor":null}"#);
        let targets = transport.targets();
        let client = client(transport);
        let params = ListRoutesParams {
            enabled: None,
            limit: Some(5),
            cursor: Some("a b&c/d".to_owned()),
        };
        client.list_routes(&params).await.expect("listed");
        assert_eq!(
            targets.lock().expect("not poisoned").as_slice(),
            ["/admin/routes?limit=5&cursor=a%20b%26c%2Fd"]
        );
    }

    #[tokio::test]
    async fn invalid_route_id_sends_nothing() {
        let transport = Recording::new(200, "{}");
        let targets = transport.targets();
        let client = client(transport);
        for id in ["", ".", ".."] {
            let err = client.get_route(id).await.expect_err("invalid id");
            assert!(matches!(err, RoutesError::InvalidRouteId { .. }));
        }
        assert!(targets.lock().expect("not poisoned").is_empty());
    }

    #[tokio::test]
    async fn a_success_body_that_is_not_json_is_unavailable() {
        let client = client(Recording::new(200, "not json"));
        let err = client.get_route("stripe").await.expect_err("not JSON");
        assert!(matches!(err, RoutesError::Unavailable(_)));
        assert!(err.is_retryable());
    }
}
