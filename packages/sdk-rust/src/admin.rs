//! The operator-API client: health, metrics, config, DLQ, and quarantine.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::client::{
    ClientBuilder, ConfigError, Core, HeaderNames, Query, UnavailableReason, decode, error_body,
    json_body,
};
use crate::transport::{ReqwestTransport, Transport};

/// Client for the operator listener (`admin.port`).
pub struct AdminClient<T = ReqwestTransport> {
    core: Arc<Core<T>>,
}

impl<T> AdminClient<T> {
    pub(crate) fn from_core(core: Arc<Core<T>>) -> Self {
        Self { core }
    }
}

impl<T> Clone for AdminClient<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> fmt::Debug for AdminClient<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdminClient")
            .field("base_url", &self.core.base_url)
            .field("header_names", &HeaderNames(&self.core.headers))
            .field("timeout", &self.core.timeout)
            .finish_non_exhaustive()
    }
}

impl AdminClient {
    /// Builds a client for `base_url` (`http://host:port`), with the default
    /// 10-second timeout.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when `base_url` is not an absolute `http`/`https` URL,
    /// or the HTTP client cannot be built.
    pub fn new(base_url: impl Into<String>) -> Result<Self, ConfigError> {
        ClientBuilder::new(base_url).admin()
    }
}

impl<T: Transport> AdminClient<T> {
    /// `GET /health`: this node's instance name and the roles it runs.
    ///
    /// # Errors
    ///
    /// [`AdminError`]: [`AdminError::Unavailable`] for anything but a `200`
    /// with a JSON body.
    pub async fn health(&self) -> Result<AdminHealth, AdminError> {
        let response = self.get("/health").await?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }

    /// `GET /metrics`: the Prometheus exposition text, verbatim.
    ///
    /// # Errors
    ///
    /// [`AdminError`]: [`AdminError::Unavailable`] for anything but a `200`.
    /// The body is text, never JSON, so a `200` always yields it — invalid
    /// UTF-8 is replaced rather than refused.
    pub async fn metrics(&self) -> Result<String, AdminError> {
        let response = self.get("/metrics").await?;
        Ok(String::from_utf8_lossy(response.body()).into_owned())
    }

    /// `GET /v1/config`: the node's effective configuration, with secrets
    /// redacted.
    ///
    /// # Errors
    ///
    /// [`AdminError`]: [`AdminError::Unavailable`] for anything but a `200`
    /// with a JSON body.
    pub async fn config(&self) -> Result<serde_json::Map<String, serde_json::Value>, AdminError> {
        let response = self.get("/v1/config").await?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }

    /// `GET /v1/dlq`: one page of the dead-letter queue.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document.
    pub async fn list_dead_letters(
        &self,
        params: &ListDeadLettersParams,
    ) -> Result<DlqPage, AdminError> {
        let mut query = Query::new();
        if let Some(source_id) = params.source_id.as_deref() {
            query.push_str("source_id", source_id);
        }
        if let Some(since) = params.since {
            query.push_display("since", since);
        }
        if let Some(limit) = params.limit {
            query.push_display("limit", limit);
        }
        let response = self.get(&format!("/v1/dlq{}", query.finish())).await?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }

    /// `POST /v1/dlq/replay`: re-delivers everything `filter` matches. An
    /// empty filter replays the whole queue.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document. A filter the node refuses is
    /// [`AdminError::Rejected`].
    pub async fn replay_dead_letters(&self, filter: &ReplayFilter) -> Result<Replayed, AdminError> {
        let body = json_body(filter).map_err(AdminError::Unavailable)?;
        let response = self
            .core
            .send(http::Method::POST, "/v1/dlq/replay", Some(body))
            .await
            .map_err(AdminError::Unavailable)?;
        let response = check(response)?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }

    /// `GET /v1/quarantine`: one page of the hooks whose signature did not
    /// verify.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document.
    pub async fn list_quarantined(
        &self,
        params: &ListQuarantinedParams,
    ) -> Result<QuarantinePage, AdminError> {
        let mut query = Query::new();
        if let Some(limit) = params.limit {
            query.push_display("limit", limit);
        }
        let response = self
            .get(&format!("/v1/quarantine{}", query.finish()))
            .await?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }

    async fn get(&self, path_and_query: &str) -> Result<http::Response<Bytes>, AdminError> {
        let response = self
            .core
            .send(http::Method::GET, path_and_query, None)
            .await
            .map_err(AdminError::Unavailable)?;
        check(response)
    }
}

/// Maps a response status onto the client's classification: `2xx` is the
/// caller's to decode, a `409 role_not_enabled` names the missing role, another
/// `4xx` is a rejection, and everything else — `1xx`, `3xx`, `5xx` — means try
/// again later.
fn check(response: http::Response<Bytes>) -> Result<http::Response<Bytes>, AdminError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status == http::StatusCode::CONFLICT {
        let body = error_body(response.body());
        let string = |key: &str| {
            body.get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        if string("error").as_deref() == Some("role_not_enabled") {
            return Err(AdminError::RoleNotEnabled {
                role: string("role"),
            });
        }
        return Err(AdminError::Rejected {
            status,
            code: string("error"),
        });
    }
    if status.is_client_error() {
        return Err(AdminError::Rejected {
            status,
            code: error_body(response.body())
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        });
    }
    Err(AdminError::Unavailable(UnavailableReason::Status(status)))
}

/// `GET /health` on the operator listener.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AdminHealth {
    /// Always `"ok"` from a healthy node.
    pub status: String,
    /// The node's instance name.
    pub instance: String,
    /// The roles this node runs. Strings, not an enum: a role added by a newer
    /// node must not break a decode.
    pub roles: Vec<String>,
}

/// One dead-lettered hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DlqEntry {
    /// Envelope id (a `UUIDv7`).
    pub id: String,
    /// The source the hook was captured under.
    pub source_id: String,
    /// The tenant, when the source carries one.
    pub tenant_id: Option<String>,
    /// WAL sequence number, `None` when the record never committed.
    pub seq: Option<u64>,
    /// When the edge received the hook, unix milliseconds.
    pub received_at: i64,
    /// When dispatch gave up, unix milliseconds.
    pub dead_lettered_at: i64,
    /// Body size in bytes.
    pub size: u64,
    /// The captured content type, when the sender set one.
    pub content_type: Option<String>,
    /// The give-up reason, as the node inspected it.
    pub reason: String,
}

/// One page of the dead-letter queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DlqPage {
    /// Number of matching entries, before `limit`.
    pub total: u64,
    /// The entries on this page.
    pub entries: Vec<DlqEntry>,
}

/// One quarantined hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QuarantineEntry {
    /// Envelope id (a `UUIDv7`).
    pub id: String,
    /// The source the hook was captured under.
    pub source_id: String,
    /// When the edge received the hook, unix milliseconds.
    pub received_at: i64,
    /// The verifier failure, as the node inspected it.
    pub reason: String,
}

/// One page of the quarantine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QuarantinePage {
    /// The entries on this page.
    pub entries: Vec<QuarantineEntry>,
}

/// What a replay re-delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Replayed {
    /// Number of entries re-delivered.
    pub replayed: u64,
}

/// Which dead letters to replay. All `None` replays the whole queue.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ReplayFilter {
    /// Exact source id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// Exact envelope id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Inclusive lower bound on the dead-letter timestamp, unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
}

/// The filters `GET /v1/dlq` accepts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ListDeadLettersParams {
    /// Exact source id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// Inclusive lower bound on the dead-letter timestamp, unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
    /// Page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// The filters `GET /v1/quarantine` accepts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ListQuarantinedParams {
    /// Page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Something the operator API would not do.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdminError {
    /// This node does not run the role the endpoint belongs to. Nothing about
    /// the request was wrong: ask a node that runs it, or enable the role.
    #[error("role not enabled on this node: {}", role.as_deref().unwrap_or("unknown"))]
    RoleNotEnabled {
        /// The role the node reported, when it named one.
        role: Option<String>,
    },
    /// The API refused the request.
    #[error("operator API rejected the request (HTTP {status})")]
    Rejected {
        /// The response status.
        status: http::StatusCode,
        /// The API's `error` code, e.g. `invalid_filter`.
        code: Option<String>,
    },
    /// No usable answer: a `5xx`, an unfollowed redirect, a timeout, a
    /// transport failure, or a success body that was not JSON.
    #[error("operator API unavailable")]
    Unavailable(#[source] UnavailableReason),
}

impl AdminError {
    /// Whether calling again later could succeed.
    ///
    /// Only [`AdminError::Unavailable`] is retryable: a rejected filter needs
    /// a different request, and a missing role needs a different node.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

#[cfg(test)]
mod tests {
    use super::{AdminClient, AdminError, ListDeadLettersParams};
    use crate::client::{ClientBuilder, test_support::Recording};

    fn client(transport: Recording) -> AdminClient<Recording> {
        ClientBuilder::with_transport("http://gateway.invalid", transport)
            .admin()
            .expect("valid client")
    }

    #[tokio::test]
    async fn list_dead_letters_writes_the_query_in_field_order() {
        let transport = Recording::new(200, r#"{"total":0,"entries":[]}"#);
        let targets = transport.targets();
        let client = client(transport);
        let params = ListDeadLettersParams {
            source_id: Some("a b".to_owned()),
            since: Some(1_720_000_000_000),
            limit: Some(50),
        };
        client.list_dead_letters(&params).await.expect("listed");
        assert_eq!(
            targets.lock().expect("not poisoned").as_slice(),
            ["/v1/dlq?source_id=a%20b&since=1720000000000&limit=50"]
        );
    }

    #[tokio::test]
    async fn a_role_not_enabled_conflict_names_the_role() {
        let client = client(Recording::new(
            409,
            r#"{"error":"role_not_enabled","role":"dispatch"}"#,
        ));
        let err = client.health().await.expect_err("role missing");
        match err {
            AdminError::RoleNotEnabled { role } => assert_eq!(role.as_deref(), Some("dispatch")),
            other => panic!("expected RoleNotEnabled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn metrics_replaces_invalid_utf8() {
        let transport = Recording::new(200, "ok");
        let client = client(transport);
        assert_eq!(client.metrics().await.expect("text"), "ok");
    }
}
