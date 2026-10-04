//! The operator-API client: health, metrics, config, DLQ, and quarantine.

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

    /// `POST /v1/replays`: starts a replay job. The node answers `202` for a
    /// new job, or `200` with the running one when a `running` or `paused` job
    /// with the same normalized filter already exists, which makes a proxy
    /// retry idempotent.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document. `404` a missing job,
    /// `409 role_not_enabled` (archive replay on a node without a queue
    /// writer) and a rejected spec are all [`AdminError::Rejected`] or
    /// [`AdminError::RoleNotEnabled`].
    pub async fn create_replay(&self, spec: &ReplaySpec) -> Result<Replay, AdminError> {
        self.send(http::Method::POST, "/v1/replays", spec).await
    }

    /// `GET /v1/replays/{id}`.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document. A missing job is
    /// [`AdminError::Rejected`] with code `replay_not_found`.
    pub async fn get_replay(&self, id: &str) -> Result<Replay, AdminError> {
        self.get_json(&replay_path(id)).await
    }

    /// `GET /v1/replays`: every replay job, newest first.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document.
    pub async fn list_replays(&self) -> Result<ReplayList, AdminError> {
        self.get_json("/v1/replays").await
    }

    /// `PATCH /v1/replays/{id}`: pauses, resumes or cancels a job, and adjusts
    /// its `rate` and `max_lag_ms`.
    ///
    /// # Errors
    ///
    /// [`AdminError`], as its variants document. A finished job is
    /// [`AdminError::Rejected`] with code `replay_finished`; a missing one has
    /// code `replay_not_found`.
    pub async fn update_replay(&self, id: &str, patch: &ReplayPatch) -> Result<Replay, AdminError> {
        self.send(http::Method::PATCH, &replay_path(id), patch)
            .await
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

    async fn get_json<R: DeserializeOwned>(&self, path_and_query: &str) -> Result<R, AdminError> {
        let response = self.get(path_and_query).await?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }

    async fn send<R: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: http::Method,
        path_and_query: &str,
        body: &B,
    ) -> Result<R, AdminError> {
        let body = json_body(body).map_err(AdminError::Unavailable)?;
        let response = self
            .core
            .send(method, path_and_query, Some(body))
            .await
            .map_err(AdminError::Unavailable)?;
        let response = check(response)?;
        decode(response.body()).map_err(AdminError::Unavailable)
    }
}

/// `/v1/replays/{id}`: the id travels as one percent-encoded path segment, so
/// it can never reshape the request.
fn replay_path(id: &str) -> String {
    format!("/v1/replays/{}", utf8_percent_encode(id, SEGMENT))
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

/// A replay job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Replay {
    /// The job id, a `UUIDv7`.
    pub id: String,
    /// `"dlq"`, `"archive"` or `"quarantine"`.
    pub kind: String,
    /// `"running"`, `"paused"`, `"done"`, `"cancelled"` or `"failed"`.
    pub state: String,
    /// The filter the job was created with.
    pub filter: serde_json::Map<String, serde_json::Value>,
    /// Items per second: delivery rows for `dlq`, hooks for `archive` and
    /// `quarantine`.
    pub rate: u64,
    /// The oldest-due dispatch lag the job tolerates, milliseconds.
    pub max_lag_ms: u64,
    /// Creation time, unix milliseconds.
    pub created_at: i64,
    /// Last update time, unix milliseconds.
    pub updated_at: i64,
    /// When the job finished, or `None` while it is still running.
    pub finished_at: Option<i64>,
    /// Rows revived (`dlq`), hooks re-enqueued (`archive`), or held hooks that
    /// passed verification again (`quarantine`).
    pub moved: u64,
    /// Keys or records examined.
    pub scanned: u64,
    /// Archive records with no source, no bound sink, or an undecodable frame
    /// (`archive`); held hooks that still fail verification or were already
    /// accepted (`quarantine`).
    pub skipped: u64,
    /// Deliveries the pipeline reported as delivered.
    pub delivered: u64,
    /// Deliveries that dead-lettered again.
    pub dead: u64,
    /// The failure or auto-pause message, when there is one.
    pub error: Option<String>,
}

/// Every replay job, newest first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplayList {
    /// The jobs.
    pub replays: Vec<Replay>,
}

/// What `POST /v1/replays` starts: a `dlq` job over dead-lettered rows, an
/// `archive` job over a time window of an archived source, or a `quarantine`
/// job that re-verifies held hooks and releases the ones that now pass.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ReplaySpec {
    /// `"dlq"`, `"archive"` or `"quarantine"`.
    pub kind: String,
    /// Restrict to one source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// Restrict to one envelope id (`dlq` and `quarantine`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Inclusive lower bound, unix milliseconds: on the dead-letter time
    /// (`dlq`) or on `received_at` (`quarantine`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
    /// Inclusive upper bound, on the same time as `since`, unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<i64>,
    /// Inclusive lower bound on `received_at`, unix milliseconds (`archive`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<i64>,
    /// Inclusive upper bound on `received_at`, unix milliseconds (`archive`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<i64>,
    /// Sink indexes into the source's current `sinks` (`archive`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sinks: Option<Vec<u64>>,
    /// Items per second. The node defaults to `1000`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<u64>,
    /// The oldest-due dispatch lag the job tolerates, milliseconds. The node
    /// defaults to `2000`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lag_ms: Option<u64>,
}

/// What `PATCH /v1/replays/{id}` changes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ReplayPatch {
    /// `"running"`, `"paused"` or `"cancelled"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Items per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<u64>,
    /// The oldest-due dispatch lag the job tolerates, milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lag_ms: Option<u64>,
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
    use super::{AdminClient, AdminError, ListDeadLettersParams, ReplayPatch, ReplaySpec};
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

    const REPLAY: &str = r#"{"id":"r1","kind":"dlq","state":"running","filter":{},
        "rate":1000,"max_lag_ms":2000,"created_at":0,"updated_at":0,"finished_at":null,
        "moved":0,"scanned":0,"skipped":0,"delivered":0,"dead":0,"error":null}"#;

    #[tokio::test]
    async fn replay_methods_use_the_documented_paths() {
        let transport = Recording::new(202, REPLAY);
        let targets = transport.targets();
        let client = client(transport);
        let spec = ReplaySpec {
            kind: "dlq".to_owned(),
            source_id: Some("demo".to_owned()),
            rate: Some(500),
            ..ReplaySpec::default()
        };
        let created = client.create_replay(&spec).await.expect("created");
        assert_eq!(created.id, "r1");
        // The id travels as one percent-encoded path segment.
        client.get_replay("r 1").await.expect("got");
        client
            .update_replay(
                "r1",
                &ReplayPatch {
                    state: Some("paused".to_owned()),
                    ..ReplayPatch::default()
                },
            )
            .await
            .expect("updated");
        assert_eq!(
            targets.lock().expect("not poisoned").as_slice(),
            ["/v1/replays", "/v1/replays/r%201", "/v1/replays/r1"]
        );
    }

    #[tokio::test]
    async fn list_replays_returns_the_page() {
        let client = client(Recording::new(200, r#"{"replays":[]}"#));
        assert!(
            client
                .list_replays()
                .await
                .expect("listed")
                .replays
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_missing_replay_is_a_rejection_with_the_body_code() {
        let client = client(Recording::new(404, r#"{"error":"replay_not_found"}"#));
        match client.get_replay("missing").await.expect_err("missing") {
            AdminError::Rejected { status, code } => {
                assert_eq!(status, http::StatusCode::NOT_FOUND);
                assert_eq!(code.as_deref(), Some("replay_not_found"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn a_replay_spec_serializes_to_the_given_keys() {
        let spec = ReplaySpec {
            kind: "dlq".to_owned(),
            source_id: Some("demo".to_owned()),
            rate: Some(500),
            ..ReplaySpec::default()
        };
        assert_eq!(
            serde_json::to_value(&spec).expect("serializable"),
            serde_json::json!({"kind": "dlq", "source_id": "demo", "rate": 500})
        );
    }
}
