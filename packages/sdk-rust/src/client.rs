//! The shared client core: base URL, headers, timeout, and the transport.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::admin::AdminClient;
use crate::claim_check::ClaimCheckClient;
use crate::routes::RoutesClient;
use crate::transport::{ReqwestTransport, Transport, TransportError};

/// The RFC 3986 unreserved set: everything else travels percent-encoded, so a
/// route id, a cursor, or a filter value can never reshape its request.
pub(crate) const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// The request timeout every client starts with, matching the TypeScript and
/// Python SDKs.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Something a client cannot be built with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The base URL is not an absolute `http`/`https` URL.
    #[error("invalid base URL {url:?}: want an absolute http(s) URL")]
    InvalidBaseUrl {
        /// The rejected URL, as passed.
        url: String,
    },
    /// A header name or value the client refuses to send.
    #[error("invalid header {name:?}")]
    InvalidHeader {
        /// The header name, as passed.
        name: String,
    },
    /// The transport itself could not be built.
    #[error("could not build the HTTP client")]
    Transport(#[source] TransportError),
}

/// Why a request never produced an answer.
///
/// Every "the deployment is not answering the way this call needs" failure
/// funnels through this type, so each client only has to add its own
/// request-level classification on top.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum UnavailableReason {
    /// A response with a status this call cannot use: a `3xx` redirect, a
    /// `5xx`, or a `2xx` that is not the one the endpoint documents.
    #[error("unexpected HTTP status {0}")]
    Status(http::StatusCode),
    /// No response before the client's timeout.
    #[error("no response within {0:?}")]
    Timeout(Duration),
    /// The transport failed outright: no connection, or a dropped body.
    #[error("transport failed")]
    Transport(#[source] TransportError),
    /// A response body that is not the JSON this call expects.
    #[error("invalid JSON")]
    Json(#[source] serde_json::Error),
}

/// Builds a client for one deployment, sharing its base URL, headers, and
/// timeout.
///
/// Every request is bounded by `tokio::time::timeout`, so a Tokio runtime with
/// the time driver enabled is required.
///
/// [`ClientBuilder::new`] uses the default [`ReqwestTransport`];
/// [`ClientBuilder::with_transport`] takes any [`Transport`].
pub struct ClientBuilder<T = ReqwestTransport> {
    base_url: String,
    headers: http::HeaderMap,
    timeout: Duration,
    transport: Result<T, ConfigError>,
    error: Option<ConfigError>,
}

// Debug by hand: a header *value* can be a bearer token, and the derive would
// print it. Only the shared configuration and the header names are shown.
impl<T> fmt::Debug for ClientBuilder<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("base_url", &self.base_url)
            .field("header_names", &HeaderNames(&self.headers))
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl ClientBuilder<ReqwestTransport> {
    /// Starts a builder for `base_url`: `http://host:port`, optionally with a
    /// path prefix.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::connecting(base_url.into(), ReqwestTransport::new())
    }
}

impl<T: Transport> ClientBuilder<T> {
    /// Starts a builder around `transport`, so requests never touch the
    /// network.
    #[must_use]
    pub fn with_transport(base_url: impl Into<String>, transport: T) -> Self {
        Self::connecting(base_url.into(), Ok(transport))
    }

    fn connecting(base_url: String, transport: Result<T, ConfigError>) -> Self {
        Self {
            base_url,
            headers: http::HeaderMap::new(),
            timeout: DEFAULT_TIMEOUT,
            transport,
            error: None,
        }
    }

    /// Adds a header sent with every request.
    ///
    /// A name or value that cannot be sent is not reported here: the terminal
    /// builder methods return it, so one `?` at the end covers the whole
    /// builder.
    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        match (
            http::HeaderName::from_bytes(name.as_bytes()),
            http::HeaderValue::from_str(value),
        ) {
            (Ok(name), Ok(value)) => {
                self.headers.insert(name, value);
            }
            _ => {
                // The first failure is kept: later ones are consequences of the
                // config already being wrong.
                if self.error.is_none() {
                    self.error = Some(ConfigError::InvalidHeader {
                        name: name.to_owned(),
                    });
                }
            }
        }
        self
    }

    /// Adds every header in `headers`, replacing ones already set.
    #[must_use]
    pub fn headers(mut self, headers: http::HeaderMap) -> Self {
        self.headers.extend(headers);
        self
    }

    /// Sets the per-request timeout. Default: 10 seconds.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Builds a claim-check client.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a header that cannot be sent, a transport that
    /// could not be built, or a base URL that is not an absolute `http`/
    /// `https` URL.
    pub fn claim_check(self) -> Result<ClaimCheckClient<T>, ConfigError> {
        Ok(ClaimCheckClient::from_core(Arc::new(self.into_core()?)))
    }

    /// Builds a route-management client.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], as [`ClientBuilder::claim_check`] documents it.
    pub fn routes(self) -> Result<RoutesClient<T>, ConfigError> {
        Ok(RoutesClient::from_core(Arc::new(self.into_core()?)))
    }

    /// Builds an operator-API client.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], as [`ClientBuilder::claim_check`] documents it.
    pub fn admin(self) -> Result<AdminClient<T>, ConfigError> {
        Ok(AdminClient::from_core(Arc::new(self.into_core()?)))
    }

    fn into_core(self) -> Result<Core<T>, ConfigError> {
        // A misconfigured header is the most useful thing to report, then a
        // transport that would not build, then a URL that cannot be asked for.
        if let Some(error) = self.error {
            return Err(error);
        }
        // The transport settles first, so a transport that cannot be built is
        // reported ahead of a URL that cannot be asked for.
        let transport = self.transport?;
        let base_url = normalize_base_url(&self.base_url)?;
        Ok(Core {
            base_url,
            headers: self.headers,
            timeout: self.timeout,
            transport,
        })
    }
}

fn normalize_base_url(base_url: &str) -> Result<Box<str>, ConfigError> {
    let invalid = || ConfigError::InvalidBaseUrl {
        url: base_url.to_owned(),
    };
    // A trailing slash only ever produces `//health`, so it is dropped here
    // and every path below starts with one.
    let trimmed = base_url.trim_end_matches('/');
    let uri: http::Uri = trimmed.parse().map_err(|_| invalid())?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.authority().is_none() {
        return Err(invalid());
    }
    Ok(trimmed.into())
}

/// What every client holds: one connection target, one timeout, one transport.
///
/// Cloning a client clones an `Arc` of this, so a client is cheap to hand to
/// another task and never clones the transport.
pub(crate) struct Core<T> {
    pub(crate) base_url: Box<str>,
    pub(crate) headers: http::HeaderMap,
    pub(crate) timeout: Duration,
    pub(crate) transport: T,
}

impl<T: Transport> Core<T> {
    /// Sends one request, timed out by the client's own timeout.
    pub(crate) async fn send(
        &self,
        method: http::Method,
        path_and_query: &str,
        json_body: Option<Bytes>,
    ) -> Result<http::Response<Bytes>, UnavailableReason> {
        let uri = format!("{}{path_and_query}", self.base_url);
        let mut headers = self.headers.clone();
        if json_body.is_some() && !headers.contains_key(http::header::CONTENT_TYPE) {
            headers.insert(
                http::header::CONTENT_TYPE,
                http::header::HeaderValue::from_static("application/json"),
            );
        }
        let mut request = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(json_body.unwrap_or_default())
            // Unreachable: the base URL was parsed when the client was built,
            // and every path segment and query value is percent-encoded.
            .map_err(|err| UnavailableReason::Transport(TransportError::new(err)))?;
        *request.headers_mut() = headers;

        // The deadline is the client's own rather than the transport's, so it
        // applies to an injected transport too.
        match tokio::time::timeout(self.timeout, self.transport.send(request)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(err)) => Err(UnavailableReason::Transport(err)),
            Err(_elapsed) => Err(UnavailableReason::Timeout(self.timeout)),
        }
    }
}

// Debug by hand: header *values* are bearer tokens and must never be printed.
impl<T> fmt::Debug for Core<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Core")
            .field("base_url", &self.base_url)
            .field("header_names", &HeaderNames(&self.headers))
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// The names of a header map, without the values.
pub(crate) struct HeaderNames<'a>(pub(crate) &'a http::HeaderMap);

impl fmt::Debug for HeaderNames<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.keys().map(http::HeaderName::as_str))
            .finish()
    }
}

/// A path query under construction, in the order the fields are pushed.
#[derive(Debug, Default)]
pub(crate) struct Query(String);

impl Query {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Appends `key=value`, percent-encoding the value.
    pub(crate) fn push_str(&mut self, key: &'static str, value: &str) {
        self.push_key(key);
        self.0
            .push_str(&utf8_percent_encode(value, SEGMENT).to_string());
    }

    /// Appends `key=value` for a number or a bool (bools print as
    /// `true`/`false`, as the API expects).
    pub(crate) fn push_display(&mut self, key: &'static str, value: impl fmt::Display) {
        self.push_key(key);
        self.0.push_str(&value.to_string());
    }

    fn push_key(&mut self, key: &'static str) {
        self.0.push(if self.0.is_empty() { '?' } else { '&' });
        self.0.push_str(key);
        self.0.push('=');
    }

    /// `""` or `"?k=v&…"`, ready to append to a path.
    pub(crate) fn finish(self) -> String {
        self.0
    }
}

/// Serializes a request body.
pub(crate) fn json_body<S: Serialize + ?Sized>(value: &S) -> Result<Bytes, UnavailableReason> {
    // Unreachable for the request types here: every field is a string, a
    // number, a bool, or a map.
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(UnavailableReason::Json)
}

/// A rejected response body as JSON: the parsed value, or — when it is not
/// JSON at all — its text (empty body included).
pub(crate) fn error_body(body: &[u8]) -> serde_json::Value {
    if body.is_empty() {
        return serde_json::Value::String(String::new());
    }
    serde_json::from_slice(body)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(body).into_owned()))
}

/// Decodes a success body into the endpoint's type.
pub(crate) fn decode<R: DeserializeOwned>(body: &[u8]) -> Result<R, UnavailableReason> {
    serde_json::from_slice(body).map_err(UnavailableReason::Json)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A transport that answers every request the same way and records the
    //! request targets it saw.

    use std::sync::{Arc, Mutex};

    use bytes::Bytes;

    use crate::transport::{Transport, TransportError};

    /// Answers `status` with `body`, recording every request target.
    #[derive(Debug, Clone)]
    pub(crate) struct Recording {
        status: http::StatusCode,
        body: Bytes,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl Recording {
        pub(crate) fn new(status: u16, body: &str) -> Self {
            Self {
                status: http::StatusCode::from_u16(status).expect("valid status"),
                body: Bytes::copy_from_slice(body.as_bytes()),
                seen: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// A handle on the requests this transport was handed, past and future.
        pub(crate) fn targets(&self) -> Arc<Mutex<Vec<String>>> {
            Arc::clone(&self.seen)
        }
    }

    impl Transport for Recording {
        fn send(
            &self,
            request: http::Request<Bytes>,
        ) -> impl Future<Output = Result<http::Response<Bytes>, TransportError>> + Send {
            let status = self.status;
            let body = self.body.clone();
            let seen = Arc::clone(&self.seen);
            let target = request.uri().path_and_query().map(ToString::to_string);
            async move {
                seen.lock().expect("not poisoned").extend(target);
                http::Response::builder()
                    .status(status)
                    .body(body)
                    .map_err(TransportError::new)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientBuilder, ConfigError, Query, test_support::Recording};
    use crate::transport::TransportError;
    use std::time::Duration;

    #[test]
    fn build_rejects_base_url_without_scheme() {
        let builder = ClientBuilder::new("localhost:4001");
        assert!(matches!(
            builder.claim_check(),
            Err(ConfigError::InvalidBaseUrl { .. })
        ));
    }

    #[test]
    fn build_rejects_invalid_header_value() {
        let client = ClientBuilder::new("http://gateway.invalid")
            .header("x-a", "bad\nvalue")
            .claim_check();
        match client {
            Err(ConfigError::InvalidHeader { name }) => assert_eq!(name, "x-a"),
            other => panic!("expected InvalidHeader, got {other:?}"),
        }
    }

    #[test]
    fn build_reports_a_bad_header_before_a_bad_base_url() {
        let client = ClientBuilder::with_transport("localhost:4001", Recording::new(200, "{}"))
            .header("x-a", "bad\nvalue")
            .claim_check();
        match client {
            Err(ConfigError::InvalidHeader { name }) => assert_eq!(name, "x-a"),
            other => panic!("expected InvalidHeader, got {other:?}"),
        }
    }

    #[test]
    fn build_reports_a_transport_failure_before_a_bad_base_url() {
        let builder = ClientBuilder::<Recording>::connecting(
            "localhost:4001".to_owned(),
            Err(ConfigError::Transport(TransportError::new(
                "no TLS backend",
            ))),
        );
        match builder.claim_check() {
            Err(ConfigError::Transport(_)) => {}
            other => panic!("expected Transport, got {other:?}"),
        }
    }

    #[test]
    fn works_on_a_runtime_with_only_the_time_driver() {
        // Every request is bounded by `tokio::time::timeout`, so the runtime
        // needs the time driver and nothing more: no IO driver is required
        // while the transport is injected.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        let client = ClientBuilder::with_transport(
            "http://gateway.invalid",
            Recording::new(200, r#"{"status":"ok"}"#),
        )
        .claim_check()
        .expect("valid client");
        let health = runtime.block_on(client.health()).expect("healthy");
        assert_eq!(health.status, "ok");
    }

    #[test]
    fn debug_output_omits_header_values() {
        let builder =
            ClientBuilder::with_transport("http://gateway.invalid", Recording::new(200, "{}"))
                .header("authorization", "Bearer s3cret");
        let builder_debug = format!("{builder:?}");
        assert!(builder_debug.contains("authorization"), "{builder_debug}");
        assert!(!builder_debug.contains("s3cret"), "{builder_debug}");

        let client = builder.claim_check().expect("valid client");
        let client_debug = format!("{client:?}");
        assert!(client_debug.contains("authorization"), "{client_debug}");
        assert!(!client_debug.contains("s3cret"), "{client_debug}");
    }

    #[tokio::test]
    async fn trailing_slash_in_base_url_is_ignored() {
        let transport = Recording::new(200, r#"{"status":"ok"}"#);
        let targets = transport.targets();
        let client = ClientBuilder::with_transport("http://gateway.invalid/", transport)
            .timeout(Duration::from_secs(5))
            .claim_check()
            .expect("valid client");
        client.health().await.expect("healthy");
        assert_eq!(
            targets.lock().expect("not poisoned").as_slice(),
            ["/health"]
        );
    }

    #[test]
    fn query_percent_encodes_values() {
        let mut query = Query::new();
        query.push_display("enabled", true);
        query.push_str("cursor", "a b&c/d");
        assert_eq!(query.finish(), "?enabled=true&cursor=a%20b%26c%2Fd");
    }
}
