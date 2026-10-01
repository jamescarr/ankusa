//! The transport hook: how a request reaches the network.

use std::error::Error as StdError;
use std::fmt;

use bytes::Bytes;

use crate::client::ConfigError;

/// A pluggable HTTP transport.
///
/// The default is [`ReqwestTransport`]. Implementations must not follow
/// redirects: every client reports a `3xx` as "unavailable, retryable", which
/// only holds while a redirect is reported rather than chased.
///
/// Replacing the transport does not replace the runtime: every request is
/// still bounded by the client's own `tokio::time::timeout`, so a Tokio
/// runtime with its timers is required whatever the implementation.
pub trait Transport: Send + Sync + 'static {
    /// Sends `request` and returns the response, headers and body included.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] for any failure to produce a response: name
    /// resolution, connection, TLS, or a body that could not be read.
    fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> impl Future<Output = Result<http::Response<Bytes>, TransportError>> + Send;
}

/// A transport failure, with the underlying error kept as its source.
pub struct TransportError(Box<dyn StdError + Send + Sync + 'static>);

impl TransportError {
    /// Wraps a transport failure.
    #[must_use]
    pub fn new(err: impl Into<Box<dyn StdError + Send + Sync + 'static>>) -> Self {
        Self(err.into())
    }
}

// Hand-written rather than derived: `Box<dyn Error>` is a Debug and a Display
// itself, so a `transparent` error cannot reach through it the way thiserror's
// derive needs.
impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TransportError").field(&self.0).finish()
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("transport failed")
    }
}

impl StdError for TransportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.0)
    }
}

/// The default [`Transport`]: [`reqwest`], with redirects disabled.
#[derive(Debug, Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    /// Builds a transport backed by a new `reqwest` client.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Transport`] if the underlying client cannot be built —
    /// in practice only when no TLS backend is compiled in.
    pub fn new() -> Result<Self, ConfigError> {
        let client = reqwest::Client::builder()
            // A 3xx has to reach the caller: the clients classify it as
            // "unavailable" rather than silently landing somewhere else.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|err| ConfigError::Transport(TransportError::new(err)))?;
        Ok(Self { client })
    }

    /// Wraps an existing `reqwest` client.
    ///
    /// The caller owns its configuration; it must not follow redirects, or a
    /// `3xx` will be chased instead of reported and the retryable/
    /// non-retryable classification breaks.
    #[must_use]
    pub fn from_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Transport for ReqwestTransport {
    fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> impl Future<Output = Result<http::Response<Bytes>, TransportError>> + Send {
        let client = self.client.clone();
        async move {
            let request = reqwest::Request::try_from(request).map_err(TransportError::new)?;
            let response = client.execute(request).await.map_err(TransportError::new)?;
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.bytes().await.map_err(TransportError::new)?;
            http::Response::builder()
                .status(status)
                .body(body)
                .map(|mut response| {
                    *response.headers_mut() = headers;
                    response
                })
                .map_err(TransportError::new)
        }
    }
}
