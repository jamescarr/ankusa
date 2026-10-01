//! The claim-check gateway client: redeem a claim's bytes, or probe it.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::claim_ref::{InvalidClaimRefError, parse_claim_ref};
use crate::client::{ClientBuilder, ConfigError, Core, UnavailableReason, decode, error_body};
use crate::transport::{ReqwestTransport, Transport};

/// Client for the claim-check gateway (`claim_check.port`): where a queue
/// worker collects the body a hook was too large to carry.
pub struct ClaimCheckClient<T = ReqwestTransport> {
    core: Arc<Core<T>>,
}

impl<T> ClaimCheckClient<T> {
    pub(crate) fn from_core(core: Arc<Core<T>>) -> Self {
        Self { core }
    }
}

// Cloning shares the core, so a client is cheap to clone and does not require
// the transport to be cloneable.
impl<T> Clone for ClaimCheckClient<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> fmt::Debug for ClaimCheckClient<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClaimCheckClient")
            .field("base_url", &self.core.base_url)
            .field(
                "header_names",
                &crate::client::HeaderNames(&self.core.headers),
            )
            .field("timeout", &self.core.timeout)
            .finish_non_exhaustive()
    }
}

impl ClaimCheckClient {
    /// Builds a client for `base_url` (`http://host:port`), with the default
    /// 10-second timeout.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when `base_url` is not an absolute `http`/`https` URL,
    /// or the HTTP client cannot be built.
    pub fn new(base_url: impl Into<String>) -> Result<Self, ConfigError> {
        ClientBuilder::new(base_url).claim_check()
    }
}

impl<T: Transport> ClaimCheckClient<T> {
    /// Fetches the claim's bytes and verifies them against `sha256`.
    ///
    /// `sha256` is the 64-character lowercase hex digest the queue message
    /// carries. Nothing is requested unless the ref and the digest are both
    /// well formed.
    ///
    /// # Errors
    ///
    /// - [`ClaimCheckError::InvalidRef`] / [`ClaimCheckError::InvalidSha256`]:
    ///   the arguments are malformed; no request is sent.
    /// - [`ClaimCheckError::NotFound`]: the claim is gone (already redeemed, or
    ///   swept after its TTL).
    /// - [`ClaimCheckError::Rejected`]: the gateway refused the request.
    /// - [`ClaimCheckError::Integrity`]: the bytes do not match `sha256`.
    /// - [`ClaimCheckError::Unavailable`]: no usable answer; retryable.
    pub async fn redeem(&self, claim_ref: &str, sha256: &str) -> Result<Bytes, ClaimCheckError> {
        let parsed = parse_claim_ref(claim_ref)?;
        let expected = decode_sha256(sha256)?;
        let response = self
            .core
            .send(http::Method::GET, &parsed.path(), None)
            .await
            .map_err(ClaimCheckError::Unavailable)?;

        let status = response.status();
        // Only a 200 is the claim itself: a 201/204 body would be some other
        // message's, and verifying it against this sha256 would report an
        // integrity failure that is not the sender's fault.
        if status == http::StatusCode::OK {
            let digest = Sha256::digest(response.body());
            if digest.as_slice() != expected.as_slice() {
                return Err(ClaimCheckError::Integrity {
                    expected: sha256.to_owned(),
                    actual: hex::encode(digest),
                });
            }
            return Ok(response.into_body());
        }
        if status == http::StatusCode::NOT_FOUND {
            return Err(ClaimCheckError::NotFound);
        }
        if status.is_client_error() {
            return Err(ClaimCheckError::Rejected {
                status,
                body: error_body(response.body()),
            });
        }
        Err(ClaimCheckError::Unavailable(UnavailableReason::Status(
            status,
        )))
    }

    /// Probes the gateway's health endpoint.
    ///
    /// # Errors
    ///
    /// [`ClaimCheckError::Unavailable`] for anything but a `200` with a JSON
    /// body — a `201`, a redirect, a `5xx`, a transport failure, or no answer
    /// within the timeout.
    pub async fn health(&self) -> Result<ClaimCheckHealth, ClaimCheckError> {
        let response = self
            .core
            .send(http::Method::GET, "/health", None)
            .await
            .map_err(ClaimCheckError::Unavailable)?;
        if response.status() != http::StatusCode::OK {
            return Err(ClaimCheckError::Unavailable(UnavailableReason::Status(
                response.status(),
            )));
        }
        decode(response.body()).map_err(ClaimCheckError::Unavailable)
    }
}

/// `GET /health`: the gateway is up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClaimCheckHealth {
    /// Always `"ok"` from a healthy gateway.
    pub status: String,
}

/// Why a claim could not be redeemed, or the gateway could not be reached.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClaimCheckError {
    /// The ref is not `urn:ankusa:claim:v1:<tenant>:<claim_id>`.
    #[error(transparent)]
    InvalidRef(#[from] InvalidClaimRefError),
    /// The expected digest is not 64 lowercase hex characters.
    #[error("invalid sha256 {sha256:?}: want 64 lowercase hex characters")]
    InvalidSha256 {
        /// The rejected digest, as passed.
        sha256: String,
    },
    /// The gateway has no such claim: already redeemed, or swept.
    #[error("claim not found")]
    NotFound,
    /// The gateway refused the request; `body` is its error payload (the
    /// parsed JSON, or its text when it was not JSON at all).
    #[error("claim-check gateway rejected the request (HTTP {status})")]
    Rejected {
        /// The response status.
        status: http::StatusCode,
        /// The response body, as JSON when it parsed.
        body: serde_json::Value,
    },
    /// The bytes the gateway returned do not hash to the expected digest.
    #[error("claim body does not match sha256 {expected}")]
    Integrity {
        /// The digest the caller expected.
        expected: String,
        /// The digest the returned bytes actually have.
        actual: String,
    },
    /// No usable answer: a `5xx`, an unfollowed redirect, a timeout, or a
    /// transport failure.
    #[error("claim-check gateway unavailable")]
    Unavailable(#[source] UnavailableReason),
}

impl ClaimCheckError {
    /// Whether calling again later could succeed.
    ///
    /// Only [`ClaimCheckError::Unavailable`] is retryable: a missing claim, a
    /// rejected request, and an integrity mismatch all need the message to be
    /// dead-lettered instead.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

/// The expected digest: 64 lowercase hex characters, decoded into 32 bytes so
/// the comparison never allocates.
fn decode_sha256(sha256: &str) -> Result<[u8; 32], ClaimCheckError> {
    let invalid = || ClaimCheckError::InvalidSha256 {
        sha256: sha256.to_owned(),
    };
    let bytes = sha256.as_bytes();
    if bytes.len() != 64
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid());
    }
    let mut digest = [0_u8; 32];
    hex::decode_to_slice(sha256, &mut digest).map_err(|_| invalid())?;
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use super::{ClaimCheckError, decode_sha256};

    const HEX: &str = "8e1bed597394cf01672e49232d9929c8bc6b3d6ea4e2f489517ec88701f01581";

    #[test]
    fn decodes_a_lowercase_digest() {
        let digest = decode_sha256(HEX).expect("valid digest");
        assert_eq!(digest[0], 0x8e);
        assert_eq!(digest[31], 0x81);
    }

    #[test]
    fn refuses_other_digest_shapes() {
        let rejected = [
            String::new(),
            HEX[..63].to_owned(),
            format!("{HEX}0"),
            HEX.to_uppercase(),
            "g".repeat(64),
            format!("sha256-{HEX}"),
        ];
        for bad in &rejected {
            let err = decode_sha256(bad).expect_err("invalid digest");
            assert!(matches!(err, ClaimCheckError::InvalidSha256 { .. }));
        }
    }
}
