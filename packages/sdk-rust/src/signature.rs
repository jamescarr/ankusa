//! Verify the Standard Webhooks signature an HTTP sink with a `secret` adds
//! (<https://www.standardwebhooks.com/>).
//!
//! `webhook-signature` holds space-separated `v1,<base64>` entries, each an
//! HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`. A delivery
//! passes when any `v1` entry matches any configured secret (several during
//! a rotation), compared in constant time, and `webhook-timestamp` is within
//! the tolerance of now.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// The default `webhook-timestamp` window, in seconds either side of now.
pub const DEFAULT_TOLERANCE_SECONDS: u64 = 300;

/// A delivery whose signature does not verify.
///
/// Never retryable: answer `401`; the sender retries with the same bytes,
/// which will not verify either.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
#[error("invalid webhook signature ({code})")]
pub struct InvalidSignatureError {
    /// `invalid_secret`, `missing_header`, `invalid_timestamp`,
    /// `timestamp_out_of_tolerance` or `no_matching_signature`.
    pub code: &'static str,
    /// The header at fault, or `None` for `invalid_secret`.
    pub field: Option<&'static str>,
}

impl InvalidSignatureError {
    fn new(code: &'static str, field: Option<&'static str>) -> Self {
        Self { code, field }
    }

    /// Whether calling again later could succeed. Always `false`.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        false
    }
}

/// A verified delivery's `webhook-id` and `webhook-timestamp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSignature<'a> {
    /// `webhook-id`.
    pub id: &'a str,
    /// `webhook-timestamp`, unix seconds.
    pub timestamp: u64,
}

/// How [`verify_signature`] judges the timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifyOptions {
    /// Seconds either side of `now` a timestamp may be.
    pub tolerance_seconds: u64,
    /// Unix seconds to judge against; `None` reads the clock.
    pub now: Option<u64>,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            tolerance_seconds: DEFAULT_TOLERANCE_SECONDS,
            now: None,
        }
    }
}

/// Verifies one delivery. `body` must be the raw bytes received; `secrets`
/// are `whsec_` + base64, or any other string used as its own bytes.
///
/// # Errors
///
/// [`InvalidSignatureError`], never retryable. The checks run in this order:
/// `invalid_secret` (no secret, or a `whsec_` secret that is not base64),
/// `missing_header` (`webhook-id`, `webhook-timestamp`, `webhook-signature`),
/// `invalid_timestamp`, `timestamp_out_of_tolerance`,
/// `no_matching_signature`.
pub fn verify_signature<'a, S: AsRef<str>>(
    headers: &'a http::HeaderMap,
    body: &[u8],
    secrets: &[S],
    options: VerifyOptions,
) -> Result<VerifiedSignature<'a>, InvalidSignatureError> {
    let keys = decode_secrets(secrets)?;

    let required = |name: &'static str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| InvalidSignatureError::new("missing_header", Some(name)))
    };
    let id = required("webhook-id")?;
    let raw_timestamp = required("webhook-timestamp")?;
    let signature = required("webhook-signature")?;

    let timestamp = if raw_timestamp.bytes().all(|b| b.is_ascii_digit()) {
        raw_timestamp.parse::<u64>().ok()
    } else {
        None
    }
    .ok_or_else(|| InvalidSignatureError::new("invalid_timestamp", Some("webhook-timestamp")))?;

    let now = options.now.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs())
    });
    if now.abs_diff(timestamp) > options.tolerance_seconds {
        return Err(InvalidSignatureError::new(
            "timestamp_out_of_tolerance",
            Some("webhook-timestamp"),
        ));
    }

    let candidates: Vec<Vec<u8>> = signature
        .split(' ')
        .filter_map(|entry| entry.strip_prefix("v1,"))
        .filter_map(|encoded| BASE64.decode(encoded).ok())
        .collect();

    for key in &keys {
        for candidate in &candidates {
            let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
                .map_err(|_| InvalidSignatureError::new("invalid_secret", None))?;
            mac.update(id.as_bytes());
            mac.update(b".");
            mac.update(raw_timestamp.as_bytes());
            mac.update(b".");
            mac.update(body);
            // `verify_slice` compares in constant time.
            if mac.verify_slice(candidate).is_ok() {
                return Ok(VerifiedSignature { id, timestamp });
            }
        }
    }

    Err(InvalidSignatureError::new(
        "no_matching_signature",
        Some("webhook-signature"),
    ))
}

fn decode_secrets<S: AsRef<str>>(secrets: &[S]) -> Result<Vec<Vec<u8>>, InvalidSignatureError> {
    if secrets.is_empty() {
        return Err(InvalidSignatureError::new("invalid_secret", None));
    }
    secrets
        .iter()
        .map(|secret| {
            let secret = secret.as_ref();
            match secret.strip_prefix("whsec_") {
                Some(encoded) => BASE64
                    .decode(encoded)
                    .ok()
                    .filter(|key| !key.is_empty())
                    .ok_or_else(|| InvalidSignatureError::new("invalid_secret", None)),
                None if secret.is_empty() => {
                    Err(InvalidSignatureError::new("invalid_secret", None))
                }
                None => Ok(secret.as_bytes().to_vec()),
            }
        })
        .collect()
}
