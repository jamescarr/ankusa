//! The receiver-side decision on one HTTP-sink delivery: is it signed, and
//! what is its identity.
//!
//! Ankusa's HTTP sink signs every delivery unless it is configured
//! `allow_unsigned: true`, so a receiver is closed by default: without the
//! sink's secret (or an explicit `allow_unsigned`) it refuses every delivery.
//! Otherwise any host that can reach the receiver's URL could inject hooks.

use crate::signature::{
    DEFAULT_TOLERANCE_SECONDS, InvalidSignatureError, VerifyOptions, verify_signature,
};
use crate::webhook::{HookHeaders, MissingHookIdError, parse_headers};

/// How [`receive_delivery`] judges a delivery. The default refuses every
/// delivery: no secret and no `allow_unsigned`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceiveOptions {
    /// The sink's secrets (`whsec_` + base64, or any other string used as its
    /// own bytes); several during a rotation. With any, the delivery must
    /// verify, and `allow_unsigned` does not weaken that.
    pub secrets: Vec<String>,
    /// Accept a delivery when no secret is configured, unverified. Any host
    /// that can reach the receiver's URL can then inject hooks.
    pub allow_unsigned: bool,
    /// Seconds either side of `now` a timestamp may be; `None` is
    /// [`DEFAULT_TOLERANCE_SECONDS`].
    pub tolerance_seconds: Option<u64>,
    /// Unix seconds to judge against; `None` reads the clock.
    pub now: Option<u64>,
}

/// An accepted delivery: its identity, and whether its signature verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedDelivery<'a> {
    /// The `x-ankusa-*` headers.
    pub headers: HookHeaders<'a>,
    /// `false` only for an unsigned delivery `allow_unsigned` let through.
    pub verified: bool,
}

/// Why a delivery was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReceiveError {
    /// No valid signature: answer `401`.
    #[error(transparent)]
    Signature(#[from] InvalidSignatureError),
    /// No `x-ankusa-id`: answer `400`.
    #[error(transparent)]
    MissingHookId(#[from] MissingHookIdError),
}

/// Judges one delivery. `body` must be the raw bytes received.
///
/// # Errors
///
/// [`ReceiveError::Signature`] when there is no valid signature, including
/// `invalid_secret` when no secret is configured and `allow_unsigned` is not
/// set; [`ReceiveError::MissingHookId`] when `x-ankusa-id` is absent. The
/// signature is judged first.
pub fn receive_delivery<'a>(
    headers: &'a http::HeaderMap,
    body: &[u8],
    options: &ReceiveOptions,
) -> Result<ReceivedDelivery<'a>, ReceiveError> {
    let verified = if options.secrets.is_empty() {
        if !options.allow_unsigned {
            return Err(InvalidSignatureError::new("invalid_secret", None).into());
        }
        false
    } else {
        let verify = VerifyOptions {
            tolerance_seconds: options
                .tolerance_seconds
                .unwrap_or(DEFAULT_TOLERANCE_SECONDS),
            now: options.now,
        };
        verify_signature(headers, body, &options.secrets, verify)?;
        true
    };

    Ok(ReceivedDelivery {
        headers: parse_headers(headers)?,
        verified,
    })
}
