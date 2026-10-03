//! Decoding the `v: 1` queue message Ankusa publishes to every sink.
//!
//! A consumer receives the message JSON, decodes it with [`decode_message`],
//! and deduplicates on [`Message::idempotency_key`]. The message carries the
//! body inline (`body_base64`) or as a claim-check ref (`claim`), the body's
//! sha256, the provider's `dedupe_key`, the forwarded request `headers`, and
//! the `replay_id` when the delivery is a replay.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::claim_ref::parse_claim_ref;

/// A decoded `v: 1` queue message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The format version. Always `1`.
    pub v: u64,
    /// The envelope id, a `UUIDv7`.
    pub id: String,
    /// The source the hook was captured under.
    pub source_id: String,
    /// The tenant, when the source carries one.
    pub tenant_id: Option<String>,
    /// When the edge received the hook, unix milliseconds.
    pub received_at: i64,
    /// The captured content type, when the sender set one.
    pub content_type: Option<String>,
    /// Body size in bytes.
    pub size: u64,
    /// The decoded body bytes for the inline form; `None` for a claim.
    pub body: Option<Vec<u8>>,
    /// The claim-check ref for the claim form; `None` for an inline body.
    pub claim: Option<String>,
    /// Lowercase hex SHA-256 of the body, when the message carries one.
    pub sha256: Option<String>,
    /// The provider event key ingest extracted, when there is one.
    pub dedupe_key: Option<String>,
    /// The replay job id, when this delivery is a replay.
    pub replay_id: Option<String>,
    /// The forwarded provider request headers, lowercased.
    pub headers: BTreeMap<String, String>,
}

impl Message {
    /// The idempotency key for this delivery: `source_id:dedupe_key` when a
    /// non-empty `dedupe_key` is set, otherwise `id`.
    ///
    /// With `include_replay`, a non-null `replay_id` appends
    /// `#replay:<replay_id>`, so a consumer that must reprocess replays treats
    /// them as distinct. The default (`false`) drops replays of events it
    /// already processed.
    #[must_use]
    pub fn idempotency_key(&self, include_replay: bool) -> String {
        let mut key = match self.dedupe_key.as_deref().filter(|key| !key.is_empty()) {
            Some(dedupe_key) => format!("{}:{}", self.source_id, dedupe_key),
            None => self.id.clone(),
        };
        if include_replay {
            if let Some(replay_id) = self.replay_id.as_deref() {
                key.push_str("#replay:");
                key.push_str(replay_id);
            }
        }
        key
    }
}

/// A queue message that cannot be decoded.
///
/// Never retryable: the same bytes decode the same way forever, so send the
/// delivery to a dead-letter queue instead of asking for it again.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
#[error("invalid message ({code})")]
pub struct InvalidMessageError {
    /// The machine-readable failure code, e.g. `invalid_json` or
    /// `invalid_field`.
    pub code: &'static str,
    /// The offending key when `code` is `invalid_field`, else `None`.
    pub field: Option<&'static str>,
}

impl InvalidMessageError {
    fn new(code: &'static str, field: Option<&'static str>) -> Self {
        Self { code, field }
    }

    /// Whether calling again later could succeed. Always `false`.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        false
    }
}

/// Decodes a `v: 1` queue message.
///
/// Unknown keys are ignored. An absent `dedupe_key`, `replay_id` or `sha256`
/// decodes to `None`; an absent `headers` decodes to an empty map.
///
/// # Errors
///
/// [`InvalidMessageError`], never retryable, with `code` (and `field` for
/// `invalid_field`): `invalid_json`, `not_an_object`, `unsupported_version`,
/// `invalid_field`, `ambiguous_body`, `missing_body`, `invalid_body_base64`,
/// `size_mismatch`, `integrity`, or `tenant_mismatch`. The checks run in that
/// order and the first failure wins.
pub fn decode_message(data: &[u8]) -> Result<Message, InvalidMessageError> {
    let value: Value = serde_json::from_slice(data).map_err(|_| invalid("invalid_json", None))?;
    let Value::Object(map) = value else {
        return Err(invalid("not_an_object", None));
    };

    if map.get("v").and_then(Value::as_u64) != Some(1) {
        return Err(invalid("unsupported_version", None));
    }

    // Field types, in the contract's order.
    let id = required_string(&map, "id")?;
    if id.is_empty() {
        return Err(invalid("invalid_field", Some("id")));
    }
    let source_id = required_string(&map, "source_id")?;
    let received_at = map
        .get("received_at")
        .and_then(Value::as_i64)
        .ok_or_else(|| invalid("invalid_field", Some("received_at")))?;
    let size = map
        .get("size")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("invalid_field", Some("size")))?;
    let tenant_id = optional_string(&map, "tenant_id")?;
    let content_type = optional_string(&map, "content_type")?;
    let dedupe_key = optional_string(&map, "dedupe_key")?;
    let replay_id = optional_string(&map, "replay_id")?;
    let headers = headers_field(&map)?;
    let sha256 = sha256_field(&map)?;

    // Body form.
    let has_body = map.get("body_base64").is_some_and(|value| !value.is_null());
    let has_claim = map.get("claim").is_some_and(|value| !value.is_null());
    if has_body && has_claim {
        return Err(invalid("ambiguous_body", None));
    }
    if !has_body && !has_claim {
        return Err(invalid("missing_body", None));
    }

    let mut body = None;
    let mut claim = None;
    if has_body {
        let encoded = map
            .get("body_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("invalid_body_base64", None))?;
        let decoded = BASE64
            .decode(encoded)
            .map_err(|_| invalid("invalid_body_base64", None))?;
        if decoded.len() as u64 != size {
            return Err(invalid("size_mismatch", None));
        }
        if let Some(expected) = &sha256 {
            if &hex::encode(Sha256::digest(&decoded)) != expected {
                return Err(invalid("integrity", None));
            }
        }
        body = Some(decoded);
    } else {
        let claim_ref = map
            .get("claim")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("invalid_field", Some("claim")))?;
        let parsed =
            parse_claim_ref(claim_ref).map_err(|_| invalid("invalid_field", Some("claim")))?;
        if sha256.is_none() {
            return Err(invalid("invalid_field", Some("sha256")));
        }
        if let Some(tenant) = &tenant_id {
            if tenant != parsed.tenant_id() {
                return Err(invalid("tenant_mismatch", None));
            }
        }
        claim = Some(claim_ref.to_owned());
    }

    Ok(Message {
        v: 1,
        id,
        source_id,
        tenant_id,
        received_at,
        content_type,
        size,
        body,
        claim,
        sha256,
        dedupe_key,
        replay_id,
        headers,
    })
}

fn invalid(code: &'static str, field: Option<&'static str>) -> InvalidMessageError {
    InvalidMessageError::new(code, field)
}

fn required_string(
    map: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<String, InvalidMessageError> {
    match map.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        _ => Err(invalid("invalid_field", Some(key))),
    }
}

fn optional_string(
    map: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<Option<String>, InvalidMessageError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid("invalid_field", Some(key))),
    }
}

fn headers_field(
    map: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, String>, InvalidMessageError> {
    let Some(value) = map.get("headers") else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(headers) = value else {
        // `null` means "no headers"; any other non-object is a violation.
        return match value {
            Value::Null => Ok(BTreeMap::new()),
            _ => Err(invalid("invalid_field", Some("headers"))),
        };
    };
    let mut out = BTreeMap::new();
    for (name, value) in headers {
        match value.as_str() {
            Some(value) => out.insert(name.clone(), value.to_owned()),
            None => return Err(invalid("invalid_field", Some("headers"))),
        };
    }
    Ok(out)
}

/// `sha256` is absent, or 64 lowercase hex characters; an explicit `null` is
/// not a valid checksum.
fn sha256_field(
    map: &serde_json::Map<String, Value>,
) -> Result<Option<String>, InvalidMessageError> {
    match map.get("sha256") {
        None => Ok(None),
        Some(Value::String(value)) if is_lowercase_hex_64(value) => Ok(Some(value.clone())),
        _ => Err(invalid("invalid_field", Some("sha256"))),
    }
}

fn is_lowercase_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::{InvalidMessageError, decode_message};

    fn err(data: &str) -> InvalidMessageError {
        decode_message(data.as_bytes()).expect_err("invalid message")
    }

    #[test]
    fn decodes_an_inline_body_and_computes_the_key() {
        let message = decode_message(
            br#"{"v":1,"id":"01a0","source_id":"stripe","received_at":1,"size":5,
                 "body_base64":"aGVsbG8=","dedupe_key":"evt_1"}"#,
        )
        .expect("valid message");
        assert_eq!(message.body.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(message.idempotency_key(false), "stripe:evt_1");
        assert_eq!(message.idempotency_key(true), "stripe:evt_1");
    }

    #[test]
    fn replays_are_distinct_only_when_asked() {
        let message = decode_message(
            br#"{"v":1,"id":"01a0","source_id":"stripe","received_at":1,"size":5,
                 "body_base64":"aGVsbG8=","dedupe_key":"evt_1","replay_id":"rid-1"}"#,
        )
        .expect("valid message");
        assert_eq!(message.idempotency_key(false), "stripe:evt_1");
        assert_eq!(message.idempotency_key(true), "stripe:evt_1#replay:rid-1");
    }

    #[test]
    fn the_first_failure_wins() {
        assert_eq!(err("not json").code, "invalid_json");
        assert_eq!(err("[1,2,3]").code, "not_an_object");
        assert_eq!(err(r#"{"v":2}"#).code, "unsupported_version");
        let missing_id = err(r#"{"v":1,"source_id":"s","received_at":1,"size":0}"#);
        assert_eq!(missing_id.code, "invalid_field");
        assert_eq!(missing_id.field, Some("id"));
    }

    #[test]
    fn rejects_a_bad_checksum_and_a_size_mismatch() {
        let size = err(
            r#"{"v":1,"id":"01a0","source_id":"s","received_at":1,"size":6,
                "body_base64":"aGVsbG8="}"#,
        );
        assert_eq!(size.code, "size_mismatch");
        let digest = err(
            r#"{"v":1,"id":"01a0","source_id":"s","received_at":1,"size":5,
                "body_base64":"aGVsbG8=",
                "sha256":"d9298a10d1b0735837dc4bd85dac641b0f3cef27a47e5d53a54f2f3f5b2fcffa"}"#,
        );
        assert_eq!(digest.code, "integrity");
    }
}
