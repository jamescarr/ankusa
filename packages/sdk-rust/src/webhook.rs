//! The headers Ankusa's HTTP sink puts on a delivered hook.

use crate::message::KeyParts;

/// The `x-ankusa-*` headers of a delivered hook, borrowed from the header map
/// they were parsed out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookHeaders<'a> {
    /// `x-ankusa-id`: the envelope id, a `UUIDv7`.
    pub id: &'a str,
    /// `x-ankusa-source`: the source id, `""` when the header is absent.
    pub source: &'a str,
    /// `x-ankusa-tenant`, when the source carries one.
    pub tenant: Option<&'a str>,
    /// `content-type`, when the sender set one.
    pub content_type: Option<&'a str>,
    /// `x-ankusa-dedupe-key`: the provider event key, when the delivery has
    /// one. `None` when the header is absent or empty.
    pub dedupe_key: Option<&'a str>,
    /// `x-ankusa-replay-id`: the replay job id, when the delivery is a replay.
    /// `None` when the header is absent or empty.
    pub replay_id: Option<&'a str>,
    /// `x-ankusa-idempotency-key`: the tenant-scoped key Ankusa computed for
    /// the hook. `None` when the header is absent or empty (a sender that
    /// predates it). Read it through [`HookHeaders::idempotency_key`].
    pub idempotency_key: Option<&'a str>,
}

impl HookHeaders<'_> {
    /// The idempotency key for this delivery.
    ///
    /// It is the key Ankusa shipped in `x-ankusa-idempotency-key` when that is
    /// a non-empty string. For a delivery from a node that predates the
    /// header it is computed: `tenant:source:dedupe_key` (tenant `default`
    /// when there is none) for a non-empty `dedupe_key`, otherwise `id`.
    ///
    /// With `include_replay`, a non-null `replay_id` appends
    /// `#replay:<replay_id>`.
    #[must_use]
    pub fn idempotency_key(&self, include_replay: bool) -> String {
        KeyParts {
            shipped: self.idempotency_key,
            tenant: self.tenant,
            source: self.source,
            dedupe_key: self.dedupe_key,
            id: self.id,
            replay_id: self.replay_id,
        }
        .key(include_replay)
    }
}

/// Reads the hook headers out of `headers`.
///
/// Lookups are case-insensitive (`http::HeaderMap` normalizes names), and a
/// value that is not visible ASCII reads as absent.
///
/// # Errors
///
/// [`MissingHookIdError`] when `x-ankusa-id` is absent or empty: without it
/// the delivery cannot be deduplicated.
pub fn parse_headers(headers: &http::HeaderMap) -> Result<HookHeaders<'_>, MissingHookIdError> {
    let id = header(headers, "x-ankusa-id")
        .filter(|id| !id.is_empty())
        .ok_or(MissingHookIdError)?;
    Ok(HookHeaders {
        id,
        source: header(headers, "x-ankusa-source").unwrap_or(""),
        tenant: header(headers, "x-ankusa-tenant"),
        content_type: header(headers, "content-type"),
        dedupe_key: header(headers, "x-ankusa-dedupe-key").filter(|value| !value.is_empty()),
        replay_id: header(headers, "x-ankusa-replay-id").filter(|value| !value.is_empty()),
        idempotency_key: header(headers, "x-ankusa-idempotency-key")
            .filter(|value| !value.is_empty()),
    })
}

fn header<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

/// A delivery with no (or an empty) `x-ankusa-id` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("missing or empty x-ankusa-id header")]
pub struct MissingHookIdError;

#[cfg(test)]
mod tests {
    use super::{MissingHookIdError, parse_headers};
    use http::{HeaderMap, HeaderValue};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                http::HeaderName::from_bytes(name.as_bytes()).expect("valid header name"),
                HeaderValue::from_str(value).expect("valid header value"),
            );
        }
        map
    }

    #[test]
    fn empty_tenant_stays_present() {
        let map = headers(&[("x-ankusa-id", "01a0"), ("x-ankusa-tenant", "")]);
        let parsed = parse_headers(&map).expect("id present");
        assert_eq!(parsed.tenant, Some(""));
        assert_eq!(parsed.source, "");
        assert_eq!(parsed.content_type, None);
    }

    #[test]
    fn non_ascii_id_reads_as_absent() {
        let mut map = HeaderMap::new();
        map.insert(
            http::HeaderName::from_static("x-ankusa-id"),
            HeaderValue::from_bytes(b"caf\xc3\xa9").expect("valid header value"),
        );
        assert!(matches!(parse_headers(&map), Err(MissingHookIdError)));
    }

    #[test]
    fn header_names_are_matched_case_insensitively() {
        let map = headers(&[("X-Ankusa-Id", "01a0"), ("Content-Type", "text/plain")]);
        let parsed = parse_headers(&map).expect("id present");
        assert_eq!(parsed.id, "01a0");
        assert_eq!(parsed.content_type, Some("text/plain"));
    }

    #[test]
    fn dedupe_and_replay_headers_decode_and_empty_means_absent() {
        let map = headers(&[
            ("x-ankusa-id", "01a0"),
            ("x-ankusa-source", "stripe"),
            ("x-ankusa-dedupe-key", "evt_1"),
            ("x-ankusa-replay-id", "rid-1"),
        ]);
        let parsed = parse_headers(&map).expect("id present");
        assert_eq!(parsed.dedupe_key, Some("evt_1"));
        assert_eq!(parsed.replay_id, Some("rid-1"));
        assert_eq!(parsed.idempotency_key(false), "default:stripe:evt_1");
        assert_eq!(
            parsed.idempotency_key(true),
            "default:stripe:evt_1#replay:rid-1"
        );

        let empty = headers(&[
            ("x-ankusa-id", "01a0"),
            ("x-ankusa-dedupe-key", ""),
            ("x-ankusa-replay-id", ""),
        ]);
        let parsed = parse_headers(&empty).expect("id present");
        assert_eq!(parsed.dedupe_key, None);
        assert_eq!(parsed.replay_id, None);
        assert_eq!(parsed.idempotency_key(true), "01a0");
    }

    #[test]
    fn the_shipped_key_wins_and_empty_means_absent() {
        let map = headers(&[
            ("x-ankusa-id", "01a0"),
            ("x-ankusa-source", "stripe"),
            ("x-ankusa-tenant", "globex"),
            ("x-ankusa-dedupe-key", "evt_1"),
            ("x-ankusa-replay-id", "rid-1"),
            ("x-ankusa-idempotency-key", "acme:stripe:evt_1"),
        ]);
        let parsed = parse_headers(&map).expect("id present");
        assert_eq!(parsed.idempotency_key, Some("acme:stripe:evt_1"));
        assert_eq!(parsed.idempotency_key(false), "acme:stripe:evt_1");
        assert_eq!(
            parsed.idempotency_key(true),
            "acme:stripe:evt_1#replay:rid-1"
        );

        let empty = headers(&[
            ("x-ankusa-id", "01a0"),
            ("x-ankusa-source", "stripe"),
            ("x-ankusa-tenant", "acme"),
            ("x-ankusa-dedupe-key", "evt_1"),
            ("x-ankusa-idempotency-key", ""),
        ]);
        let parsed = parse_headers(&empty).expect("id present");
        assert_eq!(parsed.idempotency_key, None);
        assert_eq!(parsed.idempotency_key(false), "acme:stripe:evt_1");
    }
}
