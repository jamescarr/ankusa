//! The headers Ankusa's HTTP sink puts on a delivered hook.

/// The `x-ankusa-*` headers of a delivered hook, borrowed from the header map
/// they were parsed out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookHeaders<'a> {
    /// `x-ankusa-id`: the envelope id, a `UUIDv7`. Dedupe on this.
    pub id: &'a str,
    /// `x-ankusa-source`: the source id, `""` when the header is absent.
    pub source: &'a str,
    /// `x-ankusa-tenant`, when the source carries one.
    pub tenant: Option<&'a str>,
    /// `content-type`, when the sender set one.
    pub content_type: Option<&'a str>,
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
}
