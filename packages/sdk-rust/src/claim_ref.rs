//! The claim-check ref: `urn:ankusa:claim:v1:<tenant>:<claim_id>`.

/// The ref's prefix: everything before the tenant.
const PREFIX: &str = "urn:ankusa:claim:v1:";

/// A claim-check ref, split into the parts the gateway path is built from.
///
/// Borrows from the ref it was parsed from, so parsing allocates nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParsedClaimRef<'a> {
    tenant_id: &'a str,
    claim_id: &'a str,
}

impl<'a> ParsedClaimRef<'a> {
    /// The tenant the claim belongs to.
    #[must_use]
    pub fn tenant_id(&self) -> &'a str {
        self.tenant_id
    }

    /// The claim's canonical uppercase ULID.
    #[must_use]
    pub fn claim_id(&self) -> &'a str {
        self.claim_id
    }

    /// The gateway path that returns the claim's bytes:
    /// `/v1/claims/{tenant_id}/{claim_id}`.
    #[must_use]
    pub fn path(&self) -> String {
        format!("/v1/claims/{}/{}", self.tenant_id, self.claim_id)
    }
}

/// Parses `urn:ankusa:claim:v1:<tenant>:<claim_id>`.
///
/// The tenant is 1–64 characters of `A-Za-z0-9_-`, and the claim id is a
/// canonical 26-character Crockford base32 ULID (`0-7` first, uppercase, and
/// with `I`, `L`, `O`, and `U` never used). Anything else — an extra `:`
/// segment, a lowercase or 25-character ULID, a trailing newline — is
/// refused.
///
/// # Errors
///
/// [`InvalidClaimRefError`] when `claim_ref` is not exactly that shape. The
/// error carries the rejected input.
pub fn parse_claim_ref(claim_ref: &str) -> Result<ParsedClaimRef<'_>, InvalidClaimRefError> {
    let invalid = || InvalidClaimRefError {
        input: claim_ref.to_owned(),
    };
    let rest = claim_ref.strip_prefix(PREFIX).ok_or_else(invalid)?;
    // No second `:` may follow: the claim id's charset has no `:`, so an extra
    // segment fails below rather than being silently truncated here.
    let (tenant_id, claim_id) = rest.split_once(':').ok_or_else(invalid)?;

    let tenant_ok = !tenant_id.is_empty()
        && tenant_id.len() <= 64
        && tenant_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if !tenant_ok || !is_ulid(claim_id) {
        return Err(invalid());
    }
    Ok(ParsedClaimRef {
        tenant_id,
        claim_id,
    })
}

/// A canonical Crockford base32 ULID: 26 characters, first one `0`–`7`,
/// `I`/`L`/`O`/`U` never used.
fn is_ulid(claim_id: &str) -> bool {
    let bytes = claim_id.as_bytes();
    bytes.len() == 26
        && matches!(bytes[0], b'0'..=b'7')
        && bytes[1..].iter().all(|byte| {
            matches!(byte, b'0'..=b'9' | b'A'..=b'H' | b'J' | b'K' | b'M' | b'N' | b'P'..=b'T' | b'V'..=b'Z')
        })
}

/// A claim-check ref that is not `urn:ankusa:claim:v1:<tenant>:<claim_id>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid claim-check ref: {input:?}")]
pub struct InvalidClaimRefError {
    input: String,
}

impl InvalidClaimRefError {
    /// The ref that was refused.
    #[must_use]
    pub fn input(&self) -> &str {
        &self.input
    }
}

#[cfg(test)]
mod tests {
    use super::{InvalidClaimRefError, parse_claim_ref};

    const OK: &str = "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002";

    #[test]
    fn parses_a_canonical_ref() {
        let parsed = parse_claim_ref(OK).expect("valid ref");
        assert_eq!(parsed.tenant_id(), "acme");
        assert_eq!(parsed.claim_id(), "01M39VMD8RA3C5HR4RBV67Y002");
        assert_eq!(parsed.path(), "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002");
    }

    #[test]
    fn rejects_a_tenant_over_64_characters() {
        let long = "a".repeat(65);
        let claim_ref = format!("urn:ankusa:claim:v1:{long}:01M39VMD8RA3C5HR4RBV67Y002");
        let err = parse_claim_ref(&claim_ref);
        assert!(matches!(err, Err(InvalidClaimRefError { .. })));
    }

    #[test]
    fn accepts_an_ascii_boundary_id() {
        let parsed = parse_claim_ref("urn:ankusa:claim:v1:acme:7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
            .expect("valid ref");
        assert_eq!(parsed.claim_id(), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
    }

    #[test]
    fn error_carries_the_rejected_input() {
        let err = parse_claim_ref("not-a-ref").expect_err("invalid ref");
        assert_eq!(err.input(), "not-a-ref");
        assert_eq!(err.to_string(), r#"invalid claim-check ref: "not-a-ref""#);
    }

    #[test]
    fn accepts_a_64_character_tenant() {
        let tenant = "a".repeat(64);
        let claim_ref = format!("urn:ankusa:claim:v1:{tenant}:01M39VMD8RA3C5HR4RBV67Y002");
        let parsed = parse_claim_ref(&claim_ref).expect("valid ref");
        assert_eq!(parsed.tenant_id().len(), 64);
    }
}
