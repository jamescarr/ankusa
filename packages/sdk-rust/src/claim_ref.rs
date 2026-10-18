//! The claim-check ref: `urn:ankusa:claim:v1:<tenant>:<claim_id>`, or the
//! signed, expiring `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`.

/// The v1 ref's prefix: everything before the tenant.
const PREFIX: &str = "urn:ankusa:claim:v1:";

/// The v2 ref's prefix: everything before the tenant.
const PREFIX_V2: &str = "urn:ankusa:claim:v2:";

/// A claim-check ref, split into the parts the gateway path is built from.
///
/// Borrows from the ref it was parsed from, so parsing allocates nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParsedClaimRef<'a> {
    tenant_id: &'a str,
    claim_id: &'a str,
    /// A v2 ref's expiry (Unix seconds) and signature. The gateway checks
    /// both; the SDK only carries them.
    proof: Option<(&'a str, &'a str)>,
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
    /// `/v1/claims/{tenant_id}/{claim_id}`, followed by
    /// `?exp={exp_unix}&sig={signature}` for a v2 ref.
    #[must_use]
    pub fn path(&self) -> String {
        match self.proof {
            None => format!("/v1/claims/{}/{}", self.tenant_id, self.claim_id),
            Some((exp, sig)) => format!(
                "/v1/claims/{}/{}?exp={exp}&sig={sig}",
                self.tenant_id, self.claim_id
            ),
        }
    }
}

/// Parses `urn:ankusa:claim:v1:<tenant>:<claim_id>` and
/// `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`.
///
/// The tenant is 1–64 characters of `A-Za-z0-9_-`, and the claim id is a
/// canonical 26-character Crockford base32 ULID (`0-7` first, uppercase, and
/// with `I`, `L`, `O`, and `U` never used). A v2 ref's `exp_unix` is a decimal
/// of 1–12 digits with no leading zero, and its signature is 43 unpadded
/// base64url characters; neither is checked here, the gateway does. Anything
/// else — an extra `:` segment, a lowercase or 25-character ULID, a trailing
/// newline — is refused.
///
/// # Errors
///
/// [`InvalidClaimRefError`] when `claim_ref` is not exactly that shape. The
/// error carries the rejected input.
pub fn parse_claim_ref(claim_ref: &str) -> Result<ParsedClaimRef<'_>, InvalidClaimRefError> {
    let invalid = || InvalidClaimRefError {
        input: claim_ref.to_owned(),
    };
    let (rest, signed) = match claim_ref.strip_prefix(PREFIX_V2) {
        Some(rest) => (rest, true),
        None => (claim_ref.strip_prefix(PREFIX).ok_or_else(invalid)?, false),
    };
    // No second `:` may follow the claim id of a v1 ref, nor a third the
    // signature of a v2 one: neither charset has a `:`, so an extra segment
    // fails below rather than being silently truncated here.
    let (tenant_id, rest) = rest.split_once(':').ok_or_else(invalid)?;
    let (claim_id, proof) = if signed {
        let (claim_id, rest) = rest.split_once(':').ok_or_else(invalid)?;
        let (exp, sig) = rest.split_once(':').ok_or_else(invalid)?;
        if !is_exp(exp) || !is_signature(sig) {
            return Err(invalid());
        }
        (claim_id, Some((exp, sig)))
    } else {
        (rest, None)
    };

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
        proof,
    })
}

/// A v2 ref's expiry: 1–12 decimal digits, the first not `0`.
fn is_exp(exp: &str) -> bool {
    !exp.is_empty()
        && exp.len() <= 12
        && !exp.starts_with('0')
        && exp.bytes().all(|byte| byte.is_ascii_digit())
}

/// A v2 ref's signature: 43 base64url characters, unpadded.
fn is_signature(sig: &str) -> bool {
    sig.len() == 43
        && sig
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
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

    const SIG: &str = "MAmydaNzNwA3sV38ggZG15ElUTajA8QtBkKevsaq1Ac";

    fn v2(tenant: &str, exp: &str, sig: &str) -> String {
        format!("urn:ankusa:claim:v2:{tenant}:01M39VMD8RA3C5HR4RBV67Y002:{exp}:{sig}")
    }

    #[test]
    fn parses_a_signed_ref_and_carries_the_proof_in_the_path() {
        let claim_ref = v2("acme", "1790345000", SIG);
        let parsed = parse_claim_ref(&claim_ref).expect("valid v2 ref");
        assert_eq!(parsed.tenant_id(), "acme");
        assert_eq!(parsed.claim_id(), "01M39VMD8RA3C5HR4RBV67Y002");
        assert_eq!(
            parsed.path(),
            format!("/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002?exp=1790345000&sig={SIG}")
        );
    }

    #[test]
    fn rejects_a_signed_ref_outside_the_grammar() {
        let bad = [
            "urn:ankusa:claim:v2:acme:01M39VMD8RA3C5HR4RBV67Y002".to_owned(),
            "urn:ankusa:claim:v2:acme:01M39VMD8RA3C5HR4RBV67Y002:1790345000".to_owned(),
            format!("{}:extra", v2("acme", "1790345000", SIG)),
            v2("acme", "0", SIG),
            v2("acme", "01790345000", SIG),
            v2("acme", "1000000000000", SIG),
            v2("acme", "-1", SIG),
            v2("acme", "soon", SIG),
            v2("acme", "1", &SIG[..42]),
            v2("acme", "1", &format!("{SIG}A")),
            v2("acme", "1", &format!("{SIG}=")),
            v2("acme", "1", &SIG.replace('A', "+")),
            v2("bad.tenant", "1", SIG),
            format!("{}\n", v2("acme", "1", SIG)),
            v2("acme", "1", SIG).replace(":v2:", ":v3:"),
        ];
        for claim_ref in bad {
            assert!(parse_claim_ref(&claim_ref).is_err(), "accepted {claim_ref:?}");
        }
    }
}
