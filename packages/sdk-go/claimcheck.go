package ankusa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"regexp"
	"strings"
)

// claimRefPattern matches urn:ankusa:claim:v1:<tenant>:<claim_id>, where
// claim_id is a canonical uppercase ULID. Go's RE2 anchors `$` to end of
// text (no (?m) here), so a trailing newline does not match.
var claimRefPattern = regexp.MustCompile(`^urn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})$`)

// claimSHA256Pattern matches the queue message's `sha256` field: the digest
// of the claim's bytes, 64 lowercase hex characters.
var claimSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ParsedClaimRef is a parsed claim-check URN.
type ParsedClaimRef struct {
	TenantID string `json:"tenant_id"`
	ClaimID  string `json:"claim_id"`
	// Path is where the gateway serves the claim's bytes:
	// /v1/claims/{tenant_id}/{claim_id}.
	Path string `json:"path"`
}

// ParseClaimRef parses urn:ankusa:claim:v1:<tenant>:<claim_id>. Anything else
// is an *InvalidClaimRefError.
func ParseClaimRef(ref string) (ParsedClaimRef, error) {
	match := claimRefPattern.FindStringSubmatch(ref)
	if match == nil {
		return ParsedClaimRef{}, &InvalidClaimRefError{Message: fmt.Sprintf("invalid claim-check ref: %q", ref)}
	}
	return ParsedClaimRef{
		TenantID: match[1],
		ClaimID:  match[2],
		Path:     "/v1/claims/" + match[1] + "/" + match[2],
	}, nil
}

// ClaimCheckClient talks to the claim-check gateway. Safe for concurrent use.
type ClaimCheckClient struct {
	core *httpCore
}

// NewClaimCheckClient builds a client for the claim-check gateway at baseURL
// (e.g. "http://localhost:4001"). A base URL that is not an absolute http(s)
// URL is an error.
func NewClaimCheckClient(baseURL string, opts Options) (*ClaimCheckClient, error) {
	core, err := newCore(baseURL, opts)
	if err != nil {
		return nil, err
	}
	return &ClaimCheckClient{core: core}, nil
}

// Redeem fetches the bytes of the claim named by ref (a claim-check URN) and
// verifies them against sha256Hex (the queue message's `sha256` field, 64
// lowercase hex chars) before returning them. A malformed ref or digest is an
// *InvalidClaimRefError and makes no request; every other failure is another
// concrete *XxxError carrying the retryable bit.
func (c *ClaimCheckClient) Redeem(ctx context.Context, ref, sha256Hex string) ([]byte, error) {
	parsed, err := ParseClaimRef(ref)
	if err != nil {
		return nil, err
	}
	if !claimSHA256Pattern.MatchString(sha256Hex) {
		return nil, &InvalidClaimRefError{Message: fmt.Sprintf("invalid claim sha256: %q", sha256Hex)}
	}

	resp, err := c.core.send(ctx, http.MethodGet, parsed.Path, nil)
	if err != nil {
		return nil, &ClaimCheckUnavailableError{Message: "claim-check gateway unreachable", Cause: err}
	}

	switch {
	case resp.status == http.StatusOK:
		digest := sha256.Sum256(resp.body)
		if hex.EncodeToString(digest[:]) != sha256Hex {
			return nil, &ClaimIntegrityError{TenantID: parsed.TenantID, ClaimID: parsed.ClaimID}
		}
		return resp.body, nil

	case resp.status == http.StatusNotFound:
		return nil, &ClaimNotFoundError{TenantID: parsed.TenantID, ClaimID: parsed.ClaimID}

	case resp.status >= 400 && resp.status <= 499:
		return nil, &ClaimRejectedError{Status: resp.status, Body: errorBody(resp)}

	default:
		// 1xx, 2xx other than 200, 3xx (redirects are never followed), 5xx.
		return nil, &ClaimCheckUnavailableError{Message: fmt.Sprintf("claim-check gateway error (%d)", resp.status)}
	}
}

// ClaimCheckHealth is the gateway's liveness body.
type ClaimCheckHealth struct {
	Status string `json:"status"`
}

// Health probes GET /health and requires exactly a 200 with a JSON body.
func (c *ClaimCheckClient) Health(ctx context.Context) (*ClaimCheckHealth, error) {
	resp, err := c.core.send(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return nil, &ClaimCheckUnavailableError{Message: "claim-check gateway unreachable", Cause: err}
	}
	if resp.status != http.StatusOK {
		return nil, &ClaimCheckUnavailableError{
			Message: fmt.Sprintf("claim-check gateway health check failed (%d)", resp.status),
		}
	}

	var health ClaimCheckHealth
	if err := json.Unmarshal(resp.body, &health); err != nil {
		return nil, &ClaimCheckUnavailableError{
			Message: "claim-check gateway health check failed (200)",
			Cause:   err,
		}
	}
	return &health, nil
}

// errorBody is ClaimRejectedError.Body: "" for an empty body, the decoded
// JSON value when the Content-Type is JSON, the raw string otherwise.
func errorBody(resp *rawResponse) any {
	if len(resp.body) == 0 {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(resp.header.Get("Content-Type"))
	if err == nil && (mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")) {
		var decoded any
		if err := json.Unmarshal(resp.body, &decoded); err == nil {
			return decoded
		}
	}
	return string(resp.body)
}
