package ankusa

import (
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
)

// HookHeaders is a hook delivery's identity, read from the headers Ankusa's
// HTTP sink attaches. Source is "" when absent; Tenant, ContentType,
// DedupeKey, ReplayID, ShippedIdempotencyKey, ReceivedAt, BodySHA256 and
// Verification are nil when absent.
//
// ShippedIdempotencyKey is x-ankusa-idempotency-key: the tenant-scoped key
// Ankusa computed once for the hook, nil for a sender that predates the
// header. Read the key through IdempotencyKey, which falls back to computing it.
//
// ReceivedAt (x-ankusa-received-at) is when the edge accepted the hook, in
// unix milliseconds, and BodySHA256 (x-ankusa-body-sha256) the lowercase hex
// SHA-256 of the body Ankusa sent. Both are advisory: nil when the header is
// absent, empty or malformed (not 1 to 15 decimal digits; not exactly 64
// lowercase hex characters), never an error.
type HookHeaders struct {
	ID                    string        `json:"id"`
	Source                string        `json:"source"`
	Tenant                *string       `json:"tenant"`
	ContentType           *string       `json:"content_type"`
	DedupeKey             *string       `json:"dedupe_key"`
	ReplayID              *string       `json:"replay_id"`
	ShippedIdempotencyKey *string       `json:"idempotency_key"`
	ReceivedAt            *int64        `json:"received_at"`
	BodySHA256            *string       `json:"body_sha256"`
	Verification          *Verification `json:"verification"`
}

// Verification is how Ankusa verified a hook at ingest.
type Verification struct {
	// Status is "ok", "failed" or "skipped"; other values pass through.
	Status string `json:"status"`
	// Flagged is true when a failed check was accepted
	// (on_verify_failure: accept_flag): treat the delivery as unverified.
	Flagged bool `json:"flagged"`
}

// Trusted reports whether the node verified the hook at ingest and nothing
// flags it: true only for Status "ok" with Flagged false. A flagged hook
// (accepted despite a failed check, on_verify_failure: accept_flag), a
// "failed" or "skipped" one, any other status, and a nil Verification (a node
// older than the field) are all untrusted. It is safe to call on a nil
// *Verification.
func (v *Verification) Trusted() bool {
	return v != nil && v.Status == "ok" && !v.Flagged
}

// Trusted reports whether the node verified this delivery at ingest and
// nothing flags it; see Verification.Trusted.
func (h HookHeaders) Trusted() bool {
	return h.Verification.Trusted()
}

// IdempotencyKey is the key a consumer dedupes this delivery on: the key
// Ankusa shipped in x-ankusa-idempotency-key when it is non-empty. For a
// delivery from a sender that predates the header it is computed:
// tenant:source:dedupe_key (tenant "default" when there is none) for a
// non-empty x-ankusa-dedupe-key, else the hook id. With includeReplay it also
// appends the replay id, so a replay of an event already processed is not
// deduped away; without it (the default), a replay dedupes to the original's
// key.
func (h HookHeaders) IdempotencyKey(includeReplay bool) string {
	return idempotencyKey(h.ShippedIdempotencyKey, h.Tenant, h.Source, h.ID, h.DedupeKey, h.ReplayID, includeReplay)
}

// ParseHeaders reads x-ankusa-id, x-ankusa-source, x-ankusa-tenant,
// content-type, x-ankusa-dedupe-key, x-ankusa-replay-id,
// x-ankusa-idempotency-key, and x-ankusa-verification(-flagged) off a
// request's headers, case-insensitively. A missing or empty x-ankusa-id is a
// *MissingHookIdError: delivery is at-least-once, and x-ankusa-id is the
// identity to dedupe on, so a delivery without it is a framework bug rather
// than a tolerable request. dedupe_key, replay_id, idempotency_key and
// verification are nil when their header is absent or empty.
func ParseHeaders(h http.Header) (HookHeaders, error) {
	id, ok := headerValue(h, "x-ankusa-id")
	if !ok || id == "" {
		return HookHeaders{}, &MissingHookIdError{}
	}

	source, _ := headerValue(h, "x-ankusa-source")
	var tenant, contentType, dedupeKey, replayID, shipped *string
	if value, ok := headerValue(h, "x-ankusa-tenant"); ok {
		tenant = &value
	}
	if value, ok := headerValue(h, "content-type"); ok {
		contentType = &value
	}
	if value, ok := headerValue(h, "x-ankusa-dedupe-key"); ok && value != "" {
		dedupeKey = &value
	}
	if value, ok := headerValue(h, "x-ankusa-replay-id"); ok && value != "" {
		replayID = &value
	}
	if value, ok := headerValue(h, "x-ankusa-idempotency-key"); ok && value != "" {
		shipped = &value
	}
	var receivedAt *int64
	if value, ok := headerValue(h, "x-ankusa-received-at"); ok && isDigits(value, 1, 15) {
		if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
			receivedAt = &ms
		}
	}
	var bodySHA256 *string
	if value, ok := headerValue(h, "x-ankusa-body-sha256"); ok && isLowerHex(value, 64) {
		bodySHA256 = &value
	}
	var verification *Verification
	if status, ok := headerValue(h, "x-ankusa-verification"); ok && status != "" {
		flagged, _ := headerValue(h, "x-ankusa-verification-flagged")
		verification = &Verification{Status: status, Flagged: flagged == "true"}
	}

	return HookHeaders{
		ID:                    id,
		Source:                source,
		Tenant:                tenant,
		ContentType:           contentType,
		DedupeKey:             dedupeKey,
		ReplayID:              replayID,
		ShippedIdempotencyKey: shipped,
		ReceivedAt:            receivedAt,
		BodySHA256:            bodySHA256,
		Verification:          verification,
	}, nil
}

// isDigits reports whether s is lo to hi ASCII decimal digits and nothing else.
func isDigits(s string, lo, hi int) bool {
	if len(s) < lo || len(s) > hi {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isLowerHex reports whether s is exactly n lowercase hex characters.
func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		if (s[i] < '0' || s[i] > '9') && (s[i] < 'a' || s[i] > 'f') {
			return false
		}
	}
	return true
}

// ReceiveOptions configures ReceiveDelivery. The zero value refuses every
// delivery: Ankusa's HTTP sink signs unless it is configured allow_unsigned,
// so a receiver needs the sink's secret (or AllowUnsigned) to accept anything.
type ReceiveOptions struct {
	// Secrets are the sink's secrets ("whsec_" + base64, or any other string
	// used as its own bytes); several during a rotation. With any, the
	// delivery must verify, and AllowUnsigned does not weaken that.
	Secrets []string
	// AllowUnsigned accepts a delivery when no secret is configured,
	// unverified. Any host that can reach the receiver's URL can then inject
	// hooks.
	AllowUnsigned bool
	// ToleranceSeconds bounds |now - webhook-timestamp|; nil means 300.
	ToleranceSeconds *int64
	// Now is the unix time to judge the timestamp against; nil means the clock.
	Now *int64
}

// ReceivedDelivery is an accepted delivery: its identity, and whether its
// signature verified (false only for an unsigned delivery AllowUnsigned let
// through).
type ReceivedDelivery struct {
	HookHeaders
	Verified bool `json:"verified"`
}

// ReceiveDelivery judges one HTTP-sink delivery: is it signed, and what is its
// identity. body must be the raw bytes received. The error is an
// *InvalidSignatureError (answer 401) when there is no valid signature —
// invalid_secret when no secret is configured and AllowUnsigned is not set —
// or a *MissingHookIdError (400) when x-ankusa-id is absent. The signature is
// judged first.
func ReceiveDelivery(h http.Header, body []byte, opts ReceiveOptions) (ReceivedDelivery, error) {
	verified := false
	switch {
	case len(opts.Secrets) > 0:
		if _, err := VerifySignature(h, body, opts.Secrets, VerifyOptions{ToleranceSeconds: opts.ToleranceSeconds, Now: opts.Now}); err != nil {
			return ReceivedDelivery{}, err
		}
		verified = true
	case opts.AllowUnsigned:
	default:
		return ReceivedDelivery{}, &InvalidSignatureError{Code: "invalid_secret"}
	}

	hook, err := ParseHeaders(h)
	if err != nil {
		return ReceivedDelivery{}, err
	}
	return ReceivedDelivery{HookHeaders: hook, Verified: verified}, nil
}

// headerValue returns the first value for name. The canonical key is tried
// first, then every key is compared case-insensitively, so a map built with
// non-canonical keys works too.
func headerValue(h http.Header, name string) (string, bool) {
	if values := h[textproto.CanonicalMIMEHeaderKey(name)]; len(values) > 0 {
		return values[0], true
	}
	for key, values := range h {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0], true
		}
	}
	return "", false
}
