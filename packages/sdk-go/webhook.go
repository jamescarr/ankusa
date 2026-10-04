package ankusa

import (
	"net/http"
	"net/textproto"
	"strings"
)

// HookHeaders is a hook delivery's identity, read from the headers Ankusa's
// HTTP sink attaches. Source is "" when absent; Tenant, ContentType,
// DedupeKey, ReplayID and ShippedIdempotencyKey are nil when absent.
//
// ShippedIdempotencyKey is x-ankusa-idempotency-key: the tenant-scoped key
// Ankusa computed once for the hook, nil for a sender that predates the
// header. Read the key through IdempotencyKey, which falls back to computing it.
type HookHeaders struct {
	ID                    string  `json:"id"`
	Source                string  `json:"source"`
	Tenant                *string `json:"tenant"`
	ContentType           *string `json:"content_type"`
	DedupeKey             *string `json:"dedupe_key"`
	ReplayID              *string `json:"replay_id"`
	ShippedIdempotencyKey *string `json:"idempotency_key"`
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
// content-type, x-ankusa-dedupe-key, x-ankusa-replay-id, and
// x-ankusa-idempotency-key off a request's
// headers, case-insensitively. A missing or empty x-ankusa-id is a
// *MissingHookIdError: delivery is at-least-once, and x-ankusa-id is the
// identity to dedupe on, so a delivery without it is a framework bug rather
// than a tolerable request. dedupe_key, replay_id and idempotency_key are nil
// when their header is absent or empty.
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

	return HookHeaders{
		ID:                    id,
		Source:                source,
		Tenant:                tenant,
		ContentType:           contentType,
		DedupeKey:             dedupeKey,
		ReplayID:              replayID,
		ShippedIdempotencyKey: shipped,
	}, nil
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
