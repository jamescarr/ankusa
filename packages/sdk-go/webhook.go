package ankusa

import (
	"net/http"
	"net/textproto"
	"strings"
)

// HookHeaders is a hook delivery's identity, read from the headers Ankusa's
// HTTP sink attaches. Source is "" when absent; Tenant and ContentType are
// nil when absent.
type HookHeaders struct {
	ID          string  `json:"id"`
	Source      string  `json:"source"`
	Tenant      *string `json:"tenant"`
	ContentType *string `json:"content_type"`
}

// ParseHeaders reads x-ankusa-id, x-ankusa-source, x-ankusa-tenant, and
// content-type off a request's headers, case-insensitively. A missing or
// empty x-ankusa-id is a *MissingHookIdError: delivery is at-least-once, and
// x-ankusa-id is the identity to dedupe on, so a delivery without it is a
// framework bug rather than a tolerable request.
func ParseHeaders(h http.Header) (HookHeaders, error) {
	id, ok := headerValue(h, "x-ankusa-id")
	if !ok || id == "" {
		return HookHeaders{}, &MissingHookIdError{}
	}

	source, _ := headerValue(h, "x-ankusa-source")
	var tenant, contentType *string
	if value, ok := headerValue(h, "x-ankusa-tenant"); ok {
		tenant = &value
	}
	if value, ok := headerValue(h, "content-type"); ok {
		contentType = &value
	}

	return HookHeaders{ID: id, Source: source, Tenant: tenant, ContentType: contentType}, nil
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
