package ankusa

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// AdminHealth is the admin listener's liveness body: the instance name and
// the roles this process runs.
type AdminHealth struct {
	Status   string   `json:"status"`
	Instance string   `json:"instance"`
	Roles    []string `json:"roles"`
}

// DLQEntry is one dead-lettered hook. Seq is the source-local sequence number
// Ankusa assigned at ingest; TenantID, Seq, and ContentType are nil when the
// hook had none.
type DLQEntry struct {
	ID             string  `json:"id"`
	SourceID       string  `json:"source_id"`
	TenantID       *string `json:"tenant_id"`
	Seq            *int64  `json:"seq"`
	ReceivedAt     int64   `json:"received_at"`
	DeadLetteredAt int64   `json:"dead_lettered_at"`
	Size           int64   `json:"size"`
	ContentType    *string `json:"content_type"`
	Reason         string  `json:"reason"`
}

// DLQPage is a page of dead letters. Total is the size of the whole
// (filtered) dead-letter queue, not of Entries.
type DLQPage struct {
	Total   int        `json:"total"`
	Entries []DLQEntry `json:"entries"`
}

// QuarantineEntry is one quarantined hook.
type QuarantineEntry struct {
	ID         string `json:"id"`
	SourceID   string `json:"source_id"`
	ReceivedAt int64  `json:"received_at"`
	Reason     string `json:"reason"`
}

// QuarantinePage is a page of quarantined hooks.
type QuarantinePage struct {
	Entries []QuarantineEntry `json:"entries"`
}

// ReplayFilter selects hooks to replay. The zero value replays everything the
// runtime allows; a non-zero field narrows it.
type ReplayFilter struct {
	SourceID string `json:"source_id,omitempty"`
	ID       string `json:"id,omitempty"`
	Since    int64  `json:"since,omitempty"`
}

// Replayed is how many hooks a replay enqueued.
type Replayed struct {
	Replayed int `json:"replayed"`
}

// ListDeadLettersParams filters a DLQ listing. Zero values mean "no filter".
type ListDeadLettersParams struct {
	SourceID string `json:"source_id"`
	Since    int64  `json:"since"`
	Limit    int    `json:"limit"`
}

// ListQuarantinedParams filters a quarantine listing. Zero means "no filter".
type ListQuarantinedParams struct {
	Limit int `json:"limit"`
}

// AdminClient talks to the admin listener (admin.port). Safe for concurrent
// use.
type AdminClient struct {
	core *httpCore
}

// NewAdminClient builds a client for the admin listener at baseURL (e.g.
// "http://localhost:4002"). A base URL that is not an absolute http(s) URL is
// an error.
func NewAdminClient(baseURL string, opts Options) (*AdminClient, error) {
	core, err := newCore(baseURL, opts)
	if err != nil {
		return nil, err
	}
	return &AdminClient{core: core}, nil
}

// Health probes GET /health.
func (c *AdminClient) Health(ctx context.Context) (*AdminHealth, error) {
	resp, err := c.call(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return nil, err
	}

	var health AdminHealth
	if err := decodeJSON(resp, &health); err != nil {
		return nil, err
	}
	return &health, nil
}

// Metrics fetches the Prometheus exposition text from GET /metrics.
func (c *AdminClient) Metrics(ctx context.Context) (string, error) {
	resp, err := c.call(ctx, http.MethodGet, "/metrics", nil)
	if err != nil {
		return "", err
	}
	return string(resp.body), nil
}

// Config fetches the redacted instance config from GET /v1/config.
func (c *AdminClient) Config(ctx context.Context) (map[string]any, error) {
	resp, err := c.call(ctx, http.MethodGet, "/v1/config", nil)
	if err != nil {
		return nil, err
	}

	var config map[string]any
	if err := decodeJSON(resp, &config); err != nil {
		return nil, err
	}
	return config, nil
}

// ListDeadLetters lists the dead-letter queue. Query parameters go out in a
// fixed order: source_id, since, limit.
func (c *AdminClient) ListDeadLetters(ctx context.Context, p ListDeadLettersParams) (*DLQPage, error) {
	var pairs [][2]string
	if p.SourceID != "" {
		pairs = append(pairs, [2]string{"source_id", p.SourceID})
	}
	if p.Since > 0 {
		pairs = append(pairs, [2]string{"since", strconv.FormatInt(p.Since, 10)})
	}
	if p.Limit > 0 {
		pairs = append(pairs, [2]string{"limit", strconv.Itoa(p.Limit)})
	}

	resp, err := c.call(ctx, http.MethodGet, appendQuery("/v1/dlq", pairs), nil)
	if err != nil {
		return nil, err
	}

	var page DLQPage
	if err := decodeJSON(resp, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// ReplayDeadLetters replays every dead letter matching f. The zero filter
// sends `{}`.
func (c *AdminClient) ReplayDeadLetters(ctx context.Context, f ReplayFilter) (*Replayed, error) {
	body, err := encodeJSON("replay dead letters", f)
	if err != nil {
		return nil, err
	}

	resp, err := c.call(ctx, http.MethodPost, "/v1/dlq/replay", body)
	if err != nil {
		return nil, err
	}

	var replayed Replayed
	if err := decodeJSON(resp, &replayed); err != nil {
		return nil, err
	}
	return &replayed, nil
}

// ListQuarantined lists quarantined hooks, newest first.
func (c *AdminClient) ListQuarantined(ctx context.Context, p ListQuarantinedParams) (*QuarantinePage, error) {
	var pairs [][2]string
	if p.Limit > 0 {
		pairs = append(pairs, [2]string{"limit", strconv.Itoa(p.Limit)})
	}

	resp, err := c.call(ctx, http.MethodGet, appendQuery("/v1/quarantine", pairs), nil)
	if err != nil {
		return nil, err
	}

	var page QuarantinePage
	if err := decodeJSON(resp, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// call performs one admin request and classifies the response: 2xx returns
// the response for decoding, a 409 role_not_enabled is a
// *RoleNotEnabledError, other 4xx are *AdminRejectedError, everything else is
// a retryable *AdminUnavailableError.
func (c *AdminClient) call(ctx context.Context, method, path string, body []byte) (*rawResponse, error) {
	resp, err := c.core.send(ctx, method, path, body)
	if err != nil {
		return nil, &AdminUnavailableError{Message: "admin gateway unreachable", Cause: err}
	}
	if resp.status >= 200 && resp.status <= 299 {
		return resp, nil
	}

	var payload struct {
		Error string `json:"error"`
		Role  string `json:"role"`
	}
	_ = json.Unmarshal(resp.body, &payload)

	switch {
	case resp.status == http.StatusConflict && payload.Error == "role_not_enabled":
		return nil, &RoleNotEnabledError{Role: payload.Role}

	case resp.status >= 400 && resp.status <= 499:
		return nil, &AdminRejectedError{Status: resp.status, Code: payload.Error}

	default:
		// 1xx, 3xx (redirects are never followed), 5xx.
		return nil, &AdminUnavailableError{Message: fmt.Sprintf("admin gateway error (%d)", resp.status)}
	}
}

// decodeJSON decodes a 2xx body; a malformed one is a retryable
// *AdminUnavailableError, since the listener answered wrongly rather than the
// caller asking wrongly.
func decodeJSON(resp *rawResponse, out any) error {
	if err := json.Unmarshal(resp.body, out); err != nil {
		return &AdminUnavailableError{
			Message: fmt.Sprintf("admin gateway returned an invalid JSON body (%d)", resp.status),
			Cause:   err,
		}
	}
	return nil
}
