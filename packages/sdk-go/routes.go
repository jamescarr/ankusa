package ankusa

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// IPRule is one global or per-route IP rule. Action is "allow" or "deny";
// CIDR is the range it applies to.
type IPRule struct {
	Action string `json:"action"`
	CIDR   string `json:"cidr"`
}

// IPRules is the global rule set: Default applies when no listed rule
// matches.
type IPRules struct {
	Default string   `json:"default"`
	Rules   []IPRule `json:"rules"`
}

// Route is a stored route definition. RouteInput and RoutePatch are what you
// send; Route is what the listener returns.
type Route struct {
	ID         string         `json:"id"`
	Path       string         `json:"path"`
	Methods    []string       `json:"methods"`
	Enabled    bool           `json:"enabled"`
	IPRules    []IPRule       `json:"ip_rules"`
	Metadata   map[string]any `json:"metadata"`
	InsertedAt time.Time      `json:"inserted_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// RouteInput is a create/replace body. The wire body carries exactly what the
// caller set: omitempty/omitzero keep unset fields out, while an explicit
// empty slice or map is sent as []/{}, and Enabled is a pointer so false is
// distinguishable from unset.
type RouteInput struct {
	ID       string         `json:"id,omitempty"`
	Path     string         `json:"path"`
	Methods  []string       `json:"methods,omitzero"`
	Enabled  *bool          `json:"enabled,omitempty"`
	IPRules  []IPRule       `json:"ip_rules,omitzero"`
	Metadata map[string]any `json:"metadata,omitzero"`
}

// RoutePatch is a partial update body, with the same
// exactly-what-was-set semantics as RouteInput.
type RoutePatch struct {
	Enabled  *bool          `json:"enabled,omitempty"`
	Methods  []string       `json:"methods,omitzero"`
	IPRules  []IPRule       `json:"ip_rules,omitzero"`
	Metadata map[string]any `json:"metadata,omitzero"`
}

// RoutePage is one page of ListRoutes. NextCursor is nil at the end of the
// listing.
type RoutePage struct {
	Routes     []Route `json:"routes"`
	NextCursor *string `json:"next_cursor"`
}

// ListRoutesParams filters a route listing. Zero values mean "no filter".
type ListRoutesParams struct {
	Enabled *bool  `json:"enabled"`
	Limit   int    `json:"limit"`
	Cursor  string `json:"cursor"`
}

// DryRunRequest is a request to dry-run against the route table.
type DryRunRequest struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	IP     string `json:"ip"`
}

// DryRunIPRule is the IP rule a dry run matched, with the scope that matched
// ("route" or "global").
type DryRunIPRule struct {
	Action string `json:"action"`
	CIDR   string `json:"cidr"`
	Scope  string `json:"scope"`
}

// DryRunResult is the route table's decision.
type DryRunResult struct {
	Decision string        `json:"decision"`
	Reason   string        `json:"reason"`
	RouteID  *string       `json:"route_id"`
	IPRule   *DryRunIPRule `json:"ip_rule"`
}

// RoutesHealth is the route-management listener's liveness body.
type RoutesHealth struct {
	Status string `json:"status"`
	Routes int    `json:"routes"`
}

// RoutesClient talks to the route-management listener
// (routes.admin.port). Safe for concurrent use.
type RoutesClient struct {
	core *httpCore
}

// NewRoutesClient builds a client for the route-management listener at
// baseURL (e.g. "http://localhost:4003"). A base URL that is not an absolute
// http(s) URL is an error.
func NewRoutesClient(baseURL string, opts Options) (*RoutesClient, error) {
	core, err := newCore(baseURL, opts)
	if err != nil {
		return nil, err
	}
	return &RoutesClient{core: core}, nil
}

// Health probes GET /health.
func (c *RoutesClient) Health(ctx context.Context) (*RoutesHealth, error) {
	var health RoutesHealth
	if err := c.call(ctx, http.MethodGet, "/health", "", nil, &health); err != nil {
		return nil, err
	}
	return &health, nil
}

// ListRoutes lists route definitions. Query parameters go out in a fixed
// order: enabled, limit, cursor.
func (c *RoutesClient) ListRoutes(ctx context.Context, p ListRoutesParams) (*RoutePage, error) {
	var pairs [][2]string
	if p.Enabled != nil {
		pairs = append(pairs, [2]string{"enabled", strconv.FormatBool(*p.Enabled)})
	}
	if p.Limit > 0 {
		pairs = append(pairs, [2]string{"limit", strconv.Itoa(p.Limit)})
	}
	if p.Cursor != "" {
		pairs = append(pairs, [2]string{"cursor", p.Cursor})
	}

	var page RoutePage
	if err := c.call(ctx, http.MethodGet, appendQuery("/admin/routes", pairs), "", nil, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// CreateRoute posts a new route definition.
func (c *RoutesClient) CreateRoute(ctx context.Context, in RouteInput) (*Route, error) {
	body, err := encodeJSON("create route", in)
	if err != nil {
		return nil, err
	}

	var route Route
	if err := c.call(ctx, http.MethodPost, "/admin/routes", "", body, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

// GetRoute fetches one route definition.
func (c *RoutesClient) GetRoute(ctx context.Context, id string) (*Route, error) {
	path, err := routePath(id)
	if err != nil {
		return nil, err
	}

	var route Route
	if err := c.call(ctx, http.MethodGet, path, id, nil, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

// ReplaceRoute puts a full route definition in place of the existing one.
func (c *RoutesClient) ReplaceRoute(ctx context.Context, id string, in RouteInput) (*Route, error) {
	path, err := routePath(id)
	if err != nil {
		return nil, err
	}
	body, err := encodeJSON("replace route", in)
	if err != nil {
		return nil, err
	}

	var route Route
	if err := c.call(ctx, http.MethodPut, path, id, body, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

// UpdateRoute patches a route definition.
func (c *RoutesClient) UpdateRoute(ctx context.Context, id string, p RoutePatch) (*Route, error) {
	path, err := routePath(id)
	if err != nil {
		return nil, err
	}
	body, err := encodeJSON("update route", p)
	if err != nil {
		return nil, err
	}

	var route Route
	if err := c.call(ctx, http.MethodPatch, path, id, body, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

// DeleteRoute deletes a route definition. The body, if any, is ignored.
func (c *RoutesClient) DeleteRoute(ctx context.Context, id string) error {
	path, err := routePath(id)
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodDelete, path, id, nil, nil)
}

// GetIPRules fetches the global IP rules.
func (c *RoutesClient) GetIPRules(ctx context.Context) (*IPRules, error) {
	var rules IPRules
	if err := c.call(ctx, http.MethodGet, "/admin/ip-rules", "", nil, &rules); err != nil {
		return nil, err
	}
	return &rules, nil
}

// PutIPRules replaces the global IP rules.
func (c *RoutesClient) PutIPRules(ctx context.Context, r IPRules) (*IPRules, error) {
	body, err := encodeJSON("put IP rules", r)
	if err != nil {
		return nil, err
	}

	var rules IPRules
	if err := c.call(ctx, http.MethodPut, "/admin/ip-rules", "", body, &rules); err != nil {
		return nil, err
	}
	return &rules, nil
}

// TestRoute dry-runs a request against the route table and returns the
// decision, without capturing anything.
func (c *RoutesClient) TestRoute(ctx context.Context, r DryRunRequest) (*DryRunResult, error) {
	body, err := encodeJSON("test route", r)
	if err != nil {
		return nil, err
	}

	var result DryRunResult
	if err := c.call(ctx, http.MethodPost, "/admin/routes/test", "", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// routePath validates id and escapes it as one path segment. A URL parser
// normalizes "", "." and ".." away — they'd address the collection endpoint —
// so they are rejected before any request; every other id travels with
// /, ?, #, % and space escaped.
func routePath(id string) (string, error) {
	switch id {
	case "", ".", "..":
		return "", &InvalidRouteIdError{ID: id}
	}
	return "/admin/routes/" + url.PathEscape(id), nil
}

// call performs one route-management request and classifies the response.
// id is only used for *RouteNotFoundError ("" for collection calls); out may
// be nil for calls whose body is ignored.
func (c *RoutesClient) call(ctx context.Context, method, path, id string, body []byte, out any) error {
	resp, err := c.core.send(ctx, method, path, body)
	if err != nil {
		return &RoutesUnavailableError{Message: "routes gateway unreachable", Cause: err}
	}

	switch {
	case resp.status >= 200 && resp.status <= 299:
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(resp.body, out); err != nil {
			return &RoutesUnavailableError{
				Message: fmt.Sprintf("routes gateway returned an invalid JSON body (%d)", resp.status),
				Cause:   err,
			}
		}
		return nil

	case resp.status == http.StatusNotFound:
		return &RouteNotFoundError{ID: id}

	case resp.status >= 400 && resp.status <= 499:
		var body struct {
			Error         string `json:"error"`
			Field         string `json:"field"`
			Message       string `json:"message"`
			ConflictingID string `json:"conflicting_id"`
			MaxRoutes     int    `json:"max_routes"`
		}
		_ = json.Unmarshal(resp.body, &body)
		return &RoutesRejectedError{
			Status:        resp.status,
			Code:          body.Error,
			Field:         body.Field,
			Message:       body.Message,
			ConflictingID: body.ConflictingID,
			MaxRoutes:     body.MaxRoutes,
		}

	default:
		// 1xx, 3xx (redirects are never followed), 5xx.
		return &RoutesUnavailableError{Message: fmt.Sprintf("routes gateway error (%d)", resp.status)}
	}
}
