package ankusa_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	ankusa "github.com/jamescarr/ankusa/packages/sdk-go"
)

// Edges no conformance vector expresses: caller-supplied clients, context
// cancellation, the exact wire shape of queries and bodies, id validation on
// the non-Get methods, and base-URL validation.

const (
	validRef = "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002"
	// The sha256 of the empty string, matching this file's empty bodies.
	emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// TestCallerClientRedirects: redirects are never followed, and the caller's
// *http.Client is copied, not mutated.
func TestCallerClientRedirects(t *testing.T) {
	var served int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&served, 1)
		if r.URL.Path == "/admin/routes" {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"routes":[],"next_cursor":null}`))
	}))
	defer server.Close()

	callerClient := &http.Client{}
	routes, err := ankusa.NewRoutesClient(server.URL, ankusa.Options{HTTPClient: callerClient})
	if err != nil {
		t.Fatalf("new routes client: %v", err)
	}

	_, err = routes.ListRoutes(context.Background(), ankusa.ListRoutesParams{})
	var unavailable *ankusa.RoutesUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("expected *RoutesUnavailableError, got %T: %v", err, err)
	}
	if got := atomic.LoadInt32(&served); got != 1 {
		t.Errorf("expected exactly 1 request served, got %d", got)
	}
	if callerClient.CheckRedirect != nil {
		t.Error("caller's http.Client was mutated: CheckRedirect is set")
	}
}

// TestCancelledContext: a canceled context surfaces as a retryable gateway
// failure that still unwraps to context.Canceled.
func TestCancelledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
	}))
	defer server.Close()

	claimCheck, err := ankusa.NewClaimCheckClient(server.URL, ankusa.Options{})
	if err != nil {
		t.Fatalf("new claim-check client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = claimCheck.Redeem(ctx, validRef, emptySHA)
	var unavailable *ankusa.ClaimCheckUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("expected *ClaimCheckUnavailableError, got %T: %v", err, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got %v", err)
	}
}

// TestQueryStrings: no filter means no query string, and every filter goes
// out as k=v in a fixed order.
func TestQueryStrings(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.RequestURI)
		mu.Unlock()

		switch r.URL.Path {
		case "/admin/routes":
			_, _ = w.Write([]byte(`{"routes":[],"next_cursor":null}`))
		case "/v1/dlq":
			_, _ = w.Write([]byte(`{"total":0,"entries":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()

	routes, err := ankusa.NewRoutesClient(server.URL, ankusa.Options{})
	if err != nil {
		t.Fatalf("new routes client: %v", err)
	}
	if _, err := routes.ListRoutes(ctx, ankusa.ListRoutesParams{}); err != nil {
		t.Fatalf("list routes: %v", err)
	}

	admin, err := ankusa.NewAdminClient(server.URL, ankusa.Options{})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	if _, err := admin.ListDeadLetters(ctx, ankusa.ListDeadLettersParams{SourceID: "demo", Since: 1720000000000}); err != nil {
		t.Fatalf("list dead letters: %v", err)
	}

	want := []string{"/admin/routes", "/v1/dlq?source_id=demo&since=1720000000000"}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("request URIs:\n got: %q\nwant: %q", seen, want)
	}
}

// TestReplayEmptyFilterBody: the zero filter still sends a body, `{}`.
func TestReplayEmptyFilterBody(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(raw))
		mu.Unlock()
		_, _ = w.Write([]byte(`{"replayed":0}`))
	}))
	defer server.Close()

	admin, err := ankusa.NewAdminClient(server.URL, ankusa.Options{})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	if _, err := admin.ReplayDeadLetters(context.Background(), ankusa.ReplayFilter{}); err != nil {
		t.Fatalf("replay dead letters: %v", err)
	}

	if len(bodies) != 1 {
		t.Fatalf("expected 1 request, got %d", len(bodies))
	}
	if bodies[0] != "{}" {
		t.Errorf("replay body: got %q, want %q", bodies[0], "{}")
	}
}

// TestExplicitEmptyRouteFields: an explicitly empty slice goes out as [], not
// omitted, and an explicit false goes out, not omitted.
func TestExplicitEmptyRouteFields(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(raw))
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"x","path":"/x","methods":[],"enabled":false,"ip_rules":[],"metadata":{},"inserted_at":"2026-09-28T14:16:26Z","updated_at":"2026-09-28T14:16:26Z"}`))
	}))
	defer server.Close()

	routes, err := ankusa.NewRoutesClient(server.URL, ankusa.Options{})
	if err != nil {
		t.Fatalf("new routes client: %v", err)
	}

	enabled := false
	_, err = routes.CreateRoute(context.Background(), ankusa.RouteInput{
		Path:    "/x",
		IPRules: []ankusa.IPRule{},
		Enabled: &enabled,
	})
	if err != nil {
		t.Fatalf("create route: %v", err)
	}

	if len(bodies) != 1 {
		t.Fatalf("expected 1 request, got %d", len(bodies))
	}
	var got, want any
	if err := json.Unmarshal([]byte(bodies[0]), &got); err != nil {
		t.Fatalf("decode request body %q: %v", bodies[0], err)
	}
	if err := json.Unmarshal([]byte(`{"path":"/x","ip_rules":[],"enabled":false}`), &want); err != nil {
		t.Fatalf("decode want: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("create body: got %s, want %s", bodies[0], `{"path":"/x","ip_rules":[],"enabled":false}`)
	}
}

// TestInvalidRouteIdsMakeNoRequest: Replace/Update/Delete validate the id
// before any request, like Get.
func TestInvalidRouteIdsMakeNoRequest(t *testing.T) {
	var served int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&served, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	routes, err := ankusa.NewRoutesClient(server.URL, ankusa.Options{})
	if err != nil {
		t.Fatalf("new routes client: %v", err)
	}
	ctx := context.Background()

	calls := map[string]func() error{
		"ReplaceRoute": func() error {
			_, err := routes.ReplaceRoute(ctx, "..", ankusa.RouteInput{Path: "/x"})
			return err
		},
		"UpdateRoute": func() error {
			_, err := routes.UpdateRoute(ctx, "..", ankusa.RoutePatch{})
			return err
		},
		"DeleteRoute": func() error {
			return routes.DeleteRoute(ctx, "..")
		},
	}
	for name, call := range calls {
		err := call()
		var invalid *ankusa.InvalidRouteIdError
		if !errors.As(err, &invalid) {
			t.Errorf("%s: expected *InvalidRouteIdError, got %T: %v", name, err, err)
		}
	}
	if got := atomic.LoadInt32(&served); got != 0 {
		t.Errorf("expected no requests, got %d", got)
	}
}

// TestInvalidBaseURLs: a base URL that is not an absolute http(s) URL is
// rejected at construction.
func TestInvalidBaseURLs(t *testing.T) {
	for _, baseURL := range []string{"://x", "ftp://h", "http://"} {
		if _, err := ankusa.NewRoutesClient(baseURL, ankusa.Options{}); err == nil {
			t.Errorf("NewRoutesClient(%q) returned no error", baseURL)
		}
	}
}
