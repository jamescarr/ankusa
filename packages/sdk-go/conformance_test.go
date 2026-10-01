// Package ankusa_test holds the language-neutral conformance runner: it loads
// ../../conformance/cases/*.json and drives every vector through this
// package's public surface only. `mise run check:conformance` runs it, and
// conformance/README.md is the runner contract.
package ankusa_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ankusa "github.com/jamescarr/ankusa/packages/sdk-go"
)

// modulePath is the import path this package's errors must live in; anything
// else is an unmapped error.
const modulePath = "github.com/jamescarr/ankusa/packages/sdk-go"

// conformanceCase is one vector. Input and Expect are raw maps so that
// `"ok": null` stays distinguishable from an absent key.
type conformanceCase struct {
	ID        string                     `json:"id"`
	Feature   string                     `json:"feature"`
	Operation string                     `json:"operation"`
	Input     map[string]json.RawMessage `json:"input"`
	Expect    map[string]json.RawMessage `json:"expect"`
}

// bodySpec is a vector Body: text (UTF-8) | base64 | json.
type bodySpec struct {
	Text   *string         `json:"text"`
	Base64 *string         `json:"base64"`
	JSON   json.RawMessage `json:"json"`
}

// gatewaySpec is a vector Gateway: unreachable, or a status/headers/body/delay
// answer.
type gatewaySpec struct {
	Unreachable bool              `json:"unreachable"`
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers"`
	Body        *bodySpec         `json:"body"`
	DelayMS     int               `json:"delay_ms"`
}

// clientSpec is a vector Client.
type clientSpec struct {
	Headers   map[string]string `json:"headers"`
	TimeoutMS int               `json:"timeout_ms"`
	Transport string            `json:"transport"`
}

// recordedRequest is one request the SDK made, in the shape the vectors
// assert.
type recordedRequest struct {
	Method  string
	Path    string
	Headers map[string]string // lowercase names, first value
	Body    any               // parsed JSON, nil when there was no body
}

type recorder struct {
	mu       sync.Mutex
	requests []recordedRequest
}

func (r *recorder) add(request recordedRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, request)
}

func (r *recorder) snapshot() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedRequest(nil), r.requests...)
}

func TestConformance(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "conformance", "cases", "*.json"))
	if err != nil {
		t.Fatalf("glob conformance cases: %v", err)
	}

	var cases []conformanceCase
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		var fileCases struct {
			Cases []conformanceCase `json:"cases"`
		}
		if err := json.Unmarshal(data, &fileCases); err != nil {
			t.Fatalf("decode %s: %v", file, err)
		}
		cases = append(cases, fileCases.Cases...)
	}
	if len(cases) == 0 {
		t.Fatalf("no conformance cases found under %s", filepath.Join("..", "..", "conformance", "cases"))
	}

	for _, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			runCase(t, c)
		})
	}
}

func runCase(t *testing.T, c conformanceCase) {
	t.Helper()

	recorder := &recorder{}
	result, err := dispatch(t, c, recorder)
	requests := recorder.snapshot()

	rawOK, hasOK := c.Expect["ok"]
	rawError, hasError := c.Expect["error"]

	switch {
	case hasOK && hasError:
		t.Fatalf("invalid case: expect has both ok and error")

	case hasOK:
		if err != nil {
			t.Fatalf("expected ok, got error %T: %v", err, err)
		}
		want := decodeAny(t, rawOK)
		if c.Operation == "redeem" {
			want = redeemExpectation(t, rawOK)
		}
		got := normalize(t, result)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("result mismatch:\n got: %s\nwant: %s", compact(t, got), compact(t, want))
		}

	case hasError:
		assertError(t, err, rawError)

	default:
		// Only `requests` is asserted: the operation must have completed
		// without a mapped error.
		if err != nil {
			t.Fatalf("expected no error, got %T: %v", err, err)
		}
	}

	if raw, ok := c.Expect["requests"]; ok {
		assertRequests(t, requests, raw)
	}
}

// dispatch runs one operation against the vector's mock gateway.
func dispatch(t *testing.T, c conformanceCase, recorder *recorder) (any, error) {
	t.Helper()

	gateway := decode[gatewaySpec](t, c.Input["gateway"])
	client := decode[clientSpec](t, c.Input["client"])
	ctx := context.Background()

	switch c.Operation {
	case "parse_claim_ref":
		return ankusa.ParseClaimRef(decode[string](t, c.Input["ref"]))

	case "parse_headers":
		headers := http.Header{}
		for name, value := range decode[map[string]string](t, c.Input["headers"]) {
			// Deliberately not canonicalized: the SDK's own
			// case-insensitivity is what the vectors exercise.
			headers[name] = []string{value}
		}
		return ankusa.ParseHeaders(headers)

	case "redeem":
		g := startGateway(t, gateway, client, recorder)
		defer g.close()
		claimCheck, err := ankusa.NewClaimCheckClient(g.baseURL, clientOptions(t, client, g.transport))
		if err != nil {
			t.Fatalf("new claim-check client: %v", err)
		}
		body, err := claimCheck.Redeem(ctx, decode[string](t, c.Input["ref"]), decode[string](t, c.Input["sha256"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"body": map[string]any{"base64": base64.StdEncoding.EncodeToString(body)}}, nil

	case "health":
		g := startGateway(t, gateway, client, recorder)
		defer g.close()
		claimCheck, err := ankusa.NewClaimCheckClient(g.baseURL, clientOptions(t, client, g.transport))
		if err != nil {
			t.Fatalf("new claim-check client: %v", err)
		}
		return claimCheck.Health(ctx)

	case "routes_health":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.Health(ctx)

	case "routes_list":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.ListRoutes(ctx, decode[ankusa.ListRoutesParams](t, c.Input["params"]))

	case "routes_create":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.CreateRoute(ctx, decode[ankusa.RouteInput](t, c.Input["input"]))

	case "routes_get":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.GetRoute(ctx, decode[string](t, c.Input["id"]))

	case "routes_replace":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.ReplaceRoute(ctx, decode[string](t, c.Input["id"]), decode[ankusa.RouteInput](t, c.Input["input"]))

	case "routes_update":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.UpdateRoute(ctx, decode[string](t, c.Input["id"]), decode[ankusa.RoutePatch](t, c.Input["patch"]))

	case "routes_delete":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return nil, routes.DeleteRoute(ctx, decode[string](t, c.Input["id"]))

	case "routes_ip_rules_get":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.GetIPRules(ctx)

	case "routes_ip_rules_put":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.PutIPRules(ctx, decode[ankusa.IPRules](t, c.Input["rules"]))

	case "routes_test":
		routes, closeGateway := buildRoutesClient(t, gateway, client, recorder)
		defer closeGateway()
		return routes.TestRoute(ctx, decode[ankusa.DryRunRequest](t, c.Input["request"]))

	case "admin_health":
		admin, closeGateway := buildAdminClient(t, gateway, client, recorder)
		defer closeGateway()
		return admin.Health(ctx)

	case "admin_metrics":
		admin, closeGateway := buildAdminClient(t, gateway, client, recorder)
		defer closeGateway()
		text, err := admin.Metrics(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"text": text}, nil

	case "admin_config":
		admin, closeGateway := buildAdminClient(t, gateway, client, recorder)
		defer closeGateway()
		return admin.Config(ctx)

	case "admin_dlq_list":
		admin, closeGateway := buildAdminClient(t, gateway, client, recorder)
		defer closeGateway()
		return admin.ListDeadLetters(ctx, decode[ankusa.ListDeadLettersParams](t, c.Input["params"]))

	case "admin_dlq_replay":
		admin, closeGateway := buildAdminClient(t, gateway, client, recorder)
		defer closeGateway()
		return admin.ReplayDeadLetters(ctx, decode[ankusa.ReplayFilter](t, c.Input["filter"]))

	case "admin_quarantine":
		admin, closeGateway := buildAdminClient(t, gateway, client, recorder)
		defer closeGateway()
		return admin.ListQuarantined(ctx, decode[ankusa.ListQuarantinedParams](t, c.Input["params"]))

	default:
		t.Fatalf("unknown conformance operation %q", c.Operation)
		return nil, nil
	}
}

func buildRoutesClient(t *testing.T, gateway gatewaySpec, client clientSpec, recorder *recorder) (*ankusa.RoutesClient, func()) {
	t.Helper()
	g := startGateway(t, gateway, client, recorder)
	routes, err := ankusa.NewRoutesClient(g.baseURL, clientOptions(t, client, g.transport))
	if err != nil {
		t.Fatalf("new routes client: %v", err)
	}
	return routes, g.close
}

func buildAdminClient(t *testing.T, gateway gatewaySpec, client clientSpec, recorder *recorder) (*ankusa.AdminClient, func()) {
	t.Helper()
	g := startGateway(t, gateway, client, recorder)
	admin, err := ankusa.NewAdminClient(g.baseURL, clientOptions(t, client, g.transport))
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	return admin, g.close
}

// gateway is the mock a client talks to: a real server, or an injected
// transport that never touches the network.
type gateway struct {
	baseURL   string
	transport http.RoundTripper
	close     func()
}

func startGateway(t *testing.T, spec gatewaySpec, client clientSpec, recorder *recorder) *gateway {
	t.Helper()

	if spec.Unreachable {
		return &gateway{baseURL: "http://127.0.0.1:1", close: func() {}}
	}
	if client.Transport == "injected" {
		return &gateway{
			baseURL:   "http://gateway.invalid",
			transport: injectedTransport(t, spec, recorder),
			close:     func() {},
		}
	}

	body := bodyBytes(t, spec.Body)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.add(recordedRequest{
			Method:  r.Method,
			Path:    r.RequestURI,
			Headers: lowercaseHeaderNames(r.Header),
			Body:    parseJSONBody(readBody(r)),
		})

		// Record before sleeping, so a vector still asserts the request its
		// client abandoned.
		if spec.DelayMS > 0 {
			select {
			case <-time.After(time.Duration(spec.DelayMS) * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}

		for name, value := range spec.Headers {
			w.Header().Set(name, value)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(spec.Status)
		_, _ = w.Write(body)
	})

	server := httptest.NewServer(handler)
	return &gateway{baseURL: server.URL, close: server.Close}
}

// injectedTransport serves the spec in-process and records requests, with no
// server at all.
func injectedTransport(t *testing.T, spec gatewaySpec, recorder *recorder) http.RoundTripper {
	t.Helper()
	body := bodyBytes(t, spec.Body)
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		recorder.add(recordedRequest{
			Method: r.Method,
			// RequestURI is the request target as sent, query string
			// included — the same thing the real-server transport records.
			Path:    r.URL.RequestURI(),
			Headers: lowercaseHeaderNames(r.Header),
			Body:    parseJSONBody(readBody(r)),
		})

		header := http.Header{}
		for name, value := range spec.Headers {
			header.Set(name, value)
		}
		header.Set("Content-Length", strconv.Itoa(len(body)))

		return &http.Response{
			StatusCode:    spec.Status,
			Header:        header,
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
			Request:       r,
		}, nil
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func clientOptions(t *testing.T, spec clientSpec, transport http.RoundTripper) ankusa.Options {
	t.Helper()
	options := ankusa.Options{Timeout: time.Duration(spec.TimeoutMS) * time.Millisecond}
	if len(spec.Headers) > 0 {
		headers := http.Header{}
		for name, value := range spec.Headers {
			headers.Set(name, value)
		}
		options.Headers = headers
	}
	if transport != nil {
		options.HTTPClient = &http.Client{Transport: transport}
	}
	return options
}

func readBody(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	data, _ := io.ReadAll(r.Body)
	return data
}

func parseJSONBody(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return string(raw)
	}
	return decoded
}

func lowercaseHeaderNames(header http.Header) map[string]string {
	lowered := make(map[string]string, len(header))
	for name, values := range header {
		if len(values) == 0 {
			continue
		}
		lowered[strings.ToLower(name)] = values[0]
	}
	return lowered
}

// bodyBytes is a vector Body in bytes: text as UTF-8, base64 decoded, json
// compacted; a missing body is empty.
func bodyBytes(t *testing.T, spec *bodySpec) []byte {
	t.Helper()
	if spec == nil {
		return nil
	}
	switch {
	case spec.Text != nil:
		return []byte(*spec.Text)
	case spec.Base64 != nil:
		data, err := base64.StdEncoding.DecodeString(*spec.Base64)
		if err != nil {
			t.Fatalf("decode base64 body: %v", err)
		}
		return data
	case spec.JSON != nil:
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, spec.JSON); err != nil {
			t.Fatalf("compact json body: %v", err)
		}
		return compacted.Bytes()
	}
	return nil
}

// decode unmarshals an input field; a missing field is the zero value.
func decode[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var out T
	if len(raw) == 0 {
		return out
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	return out
}

func decodeAny(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

// redeemExpectation rebuilds a redeem `ok` from `{"body": Body}` into
// `{"body": {"base64": ...}}`: bytes can't round-trip through JSON.
func redeemExpectation(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var expected struct {
		Body *bodySpec `json:"body"`
	}
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatalf("decode redeem expectation: %v", err)
	}
	return map[string]any{
		"body": map[string]any{"base64": base64.StdEncoding.EncodeToString(bodyBytes(t, expected.Body))},
	}
}

// normalize round-trips a value through JSON, so structs and raw JSON compare
// in one domain.
func normalize(t *testing.T, v any) any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %#v: %v", v, err)
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	return out
}

func compact(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%#v", v)
	}
	return string(data)
}

// errorFields maps the vectors' error keys to the Go field names.
var errorFields = map[string]string{
	"status":         "Status",
	"body":           "Body",
	"code":           "Code",
	"field":          "Field",
	"message":        "Message",
	"conflicting_id": "ConflictingID",
	"max_routes":     "MaxRoutes",
	"role":           "Role",
}

// assertError checks a vector's `expect.error` against the returned error: the
// dynamic type must be an exported *XxxError of this module with the exact
// class name, and every other expected key must match its field.
func assertError(t *testing.T, err error, raw json.RawMessage) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error %s, got nil", raw)
	}

	var want map[string]json.RawMessage
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("decode expected error: %v", err)
	}
	var class string
	if err := json.Unmarshal(want["class"], &class); err != nil {
		t.Fatalf("expected error has no class: %v", err)
	}

	rt := reflect.TypeOf(err)
	if rt.Kind() != reflect.Pointer || rt.Elem().PkgPath() != modulePath {
		t.Fatalf("unmapped error %T: %v", err, err)
	}
	if got := rt.Elem().Name(); got != class {
		t.Fatalf("expected %s, got %s: %v", class, got, err)
	}

	for key, value := range want {
		if key == "class" {
			continue
		}

		var actual any
		if key == "retryable" {
			apiErr, ok := err.(ankusa.Error)
			if !ok {
				t.Fatalf("%T does not implement ankusa.Error", err)
			}
			actual = apiErr.Retryable()
		} else {
			actual = errorField(t, err, key)
		}

		normalized := normalize(t, actual)
		expected := decodeAny(t, value)
		if !reflect.DeepEqual(normalized, expected) {
			t.Errorf("%s.%s: got %s, want %s", class, key, compact(t, normalized), compact(t, expected))
		}
	}
}

func errorField(t *testing.T, err error, key string) any {
	t.Helper()
	name, ok := errorFields[key]
	if !ok {
		t.Fatalf("no field mapping for error key %q", key)
	}
	field := reflect.ValueOf(err).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("%T has no field %s (for %q)", err, name, key)
	}
	return field.Interface()
}

// assertRequests checks the recorded requests: same count and order, exact
// method and path, headers as a subset match on lowercase names, and the body
// only when the vector pins it (`null` meaning "no body").
func assertRequests(t *testing.T, got []recordedRequest, raw json.RawMessage) {
	t.Helper()

	var want []struct {
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("decode expected requests: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d requests, got %d: %+v", len(want), len(got), got)
	}
	for i, expected := range want {
		actual := got[i]
		if actual.Method != expected.Method {
			t.Errorf("request %d method: got %q, want %q", i, actual.Method, expected.Method)
		}
		if actual.Path != expected.Path {
			t.Errorf("request %d path: got %q, want %q", i, actual.Path, expected.Path)
		}
		for name, value := range expected.Headers {
			if actual.Headers[name] != value {
				t.Errorf("request %d header %s: got %q, want %q", i, name, actual.Headers[name], value)
			}
		}
		if expected.Body != nil {
			expectedBody := decodeAny(t, expected.Body)
			if !reflect.DeepEqual(actual.Body, expectedBody) {
				t.Errorf("request %d body: got %s, want %s", i, compact(t, actual.Body), compact(t, expectedBody))
			}
		}
	}
}
