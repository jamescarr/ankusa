package ankusa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultTimeout bounds one request — connect through the last body byte —
// when Options.Timeout is not set.
const DefaultTimeout = 10 * time.Second

// Options configures any client. The zero value is valid.
type Options struct {
	// Headers are sent on every request; cloned at construction, so later
	// mutations do not affect the client.
	Headers http.Header
	// Timeout is the per-request deadline, covering connect through the last
	// body byte. Zero or negative means DefaultTimeout.
	Timeout time.Duration
	// HTTPClient is the transport hook. Nil means a new client on
	// http.DefaultTransport. The caller's client is copied, never mutated,
	// and redirects are never followed: a 3xx comes back as a response and is
	// classified per client.
	HTTPClient *http.Client
}

// Error is implemented by every error this package returns from a request or
// parser. Retryable reports whether the same call may succeed later: only the
// three *UnavailableError types return true.
type Error interface {
	error
	Retryable() bool
}

// httpCore is the shared request machinery under every client: fixed base URL
// and headers, a per-request timeout, and a client that never follows
// redirects. Immutable after newCore returns.
type httpCore struct {
	baseURL string
	headers http.Header
	timeout time.Duration
	client  *http.Client
}

// newCore validates baseURL and freezes opts. The error is a plain error, not
// an [Error]: an unusable base URL is misuse at construction, not a failed
// request to retry.
func newCore(baseURL string, opts Options) (*httpCore, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("ankusa: invalid base URL %q: must be an absolute http(s) URL: %w", baseURL, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("ankusa: invalid base URL %q: must be an absolute http(s) URL", baseURL)
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	// A copy, so CheckRedirect never touches the caller's client.
	client := http.Client{}
	if opts.HTTPClient != nil {
		client = *opts.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &httpCore{
		// Trimmed so a base path prefix survives baseURL + path.
		baseURL: strings.TrimRight(baseURL, "/"),
		headers: opts.Headers.Clone(),
		timeout: timeout,
		client:  &client,
	}, nil
}

// rawResponse is what a client classifies: status, headers, and the full body
// (read inside the timeout window, so a stalled body is a timeout, not a
// partial success).
type rawResponse struct {
	status int
	header http.Header
	body   []byte
}

// send performs one request to baseURL+path and returns the response body
// verbatim. Every returned error — build, transport, timeout, read — is raw;
// each caller wraps it as its client's *UnavailableError.
func (c *httpCore) send(ctx context.Context, method, path string, body []byte) (*rawResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header = c.headers.Clone()
	if req.Header == nil {
		req.Header = http.Header{}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &rawResponse{status: resp.StatusCode, header: resp.Header, body: data}, nil
}

// encodeJSON marshals a request body. The error is a caller bug — an
// unencodable Metadata value — so it is plain, never retryable.
func encodeJSON(op string, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ankusa: encode %s request: %w", op, err)
	}
	return body, nil
}

// appendQuery appends k=v pairs, escaped, in the given order. Deliberately
// not url.Values.Encode: that sorts keys, and query order is the caller's
// input order.
func appendQuery(path string, pairs [][2]string) string {
	if len(pairs) == 0 {
		return path
	}
	var b strings.Builder
	b.WriteString(path)
	for i, pair := range pairs {
		if i == 0 {
			b.WriteByte('?')
		} else {
			b.WriteByte('&')
		}
		b.WriteString(pair[0])
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(pair[1]))
	}
	return b.String()
}
