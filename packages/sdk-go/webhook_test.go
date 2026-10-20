package ankusa_test

import (
	"net/http"
	"testing"

	ankusa "github.com/jamescarr/ankusa/packages/sdk-go"
)

// Edges no conformance vector expresses: the field is absent entirely
// (rather than present-but-empty), and a flagged header spelled "true".

func TestParseHeadersVerificationAbsentIsNil(t *testing.T) {
	h := http.Header{}
	h.Set("x-ankusa-id", "01a0")
	h.Set("x-ankusa-source", "demo")

	hook, err := ankusa.ParseHeaders(h)
	if err != nil {
		t.Fatalf("ParseHeaders: %v", err)
	}
	if hook.Verification != nil {
		t.Fatalf("expected nil Verification, got %+v", hook.Verification)
	}
}

func TestParseHeadersVerificationFlaggedTrue(t *testing.T) {
	h := http.Header{}
	h.Set("x-ankusa-id", "01a0")
	h.Set("x-ankusa-source", "demo")
	h.Set("x-ankusa-verification", "failed")
	h.Set("x-ankusa-verification-flagged", "true")

	hook, err := ankusa.ParseHeaders(h)
	if err != nil {
		t.Fatalf("ParseHeaders: %v", err)
	}
	if hook.Verification == nil {
		t.Fatalf("expected non-nil Verification")
	}
	if hook.Verification.Status != "failed" || !hook.Verification.Flagged {
		t.Fatalf("got %+v, want {failed true}", hook.Verification)
	}
}
