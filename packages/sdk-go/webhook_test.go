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

func TestVerificationTrusted(t *testing.T) {
	var absent *ankusa.Verification
	cases := []struct {
		name string
		v    *ankusa.Verification
		want bool
	}{
		{"ok", &ankusa.Verification{Status: "ok"}, true},
		{"flagged", &ankusa.Verification{Status: "failed", Flagged: true}, false},
		{"ok but flagged", &ankusa.Verification{Status: "ok", Flagged: true}, false},
		{"skipped", &ankusa.Verification{Status: "skipped"}, false},
		{"unknown status", &ankusa.Verification{Status: "attested"}, false},
		{"absent", absent, false},
	}
	for _, tc := range cases {
		if got := tc.v.Trusted(); got != tc.want {
			t.Errorf("%s: Trusted() = %v, want %v", tc.name, got, tc.want)
		}
	}
	if (ankusa.HookHeaders{}).Trusted() {
		t.Errorf("HookHeaders without verification must be untrusted")
	}
	if (ankusa.Message{}).Trusted() {
		t.Errorf("Message without verification must be untrusted")
	}
}

func TestParseHeadersDeliveryMetadata(t *testing.T) {
	const sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	h := http.Header{}
	h.Set("x-ankusa-id", "01a0")
	h.Set("x-ankusa-received-at", "1720000000000")
	h.Set("x-ankusa-body-sha256", sha256)

	hook, err := ankusa.ParseHeaders(h)
	if err != nil {
		t.Fatalf("ParseHeaders: %v", err)
	}
	if hook.ReceivedAt == nil || *hook.ReceivedAt != 1720000000000 {
		t.Fatalf("ReceivedAt = %v, want 1720000000000", hook.ReceivedAt)
	}
	if hook.BodySHA256 == nil || *hook.BodySHA256 != sha256 {
		t.Fatalf("BodySHA256 = %v, want %s", hook.BodySHA256, sha256)
	}
}

func TestParseHeadersDeliveryMetadataMalformedIsNil(t *testing.T) {
	h := http.Header{}
	h.Set("x-ankusa-id", "01a0")
	h.Set("x-ankusa-received-at", "-1")
	h.Set("x-ankusa-body-sha256", "ABC")

	hook, err := ankusa.ParseHeaders(h)
	if err != nil {
		t.Fatalf("ParseHeaders: %v", err)
	}
	if hook.ReceivedAt != nil || hook.BodySHA256 != nil {
		t.Fatalf("expected nil ReceivedAt and BodySHA256, got %v and %v", hook.ReceivedAt, hook.BodySHA256)
	}
}
