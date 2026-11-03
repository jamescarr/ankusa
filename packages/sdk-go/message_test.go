package ankusa_test

import (
	"errors"
	"testing"

	ankusa "github.com/jamescarr/ankusa/packages/sdk-go"
)

// The vectors (message.event_type) cover the decode rules; this covers the
// field by its Go type: a nil pointer for null or absent, the string otherwise.

func inlineMessage(extra string) []byte {
	return []byte(`{"v":1,"id":"01a0","source_id":"demo","received_at":1,"size":5,"body_base64":"aGVsbG8="` + extra + `}`)
}

func TestDecodeMessageEventType(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra string
		want  *string
	}{
		{"absent is nil", "", nil},
		{"null is nil", `,"event_type":null`, nil},
		{"a string is carried", `,"event_type":"invoice.paid"`, new("invoice.paid")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, err := ankusa.DecodeMessage(inlineMessage(tc.extra))
			if err != nil {
				t.Fatalf("DecodeMessage: %v", err)
			}
			switch {
			case tc.want == nil && message.EventType != nil:
				t.Fatalf("EventType = %q, want nil", *message.EventType)
			case tc.want != nil && (message.EventType == nil || *message.EventType != *tc.want):
				t.Fatalf("EventType = %v, want %q", message.EventType, *tc.want)
			}
		})
	}
}

func TestDecodeMessageEventTypeMustBeAString(t *testing.T) {
	_, err := ankusa.DecodeMessage(inlineMessage(`,"event_type":7`))

	var invalid *ankusa.InvalidMessageError
	if !errors.As(err, &invalid) {
		t.Fatalf("expected InvalidMessageError, got %v", err)
	}
	if invalid.Code != "invalid_field" || invalid.Field == nil || *invalid.Field != "event_type" {
		t.Fatalf("got code %q field %v, want invalid_field event_type", invalid.Code, invalid.Field)
	}
}
