package ankusa

import (
	"bytes"
	"encoding/json"
	"regexp"
)

// decimalDigits matches a lifecycle event's `sequence`: the audit sequence
// number as a decimal string.
var decimalDigits = regexp.MustCompile(`^[0-9]+$`)

// LifecycleEvent is a decoded lifecycle event: the CloudEvents 1.0 (structured
// mode) event an Ankusa instance publishes when a source or a route is
// created, updated or deleted, when a tenant is erased, or when the instance is
// restored from a backup. It arrives as the body of a queue message from the
// instance's lifecycle sinks (content type application/cloudevents+json,
// source_id "ankusa:lifecycle"): decode the message with DecodeMessage, redeem
// its body, and hand the bytes to DecodeLifecycle.
//
// Delivery is at least once and in commit order. ID is derived from the change,
// so the same event always has the same id, and Revision (per Subject) lets a
// consumer ignore a stale update. The pointer fields are nil when the event
// omitted them or said null.
type LifecycleEvent struct {
	Specversion string `json:"specversion"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	// Type is io.ankusa.source.created, io.ankusa.route.updated, …; a type
	// this SDK does not know passes through.
	Type    string `json:"type"`
	Subject string `json:"subject"`
	// Time is when the change committed, as the wire says it (an RFC 3339
	// string, not parsed).
	Time            string  `json:"time"`
	Datacontenttype *string `json:"datacontenttype"`
	// Data is the entity as the admin API shows it, secrets redacted; nil when
	// the event carries none. Numbers are json.Number, so none loses precision.
	Data     map[string]any `json:"data"`
	Revision *int64         `json:"revision"`
	Actor    *string        `json:"actor"`
	Instance *string        `json:"instance"`
	// Sequence is the instance's audit sequence number, the decimal string it
	// is on the wire.
	Sequence    *string `json:"sequence"`
	Replayid    *string `json:"replayid"`
	Traceparent *string `json:"traceparent"`
	Tracestate  *string `json:"tracestate"`
}

// DecodeLifecycle decodes one lifecycle event (the JSON of the CloudEvent).
// Every failure is a *InvalidMessageError (Retryable() false): invalid_json,
// not_an_object, unsupported_version (specversion is not "1.0") or
// invalid_field with the offending Field. The checks run in a fixed order and
// the first failure wins; see conformance/README.md. Keys and event types this
// SDK does not know are ignored.
func DecodeLifecycle(data []byte) (LifecycleEvent, error) {
	if !json.Valid(data) {
		return LifecycleEvent{}, &InvalidMessageError{Code: "invalid_json"}
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return LifecycleEvent{}, &InvalidMessageError{Code: "invalid_json"}
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return LifecycleEvent{}, &InvalidMessageError{Code: "not_an_object"}
	}

	if spec, ok := obj["specversion"].(string); !ok || spec != "1.0" {
		return LifecycleEvent{}, &InvalidMessageError{Code: "unsupported_version"}
	}
	event := LifecycleEvent{Specversion: "1.0"}

	required := []struct {
		key string
		dst *string
	}{
		{"id", &event.ID},
		{"source", &event.Source},
		{"type", &event.Type},
		{"subject", &event.Subject},
		{"time", &event.Time},
	}
	for _, field := range required {
		value, ok := obj[field.key].(string)
		if !ok || value == "" {
			return LifecycleEvent{}, invalidField(field.key)
		}
		*field.dst = value
	}

	datacontenttype, ok := optionalString(obj, "datacontenttype")
	if !ok {
		return LifecycleEvent{}, invalidField("datacontenttype")
	}
	event.Datacontenttype = datacontenttype

	if value, present := obj["data"]; present && value != nil {
		body, ok := value.(map[string]any)
		if !ok {
			return LifecycleEvent{}, invalidField("data")
		}
		event.Data = body
	}

	if value, present := obj["revision"]; present && value != nil {
		revision, ok := integerValue(value)
		if !ok || revision < 1 {
			return LifecycleEvent{}, invalidField("revision")
		}
		event.Revision = &revision
	}

	actor, ok := optionalString(obj, "actor")
	if !ok {
		return LifecycleEvent{}, invalidField("actor")
	}
	event.Actor = actor

	instance, ok := optionalString(obj, "instance")
	if !ok {
		return LifecycleEvent{}, invalidField("instance")
	}
	event.Instance = instance

	sequence, ok := optionalString(obj, "sequence")
	if !ok || (sequence != nil && !decimalDigits.MatchString(*sequence)) {
		return LifecycleEvent{}, invalidField("sequence")
	}
	event.Sequence = sequence

	replayid, ok := optionalString(obj, "replayid")
	if !ok {
		return LifecycleEvent{}, invalidField("replayid")
	}
	event.Replayid = replayid

	traceparent, ok := optionalString(obj, "traceparent")
	if !ok {
		return LifecycleEvent{}, invalidField("traceparent")
	}
	event.Traceparent = traceparent

	tracestate, ok := optionalString(obj, "tracestate")
	if !ok {
		return LifecycleEvent{}, invalidField("tracestate")
	}
	event.Tracestate = tracestate

	return event, nil
}
