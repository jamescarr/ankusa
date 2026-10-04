package ankusa

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

// lowercaseHex64 matches the queue message's `sha256`: 64 lowercase hex
// characters.
var lowercaseHex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Message is a decoded v1 queue message, as published by Ankusa's sinks. The
// body travels either inline (BodyBase64, with Body holding the decoded
// bytes) or as a claim-check reference (Claim) to be redeemed separately.
//
// The pointer fields are nil when the message omitted them, matching the
// wire contract; Headers is never nil (absent decodes to an empty map).
//
// ShippedIdempotencyKey is the wire's idempotency_key: the tenant-scoped key
// Ankusa computed once for the hook, nil for a message from a node that
// predates the field. Read the key through IdempotencyKey, which falls back
// to computing it.
type Message struct {
	V                     int               `json:"v"`
	ID                    string            `json:"id"`
	SourceID              string            `json:"source_id"`
	TenantID              *string           `json:"tenant_id"`
	ReceivedAt            int64             `json:"received_at"`
	ContentType           *string           `json:"content_type"`
	Size                  int64             `json:"size"`
	BodyBase64            *string           `json:"body_base64"`
	Claim                 *string           `json:"claim"`
	SHA256                *string           `json:"sha256"`
	DedupeKey             *string           `json:"dedupe_key"`
	ReplayID              *string           `json:"replay_id"`
	ShippedIdempotencyKey *string           `json:"idempotency_key"`
	Headers               map[string]string `json:"headers"`

	// Body is the decoded inline body. It is nil for a claim message.
	Body []byte `json:"-"`
}

// DecodeMessage decodes a v1 queue message. Every failure is a
// *InvalidMessageError with a Code, a Field (nil when no field applies), and
// Retryable() false: a malformed message will never decode, so it must be
// dead-lettered rather than retried.
//
// The checks run in a fixed order and the first failure wins; see
// conformance/README.md for the normative rules.
func DecodeMessage(data []byte) (Message, error) {
	if !json.Valid(data) {
		return Message{}, &InvalidMessageError{Code: "invalid_json"}
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return Message{}, &InvalidMessageError{Code: "invalid_json"}
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return Message{}, &InvalidMessageError{Code: "not_an_object"}
	}

	// 2. Version: `v` must be the integer 1.
	if v, ok := integerValue(obj["v"]); !ok || v != 1 {
		return Message{}, &InvalidMessageError{Code: "unsupported_version"}
	}

	msg := Message{V: 1, Headers: map[string]string{}}

	// 3. Field types, in order.
	id, ok := obj["id"].(string)
	if !ok || id == "" {
		return Message{}, invalidField("id")
	}
	msg.ID = id

	sourceID, ok := obj["source_id"].(string)
	if !ok {
		return Message{}, invalidField("source_id")
	}
	msg.SourceID = sourceID

	receivedAt, ok := integerValue(obj["received_at"])
	if !ok {
		return Message{}, invalidField("received_at")
	}
	msg.ReceivedAt = receivedAt

	size, ok := integerValue(obj["size"])
	if !ok || size < 0 {
		return Message{}, invalidField("size")
	}
	msg.Size = size

	tenantID, ok := optionalString(obj, "tenant_id")
	if !ok {
		return Message{}, invalidField("tenant_id")
	}
	msg.TenantID = tenantID

	contentType, ok := optionalString(obj, "content_type")
	if !ok {
		return Message{}, invalidField("content_type")
	}
	msg.ContentType = contentType

	dedupeKey, ok := optionalString(obj, "dedupe_key")
	if !ok {
		return Message{}, invalidField("dedupe_key")
	}
	msg.DedupeKey = dedupeKey

	replayID, ok := optionalString(obj, "replay_id")
	if !ok {
		return Message{}, invalidField("replay_id")
	}
	msg.ReplayID = replayID

	shipped, ok := optionalString(obj, "idempotency_key")
	if !ok {
		return Message{}, invalidField("idempotency_key")
	}
	msg.ShippedIdempotencyKey = shipped

	if headers, ok := headersValue(obj["headers"]); ok {
		msg.Headers = headers
	} else {
		return Message{}, invalidField("headers")
	}

	sha, ok := optionalString(obj, "sha256")
	if !ok || (sha != nil && !lowercaseHex64.MatchString(*sha)) {
		return Message{}, invalidField("sha256")
	}
	msg.SHA256 = sha

	// 4. Body form.
	bodyRaw, hasBodyKey := obj["body_base64"]
	claimRaw, hasClaimKey := obj["claim"]
	bodyPresent := hasBodyKey && bodyRaw != nil
	claimPresent := hasClaimKey && claimRaw != nil

	var claimTenant string

	switch {
	case bodyPresent && claimPresent:
		return Message{}, &InvalidMessageError{Code: "ambiguous_body"}

	case !bodyPresent && !claimPresent:
		return Message{}, &InvalidMessageError{Code: "missing_body"}

	case bodyPresent:
		bodyStr, ok := bodyRaw.(string)
		if !ok {
			return Message{}, &InvalidMessageError{Code: "invalid_body_base64"}
		}
		decoded, err := base64.StdEncoding.DecodeString(bodyStr)
		if err != nil {
			return Message{}, &InvalidMessageError{Code: "invalid_body_base64"}
		}
		msg.Body = decoded
		encoded := base64.StdEncoding.EncodeToString(decoded)
		msg.BodyBase64 = &encoded

	default:
		claimStr, ok := claimRaw.(string)
		if !ok {
			return Message{}, invalidField("claim")
		}
		parsed, err := ParseClaimRef(claimStr)
		if err != nil {
			return Message{}, invalidField("claim")
		}
		if msg.SHA256 == nil {
			return Message{}, invalidField("sha256")
		}
		msg.Claim = &claimStr
		claimTenant = parsed.TenantID
	}

	// 5. Inline body integrity.
	if msg.BodyBase64 != nil {
		if int64(len(msg.Body)) != msg.Size {
			return Message{}, &InvalidMessageError{Code: "size_mismatch"}
		}
		if msg.SHA256 != nil {
			digest := sha256.Sum256(msg.Body)
			if hex.EncodeToString(digest[:]) != *msg.SHA256 {
				return Message{}, &InvalidMessageError{Code: "integrity"}
			}
		}
	}

	// 6. Claim tenant agreement.
	if msg.Claim != nil && msg.TenantID != nil && *msg.TenantID != claimTenant {
		return Message{}, &InvalidMessageError{Code: "tenant_mismatch"}
	}

	return msg, nil
}

// IdempotencyKey is the key a consumer dedupes this delivery on: the key
// Ankusa shipped in the message's idempotency_key when it is a non-empty
// string. For a message from a node that predates the field it is computed:
// tenant:source_id:dedupe_key (tenant "default" when there is none) for a
// non-empty dedupe key, else the message id. With includeReplay it also
// distinguishes a replay of an event already processed; without it (the
// default), a replay dedupes to the same key as the original delivery.
func (m Message) IdempotencyKey(includeReplay bool) string {
	return idempotencyKey(m.ShippedIdempotencyKey, m.TenantID, m.SourceID, m.ID, m.DedupeKey, m.ReplayID, includeReplay)
}

// idempotencyKey is the shared rule behind Message.IdempotencyKey and
// HookHeaders.IdempotencyKey. sourceID and tenant are the message's source_id
// and tenant_id, or the HTTP sink's x-ankusa-source and x-ankusa-tenant for
// headers; shipped is the idempotency_key / x-ankusa-idempotency-key value.
func idempotencyKey(shipped, tenant *string, sourceID, id string, dedupeKey, replayID *string, includeReplay bool) string {
	var key string
	switch {
	case shipped != nil && *shipped != "":
		key = *shipped
	case dedupeKey != nil && *dedupeKey != "":
		tenantID := "default"
		if tenant != nil {
			tenantID = *tenant
		}
		key = tenantID + ":" + sourceID + ":" + *dedupeKey
	default:
		key = id
	}
	if includeReplay && replayID != nil {
		key += "#replay:" + *replayID
	}
	return key
}

// integerValue reads a json.Number as an int64; a missing, non-numeric, or
// fractional value is not an integer.
func integerValue(v any) (int64, bool) {
	number, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := number.Int64()
	if err != nil {
		return 0, false
	}
	return parsed, true
}

// optionalString reads a field that may be absent, null, or a string. The
// bool is false only when the field is present and neither null nor a string.
func optionalString(obj map[string]any, key string) (*string, bool) {
	value, present := obj[key]
	if !present || value == nil {
		return nil, true
	}
	str, ok := value.(string)
	if !ok {
		return nil, false
	}
	return &str, true
}

// headersValue reads the `headers` field: absent or null is an empty map, and
// a present object must have only string values. The bool is false on any
// other shape.
func headersValue(value any) (map[string]string, bool) {
	headers := map[string]string{}
	if value == nil {
		return headers, true
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	for name, raw := range obj {
		str, ok := raw.(string)
		if !ok {
			return nil, false
		}
		headers[name] = str
	}
	return headers, true
}

// invalidField builds an invalid_field error naming the offending key.
func invalidField(field string) *InvalidMessageError {
	return &InvalidMessageError{Code: "invalid_field", Field: &field}
}

// InvalidMessageError reports a queue message DecodeMessage could not decode.
// Code is the machine-readable rule name (e.g. "invalid_json",
// "unsupported_version", "integrity"); Field names the offending key for the
// invalid_field code and is nil otherwise.
type InvalidMessageError struct {
	Code  string
	Field *string
}

func (e *InvalidMessageError) Error() string {
	if e.Field != nil {
		return fmt.Sprintf("invalid queue message (%s): field %q", e.Code, *e.Field)
	}
	return fmt.Sprintf("invalid queue message (%s)", e.Code)
}

func (e *InvalidMessageError) Retryable() bool { return false }
