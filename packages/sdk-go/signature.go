package ankusa

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// InvalidSignatureError reports a delivery whose Standard Webhooks signature
// does not verify. Code is one of invalid_secret, missing_header,
// invalid_timestamp, timestamp_out_of_tolerance, no_matching_signature; Field
// names the header at fault and is nil for invalid_secret. Never retryable:
// answer 401.
type InvalidSignatureError struct {
	Code  string
	Field *string
}

func (e *InvalidSignatureError) Error() string {
	if e.Field != nil {
		return fmt.Sprintf("invalid webhook signature (%s): %s", e.Code, *e.Field)
	}
	return fmt.Sprintf("invalid webhook signature (%s)", e.Code)
}

// Retryable is always false: the sender retries with the same bytes.
func (e *InvalidSignatureError) Retryable() bool { return false }

// VerifiedSignature is a verified delivery's webhook-id and webhook-timestamp.
type VerifiedSignature struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

// VerifyOptions tunes VerifySignature. The zero value is a 300 s tolerance
// against the clock.
type VerifyOptions struct {
	// ToleranceSeconds bounds |now - webhook-timestamp|; nil means 300.
	ToleranceSeconds *int64
	// Now is the unix time to judge the timestamp against; nil means the clock.
	Now *int64
}

// VerifySignature verifies the Standard Webhooks signature an HTTP sink with
// a secret adds (https://www.standardwebhooks.com/). webhook-signature holds
// space-separated "v1,<base64>" entries, each an HMAC-SHA256 over
// "<webhook-id>.<webhook-timestamp>.<body>". Any v1 entry matching any secret
// ("whsec_" + base64, or any other string used as its own bytes) passes,
// compared in constant time, provided the timestamp is inside the tolerance.
// body must be the raw bytes received.
func VerifySignature(h http.Header, body []byte, secrets []string, opts VerifyOptions) (VerifiedSignature, error) {
	keys, err := decodeSecrets(secrets)
	if err != nil {
		return VerifiedSignature{}, err
	}

	required := func(name string) (string, error) {
		value, ok := headerValue(h, name)
		if !ok || value == "" {
			field := name
			return "", &InvalidSignatureError{Code: "missing_header", Field: &field}
		}
		return value, nil
	}

	id, err := required("webhook-id")
	if err != nil {
		return VerifiedSignature{}, err
	}
	rawTimestamp, err := required("webhook-timestamp")
	if err != nil {
		return VerifiedSignature{}, err
	}
	signature, err := required("webhook-signature")
	if err != nil {
		return VerifiedSignature{}, err
	}

	timestampField := "webhook-timestamp"
	if strings.Trim(rawTimestamp, "0123456789") != "" {
		return VerifiedSignature{}, &InvalidSignatureError{Code: "invalid_timestamp", Field: &timestampField}
	}
	timestamp, err := strconv.ParseInt(rawTimestamp, 10, 64)
	if err != nil {
		return VerifiedSignature{}, &InvalidSignatureError{Code: "invalid_timestamp", Field: &timestampField}
	}

	now := time.Now().Unix()
	if opts.Now != nil {
		now = *opts.Now
	}
	tolerance := int64(300)
	if opts.ToleranceSeconds != nil {
		tolerance = *opts.ToleranceSeconds
	}
	if diff := now - timestamp; diff > tolerance || -diff > tolerance {
		return VerifiedSignature{}, &InvalidSignatureError{Code: "timestamp_out_of_tolerance", Field: &timestampField}
	}

	signed := append([]byte(id+"."+rawTimestamp+"."), body...)
	var candidates [][]byte
	for _, entry := range strings.Split(signature, " ") {
		if rest, ok := strings.CutPrefix(entry, "v1,"); ok {
			candidates = append(candidates, []byte(rest))
		}
	}

	for _, key := range keys {
		mac := hmac.New(sha256.New, key)
		mac.Write(signed)
		expected := []byte(base64.StdEncoding.EncodeToString(mac.Sum(nil)))
		for _, candidate := range candidates {
			if hmac.Equal(candidate, expected) {
				return VerifiedSignature{ID: id, Timestamp: timestamp}, nil
			}
		}
	}

	signatureField := "webhook-signature"
	return VerifiedSignature{}, &InvalidSignatureError{Code: "no_matching_signature", Field: &signatureField}
}

func decodeSecrets(secrets []string) ([][]byte, error) {
	if len(secrets) == 0 {
		return nil, &InvalidSignatureError{Code: "invalid_secret"}
	}
	keys := make([][]byte, 0, len(secrets))
	for _, secret := range secrets {
		if encoded, ok := strings.CutPrefix(secret, "whsec_"); ok {
			key, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil || len(key) == 0 {
				return nil, &InvalidSignatureError{Code: "invalid_secret"}
			}
			keys = append(keys, key)
			continue
		}
		if secret == "" {
			return nil, &InvalidSignatureError{Code: "invalid_secret"}
		}
		keys = append(keys, []byte(secret))
	}
	return keys, nil
}
