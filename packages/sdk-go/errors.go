package ankusa

import (
	"encoding/json"
	"fmt"
)

// Every error type below is returned unwrapped as a pointer — `&T{...}` —
// because the conformance vectors match the dynamic type by exact name.
//
// The names also deliberately break Go's ID-initialism convention
// (MissingHookIdError, InvalidRouteIdError): the vectors and the other SDKs
// spell them that way, and `conformance/` matches exact names.

// InvalidClaimRefError reports a `ref` that is not a well-formed claim-check
// URN, or a `sha256` that is not 64 lowercase hex characters. No request is
// made.
type InvalidClaimRefError struct {
	Message string
}

// ClaimNotFoundError reports a gateway 404: expired by retention, or never
// written.
type ClaimNotFoundError struct {
	TenantID string
	ClaimID  string
}

// ClaimRejectedError reports a gateway 4xx other than 404. Body is the
// response body decoded as JSON when the Content-Type is JSON, and the raw
// string otherwise (possibly "" for an empty body).
type ClaimRejectedError struct {
	Status int
	Body   any
}

// ClaimIntegrityError reports bytes whose sha256 does not match the expected
// digest.
type ClaimIntegrityError struct {
	TenantID string
	ClaimID  string
}

// ClaimCheckUnavailableError reports a claim-check gateway that could not be
// reached, timed out, or answered anything other than 200/404/4xx. Retryable.
type ClaimCheckUnavailableError struct {
	Message string
	Cause   error
}

// MissingHookIdError reports a request without a non-empty x-ankusa-id.
type MissingHookIdError struct{}

// RouteNotFoundError reports a 404 from the route-management listener. ID is
// "" for collection calls.
type RouteNotFoundError struct {
	ID string
}

// InvalidRouteIdError reports a route id that is empty or exactly `.` or
// `..`. No request is made.
type InvalidRouteIdError struct {
	ID string
}

// RoutesRejectedError reports a 4xx other than 404 from the
// route-management listener.
type RoutesRejectedError struct {
	Status        int
	Code          string
	Field         string
	Message       string
	ConflictingID string
	MaxRoutes     int
}

// RoutesUnavailableError reports a route-management listener that could not
// be reached, answered a non-2xx that is not 4xx, or answered 2xx with a body
// that is not JSON. Retryable.
type RoutesUnavailableError struct {
	Message string
	Cause   error
}

// RoleNotEnabledError reports a 409 `role_not_enabled` from the admin
// listener: the queried role is not running on this instance.
type RoleNotEnabledError struct {
	Role string
}

// AdminRejectedError reports a 4xx other than that 409 from the admin
// listener. Code is the body's `error` field.
type AdminRejectedError struct {
	Status int
	Code   string
}

// AdminUnavailableError reports an admin listener that could not be reached,
// answered a non-2xx that is not 4xx, or answered 2xx with a body that is not
// JSON. Retryable.
type AdminUnavailableError struct {
	Message string
	Cause   error
}

func (e *InvalidClaimRefError) Error() string { return e.Message }

func (e *ClaimNotFoundError) Error() string {
	return fmt.Sprintf("claim not found: %s/%s", e.TenantID, e.ClaimID)
}

func (e *ClaimRejectedError) Error() string {
	body, err := json.Marshal(e.Body)
	if err != nil {
		body = []byte(fmt.Sprintf("%v", e.Body))
	}
	return fmt.Sprintf("claim-check rejected redeem (%d): %s", e.Status, body)
}

func (e *ClaimIntegrityError) Error() string {
	return fmt.Sprintf("claim sha256 mismatch for %s/%s", e.TenantID, e.ClaimID)
}

func (e *ClaimCheckUnavailableError) Error() string { return unavailableMessage(e.Message, e.Cause) }

func (e *MissingHookIdError) Error() string { return "missing x-ankusa-id header" }

func (e *RouteNotFoundError) Error() string {
	if e.ID == "" {
		return "route not found"
	}
	return fmt.Sprintf("route not found: %q", e.ID)
}

func (e *InvalidRouteIdError) Error() string { return fmt.Sprintf("invalid route id: %q", e.ID) }

func (e *RoutesRejectedError) Error() string {
	message := fmt.Sprintf("routes rejected (%d)", e.Status)
	if e.Code != "" {
		message += ": " + e.Code
	}
	if e.Message != "" {
		message += ": " + e.Message
	}
	return message
}

func (e *RoutesUnavailableError) Error() string { return unavailableMessage(e.Message, e.Cause) }

func (e *RoleNotEnabledError) Error() string {
	message := "role not enabled"
	if e.Role != "" {
		message += ": " + e.Role
	}
	return message
}

func (e *AdminRejectedError) Error() string {
	message := fmt.Sprintf("admin rejected (%d)", e.Status)
	if e.Code != "" {
		message += ": " + e.Code
	}
	return message
}

func (e *AdminUnavailableError) Error() string { return unavailableMessage(e.Message, e.Cause) }

func (e *InvalidClaimRefError) Retryable() bool       { return false }
func (e *ClaimNotFoundError) Retryable() bool         { return false }
func (e *ClaimRejectedError) Retryable() bool         { return false }
func (e *ClaimIntegrityError) Retryable() bool        { return false }
func (e *MissingHookIdError) Retryable() bool         { return false }
func (e *RouteNotFoundError) Retryable() bool         { return false }
func (e *InvalidRouteIdError) Retryable() bool        { return false }
func (e *RoutesRejectedError) Retryable() bool        { return false }
func (e *RoleNotEnabledError) Retryable() bool        { return false }
func (e *AdminRejectedError) Retryable() bool         { return false }
func (e *ClaimCheckUnavailableError) Retryable() bool { return true }
func (e *RoutesUnavailableError) Retryable() bool     { return true }
func (e *AdminUnavailableError) Retryable() bool      { return true }

func (e *ClaimCheckUnavailableError) Unwrap() error { return e.Cause }
func (e *RoutesUnavailableError) Unwrap() error     { return e.Cause }
func (e *AdminUnavailableError) Unwrap() error      { return e.Cause }

func unavailableMessage(message string, cause error) string {
	if cause == nil {
		return message
	}
	return message + ": " + cause.Error()
}
