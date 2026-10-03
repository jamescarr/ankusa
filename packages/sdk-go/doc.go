// Package ankusa is the Go client SDK for an Ankusa deployment: the
// claim-check gateway client ([ClaimCheckClient]), the route-management
// client ([RoutesClient]), the operator client ([AdminClient]), plus
// [DecodeMessage], [ParseClaimRef], and [ParseHeaders] for consumers of the
// v1 queue message, claim-check URNs, and Ankusa's HTTP sink deliveries.
//
// Every client is built by a New…Client constructor that takes a base URL —
// the listener's own host and port, e.g. the claim-check gateway's, the
// route-management listener's (`routes.admin.port`), or the admin listener's
// (`admin.port`) — and [Options]. Constructed clients are immutable and safe
// for concurrent use.
//
// Every error this package returns from a request or a parser is a concrete
// *XxxError type that implements [Error], so a single retryable bit decides
// dead-letter vs. retry. Caller misuse is a plain error instead, raised before
// any request: a base URL that is not an absolute http(s) URL, or a request
// body that cannot be encoded as JSON (an unencodable Metadata value).
package ankusa
