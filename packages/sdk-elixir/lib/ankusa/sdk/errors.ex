defmodule Ankusa.SDK.InvalidClaimRefError do
  @moduledoc """
  The ref isn't a `urn:ankusa:claim:v1:<tenant>:<claim_id>` claim-check ref, or
  the expected sha256 isn't 64-char lowercase hex.
  """

  @type t :: %__MODULE__{}

  defexception [:message, retryable: false]
end

defmodule Ankusa.SDK.ClaimNotFoundError do
  @moduledoc """
  The gateway returned `404`: no such object, expired by retention or never
  written.
  """

  @type t :: %__MODULE__{}

  defexception [:message, retryable: false]
end

defmodule Ankusa.SDK.ClaimRejectedError do
  @moduledoc """
  The gateway rejected the request (`400`, `403` — any `4xx` but `404`, `408`
  and `429`).

  `:status` is the HTTP status; `:body` is the decoded body when it was JSON,
  else the raw text (`""` for an empty body).
  """

  @type t :: %__MODULE__{status: non_neg_integer(), body: term()}

  defexception [:message, :status, :body, retryable: false]
end

defmodule Ankusa.SDK.ClaimIntegrityError do
  @moduledoc """
  The sha256 of the bytes the gateway returned doesn't match the expected
  sha256 the queue message carries.

  The gateway itself never checks this — see "Redeem a claim" in
  `docs/claim-check.md` — so this is the reader's own end-to-end check, always
  run before `redeem/3` returns.
  """

  @type t :: %__MODULE__{}

  defexception [:message, retryable: false]
end

defmodule Ankusa.SDK.ClaimCheckUnavailableError do
  @moduledoc """
  The gateway is unreachable, answered `408` or `429` (throttled or timed
  out), or answered a status other than `200` that
  `Ankusa.SDK.ClaimCheck.redeem/3` cannot classify as a rejection. Safe to
  retry.

  `:reason` is the `Req.TransportError`, `{:status, integer}`, or
  `:invalid_json`.
  """

  @type t :: %__MODULE__{reason: term()}

  defexception [:message, :reason, retryable: true]
end

defmodule Ankusa.SDK.MissingHookIdError do
  @moduledoc """
  The request carries no `x-ankusa-id` header (or an empty one), so it isn't an
  Ankusa HTTP-sink delivery.
  """

  @type t :: %__MODULE__{}

  defexception [:message]
end

defmodule Ankusa.SDK.InvalidSignatureError do
  @moduledoc """
  A delivery's Standard Webhooks signature does not verify
  (`Ankusa.SDK.Signature.verify/4`). Always `retryable: false`: answer `401`.

  `:code` is `"invalid_secret"`, `"missing_header"`, `"invalid_timestamp"`,
  `"timestamp_out_of_tolerance"` or `"no_matching_signature"`; `:field` names
  the header at fault (`nil` for `"invalid_secret"`).
  """

  @type t :: %__MODULE__{code: String.t(), field: String.t() | nil}

  defexception [:message, :code, :field, retryable: false]
end

defmodule Ankusa.SDK.InvalidRouteIdError do
  @moduledoc """
  The route id cannot be used to build a path.

  Raised before any request is sent. An id that isn't a string, is empty, or is
  exactly `.` or `..` is refused: URL parsers normalize those away, so
  `get_route(client, "..")` would quietly hit `/admin/` and return the list
  page as if it were a route. Every other id is percent-encoded as one path
  segment, never refused.
  """

  @type t :: %__MODULE__{}

  defexception [:message, retryable: false]
end

defmodule Ankusa.SDK.RouteNotFoundError do
  @moduledoc "The listener returned `404`: no such route."

  @type t :: %__MODULE__{}

  defexception [:message, retryable: false]
end

defmodule Ankusa.SDK.RoutesRejectedError do
  @moduledoc """
  The listener rejected the request (`400` or any other non-404 `4xx`).

  `:code` is the body's `error` field (`invalid_route`, `duplicate_route`,
  `too_many_routes`, ...); `:field`, `:message`, `:conflicting_id` and
  `:max_routes` are carried through when the body supplies them.
  """

  @type t :: %__MODULE__{
          status: non_neg_integer(),
          code: String.t() | nil,
          field: String.t() | nil,
          message: String.t() | nil,
          conflicting_id: String.t() | nil,
          max_routes: non_neg_integer() | nil
        }

  defexception [:message, :status, :code, :field, :conflicting_id, :max_routes, retryable: false]

  @impl Exception
  def message(%__MODULE__{} = error) do
    base = "route request rejected (#{error.status} #{error.code})"

    if is_nil(error.message), do: base, else: base <> ": #{error.message}"
  end
end

defmodule Ankusa.SDK.RoutesUnavailableError do
  @moduledoc """
  The listener is unreachable, answered `5xx`, returned an unfollowed redirect,
  or returned a non-JSON success body. Safe to retry.
  """

  @type t :: %__MODULE__{reason: term()}

  defexception [:message, :reason, retryable: true]
end

defmodule Ankusa.SDK.RoleNotEnabledError do
  @moduledoc """
  The listener returned `409 role_not_enabled`: this node does not run the role
  the operation needs. Ask another node; `:role` names it.
  """

  @type t :: %__MODULE__{role: String.t() | nil}

  defexception [:message, :role, retryable: false]
end

defmodule Ankusa.SDK.AdminRejectedError do
  @moduledoc """
  The listener rejected the request (any other `4xx`).

  `:status` is the HTTP status; `:code` is the body's `error` field
  (`invalid_filter`, ...).
  """

  @type t :: %__MODULE__{status: non_neg_integer(), code: String.t() | nil}

  defexception [:message, :status, :code, retryable: false]
end

defmodule Ankusa.SDK.AdminUnavailableError do
  @moduledoc """
  The listener is unreachable, answered `5xx`, returned an unfollowed redirect,
  or returned a non-JSON success body. Safe to retry.
  """

  @type t :: %__MODULE__{reason: term()}

  defexception [:message, :reason, retryable: true]
end

defmodule Ankusa.SDK.InvalidMessageError do
  @moduledoc """
  The bytes aren't a valid v1 `Ankusa.Sink.Message` payload. Always
  `retryable: false` — the same bytes will fail the same way.

  `:code` is a stable string (`"invalid_json"`, `"not_an_object"`,
  `"unsupported_version"`, `"invalid_field"`, `"ambiguous_body"`,
  `"missing_body"`, `"invalid_body_base64"`, `"size_mismatch"`, `"integrity"`,
  `"tenant_mismatch"`); `:field` names the offending key when `:code` is
  `"invalid_field"`, else `nil`. `:reason` carries the same as an Elixir term.
  """

  @type t :: %__MODULE__{reason: term(), code: String.t(), field: String.t() | nil}

  defexception [:message, :reason, :code, :field, retryable: false]
end

defmodule Ankusa.SDK.SourceNotFoundError do
  @moduledoc "`404`: no such source for this tenant."

  @type t :: %__MODULE__{status: non_neg_integer() | nil, body: term()}

  defexception [:message, :status, :body]
end

defmodule Ankusa.SDK.SourceConflictError do
  @moduledoc """
  `409`: a source with that name already exists (`source_exists`), or a delete
  found the source still has undelivered hooks (`source_has_deliveries`); the
  body's `"error"` says which.
  """

  @type t :: %__MODULE__{status: non_neg_integer() | nil, body: term()}

  defexception [:message, :status, :body]
end

defmodule Ankusa.SDK.SourceStoreReadOnlyError do
  @moduledoc """
  `409 source_store_read_only`: the deployment's source store is a static seed,
  so writes are impossible.
  """

  @type t :: %__MODULE__{status: non_neg_integer() | nil, body: term()}

  defexception [:message, :status, :body]
end

defmodule Ankusa.SDK.SourceInvalidError do
  @moduledoc """
  `400`: bad tenant, bad source name, or a spec the server rejected.

  `:message` is the server's own `message` (for a bad spec) or the `error` code
  (for a bad tenant/name, which has no message).
  """

  @type t :: %__MODULE__{status: non_neg_integer() | nil, body: term()}

  defexception [:message, :status, :body]
end

defmodule Ankusa.SDK.SourcesUnavailableError do
  @moduledoc "The admin API is unreachable, timed out, or answered `5xx`."

  @type t :: %__MODULE__{status: non_neg_integer() | nil, body: term()}

  defexception [:message, :status, :body]
end

defmodule Ankusa.SDK.VersionMismatchError do
  @moduledoc "`GET /health` reported a version other than `expected_version`."

  @type t :: %__MODULE__{status: non_neg_integer() | nil, body: term()}

  defexception [:message, :status, :body]
end
