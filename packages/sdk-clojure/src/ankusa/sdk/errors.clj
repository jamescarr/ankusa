(ns ankusa.sdk.errors
  "Classifying what the SDK throws.

  Every failure the SDK raises for a bad response, an unreachable server, or a
  bad message is an `ex-info` whose `ex-data` has a `:type` keyword in the
  `ankusa.sdk` namespace and the type's own keys, all always present (`nil`
  when they do not apply). Keys are kebab-case.

  | `:type` (in `:ankusa.sdk`) | `:retryable` | other keys |
  | --- | --- | --- |
  | `InvalidClaimRefError` | false | |
  | `ClaimNotFoundError` | false | |
  | `ClaimRejectedError` | false | `:status`, `:body` |
  | `ClaimIntegrityError` | false | |
  | `ClaimCheckUnavailableError` | true | `:status`, `:reason` |
  | `MissingHookIdError` | false | |
  | `InvalidRouteIdError` | false | |
  | `RouteNotFoundError` | false | |
  | `RoutesRejectedError` | false | `:status`, `:code`, `:field`, `:message`, `:conflicting-id`, `:max-routes` |
  | `RoutesUnavailableError` | true | `:status`, `:reason` |
  | `RoleNotEnabledError` | false | `:role` |
  | `AdminRejectedError` | false | `:status`, `:code` |
  | `AdminUnavailableError` | true | `:status`, `:reason` |
  | `InvalidMessageError` | false | `:code`, `:field` |
  | `SourceNotFoundError`, `SourceConflictError`, `SourceStoreReadOnlyError`, `SourceInvalidError`, `VersionMismatchError` | false | `:status`, `:body` |
  | `SourcesUnavailableError` | true | `:status`, `:body` |

  `:reason` is one of `:status` (the server answered a status the client
  cannot use; `:status` holds it), `:transport` (nothing usable came back;
  `:status` is `nil` and the exception that caused it is the `ex-cause`), or
  `:invalid-json` (a `200` whose body was not JSON).

  Caller misuse — a bad base URL, a bad option, a bad header, a non-positive
  timeout, input `decode` cannot read — throws `IllegalArgumentException`
  instead: it is a bug to fix, not a failure to handle.")

(set! *warn-on-reflection* true)

(defn error-type
  "The `:ankusa.sdk/...` keyword naming what `e` is, or `nil` when `e` is not
  an SDK failure."
  [e]
  (let [t (:type (ex-data e))]
    (when (and (keyword? t) (= "ankusa.sdk" (namespace t)))
      t)))

(defn retryable?
  "True when trying the same call again can succeed: the server was
  unreachable or answered a status that is not a rejection of the request."
  [e]
  (true? (:retryable (ex-data e))))
