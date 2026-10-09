(ns ankusa.sdk.impl.error
  "Builds the `ex-info` every SDK failure is raised as.

  The `:type` of its `ex-data` names the failure (`:ankusa.sdk/ClaimNotFoundError`)
  and the rest of the data is that type's fixed key set: every key is present,
  `nil` when it does not apply, so callers can read any of them without a
  `contains?` guard. See `ankusa.sdk.errors`.")

(set! *warn-on-reflection* true)

(def ^:private shapes
  "Every error type with the keys its ex-data always carries."
  (let [rejected {:retryable false :status nil :body nil}]
    {"InvalidClaimRefError" {:retryable false}
     "ClaimNotFoundError" {:retryable false}
     "ClaimRejectedError" {:retryable false :status nil :body nil}
     "ClaimIntegrityError" {:retryable false}
     "ClaimCheckUnavailableError" {:retryable true :status nil :reason nil}
     "MissingHookIdError" {:retryable false}
     "InvalidRouteIdError" {:retryable false}
     "RouteNotFoundError" {:retryable false}
     "RoutesRejectedError" {:retryable false :status nil :code nil :field nil
                            :message nil :conflicting-id nil :max-routes nil}
     "RoutesUnavailableError" {:retryable true :status nil :reason nil}
     "RoleNotEnabledError" {:retryable false :role nil}
     "AdminRejectedError" {:retryable false :status nil :code nil}
     "AdminUnavailableError" {:retryable true :status nil :reason nil}
     "InvalidMessageError" {:retryable false :code nil :field nil}
     "SourceNotFoundError" rejected
     "SourceConflictError" rejected
     "SourceStoreReadOnlyError" rejected
     "SourceInvalidError" rejected
     "VersionMismatchError" rejected
     "SourcesUnavailableError" {:retryable true :status nil :body nil}}))

(defn error
  "An `ex-info` of type `:ankusa.sdk/<type-name>` with `message`. `data` holds
  the type's own keys; any it leaves out are `nil` (and `:retryable` is the
  type's fixed value). `cause` is the exception that caused it, if any."
  ([type-name message data]
   (error type-name message data nil))
  ([type-name message data cause]
   (let [shape (or (get shapes type-name)
                   (throw (IllegalArgumentException.
                           (str "unknown error type " (pr-str type-name)))))]
     (ex-info message
              (merge shape data {:type (keyword "ankusa.sdk" type-name)})
              cause))))
