(ns ankusa.sdk.claim-ref
  "A claim-check ref: `urn:ankusa:claim:v1:<tenant>:<claim_id>`, the string a
  queue message carries in its `\"claim\"` field once its body outgrows the
  sink's inline limit.

  The claim id is a Crockford base32 ULID: 26 characters, no `I`, `L`, `O` or
  `U`, and a first character in `0`..`7` so the timestamp stays in range."
  (:require [ankusa.sdk.impl.error :as error]))

(set! *warn-on-reflection* true)

(def ^:private pattern
  #"urn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})")

(defn parse
  "Parse `ref` into `{:tenant-id :claim-id :path}`, where `:path` is the
  gateway path `/v1/claims/<tenant>/<claim_id>`. Anything that is not a string
  in that form throws an `:ankusa.sdk/InvalidClaimRefError`."
  [ref]
  (if-let [[_ tenant-id claim-id] (when (string? ref) (re-matches pattern ref))]
    {:tenant-id tenant-id
     :claim-id claim-id
     :path (str "/v1/claims/" tenant-id "/" claim-id)}
    (throw (error/error "InvalidClaimRefError" (str "invalid claim ref: " (pr-str ref)) {}))))
