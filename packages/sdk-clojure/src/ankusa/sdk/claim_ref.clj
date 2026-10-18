(ns ankusa.sdk.claim-ref
  "A claim-check ref: `urn:ankusa:claim:v1:<tenant>:<claim_id>`, or the signed,
  expiring `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`; the
  string a queue message carries in its `\"claim\"` field once its body outgrows
  the sink's inline limit.

  The claim id is a Crockford base32 ULID: 26 characters, no `I`, `L`, `O` or
  `U`, and a first character in `0`..`7` so the timestamp stays in range.
  `exp_unix` is a decimal of 1-12 digits with no leading zero and `signature`
  43 unpadded base64url characters. The gateway checks the signature and the
  expiry; the SDK only carries them in the path."
  (:require [ankusa.sdk.impl.error :as error]))

(set! *warn-on-reflection* true)

(def ^:private pattern
  #"urn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})")

(def ^:private pattern-v2
  #"urn:ankusa:claim:v2:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25}):([1-9][0-9]{0,11}):([A-Za-z0-9_-]{43})")

(defn parse
  "Parse `ref` into `{:tenant-id :claim-id :path}`, where `:path` is the
  gateway path `/v1/claims/<tenant>/<claim_id>`, followed by
  `?exp=<exp_unix>&sig=<signature>` for a v2 ref. Anything that is not a string
  in that form throws an `:ankusa.sdk/InvalidClaimRefError`."
  [ref]
  (if-let [[_ tenant-id claim-id exp sig]
           (when (string? ref)
             (or (re-matches pattern-v2 ref) (re-matches pattern ref)))]
    {:tenant-id tenant-id
     :claim-id claim-id
     :path (str "/v1/claims/" tenant-id "/" claim-id
                (when sig (str "?exp=" exp "&sig=" sig)))}
    (throw (error/error "InvalidClaimRefError" (str "invalid claim ref: " (pr-str ref)) {}))))
