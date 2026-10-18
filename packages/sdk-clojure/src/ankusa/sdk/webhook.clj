(ns ankusa.sdk.webhook
  "The headers every receiver of Ankusa's HTTP sink needs, parsed off a request.

  The HTTP sink sends these headers; the hook id is the only one that is
  always there:

  | Header | Key | Default |
  | --- | --- | --- |
  | `x-ankusa-id` | `:id` | required: its absence means the request is not a delivery |
  | `x-ankusa-source` | `:source` | `\"\"` |
  | `x-ankusa-tenant` | `:tenant` | `nil` |
  | `content-type` | `:content-type` | `nil` |
  | `x-ankusa-dedupe-key` | `:dedupe-key` | `nil` (also when empty) |
  | `x-ankusa-replay-id` | `:replay-id` | `nil` (also when empty) |
  | `x-ankusa-idempotency-key` | `:idempotency-key` | `nil` (also when empty) |

  The result also carries every request header under `:headers`, lowercased, so
  a receiver can read the provider's own headers the sink forwarded."
  (:require [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.headers :refer [normalize]]))

(set! *warn-on-reflection* true)

(defn- non-empty
  "The header, or `nil` when it is absent or empty, so a consumer never keys on
  `\"\"`."
  [headers n]
  (let [v (get headers n)]
    (when-not (= "" v) v)))

(defn parse-headers
  "Parse the Ankusa headers out of `headers`: a map (Ring's `:headers`) or a
  seq of `[name value]` pairs. Names are strings or keywords and are matched
  case-insensitively; values may be strings, numbers, booleans, or keywords.

  Returns `{:id :source :tenant :content-type :dedupe-key :replay-id
  :idempotency-key :headers}`, or throws `:ankusa.sdk/MissingHookIdError` when
  `x-ankusa-id` is absent or empty."
  [headers]
  (let [h (normalize headers)
        id (get h "x-ankusa-id")]
    (when (or (nil? id) (= "" id))
      (throw (error/error "MissingHookIdError" "missing x-ankusa-id header" {})))
    {:id id
     :source (or (get h "x-ankusa-source") "")
     :tenant (get h "x-ankusa-tenant")
     :content-type (get h "content-type")
     :dedupe-key (non-empty h "x-ankusa-dedupe-key")
     :replay-id (non-empty h "x-ankusa-replay-id")
     :idempotency-key (non-empty h "x-ankusa-idempotency-key")
     :headers h}))
