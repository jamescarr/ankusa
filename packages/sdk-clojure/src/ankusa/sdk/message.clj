(ns ankusa.sdk.message
  "Decode and verify a v1 queue message: the JSON payload Ankusa's broker sinks
  (RabbitMQ, Kafka, NATS) publish.

  ```clojure
  (let [message (message/decode payload)
        hook (message/->hook message gateway)]
    (handle hook (idempotency/key hook)))
  ```

  A message carries its body inline (`body_base64`) or by reference (`claim`,
  redeemed from the claim-check gateway). `decode` never touches the network:
  an inline body is decoded and verified, a claim is only parsed.

  Every failure is an `:ankusa.sdk/InvalidMessageError` with a stable `:code`
  (`\"invalid_json\"`, `\"not_an_object\"`, `\"unsupported_version\"`,
  `\"invalid_field\"`, `\"ambiguous_body\"`, `\"missing_body\"`,
  `\"invalid_body_base64\"`, `\"size_mismatch\"`, `\"integrity\"`,
  `\"tenant_mismatch\"`) and, for `\"invalid_field\"`, the offending `:field`
  (otherwise `nil`). It is never retryable: the same bytes fail the same way."
  (:require [ankusa.sdk.claim-check :as claim-check]
            [ankusa.sdk.claim-ref :as claim-ref]
            [ankusa.sdk.impl.digest :as digest]
            [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.http :as http])
  (:import (ankusa.sdk.impl.http Client)
           (java.util Base64)))

(set! *warn-on-reflection* true)

(defn- fail
  [code field]
  (throw (error/error "InvalidMessageError"
                      (str "invalid message: " code (when field (str " (" field ")")))
                      {:code code :field field})))

(defn- check-field!
  [ok? field]
  (when-not ok?
    (fail "invalid_field" field)))

(defn- optional-string!
  [raw field]
  (let [v (get raw field)]
    (check-field! (or (nil? v) (string? v)) field)
    v))

(def ^:private sha256-pattern #"[0-9a-f]{64}")

(defn- parse-json
  "The message text as a string-keyed map."
  [data]
  (let [text (cond (string? data) data
                   (bytes? data) (http/utf8-string data)
                   :else (throw (IllegalArgumentException.
                                 (str "message must be a string or byte array, got: "
                                      (pr-str (class data))))))
        raw (if text (http/parse-json text identity) ::http/invalid)]
    (when (http/invalid? raw)
      (fail "invalid_json" nil))
    (when-not (map? raw)
      (fail "not_an_object" nil))
    raw))

(defn- base64-decode
  "The bytes of standard padded base64 `s`, or `nil` when it is not."
  [^String s]
  (when (zero? (mod (count s) 4))
    (try
      (.decode (Base64/getDecoder) s)
      (catch IllegalArgumentException _ nil))))

(defn- claim-tenant
  "The tenant of claim ref `claim`, or a failure naming the `claim` field when
  it is not a string or not a ref."
  [claim]
  (try
    (:tenant-id (claim-ref/parse claim))
    (catch clojure.lang.ExceptionInfo _
      (fail "invalid_field" "claim"))))

(defn- body-fields
  "`{:body :claim :claim-tenant}` for the one body form `raw` carries."
  [raw]
  (let [inline? (contains? raw "body_base64")
        claim? (contains? raw "claim")]
    (cond
      (and inline? claim?) (fail "ambiguous_body" nil)
      (not (or inline? claim?)) (fail "missing_body" nil)
      inline? (let [encoded (get raw "body_base64")
                    body (when (string? encoded) (base64-decode encoded))]
                (when-not body
                  (fail "invalid_body_base64" nil))
                {:body body})
      :else (let [claim (get raw "claim")
                  tenant (claim-tenant claim)]
              (check-field! (string? (get raw "sha256")) "sha256")
              {:claim claim :claim-tenant tenant}))))

(defn decode
  "Decode and verify a v1 message from `data`, a string or a `byte[]` of UTF-8
  JSON. Returns

  `{:v :id :source-id :tenant-id :received-at :content-type :size :body :claim
  :sha256 :dedupe-key :replay-id :idempotency-key :headers}`

  where `:body` is a `byte[]` for an inline message and `nil` for a claim, and
  `:claim` the claim ref for a claim and `nil` otherwise. `:headers` keeps its
  string keys. Unknown keys are ignored.

  The rules run in order and the first failure wins: valid JSON, an object,
  `v` = 1, the typed fields, the body form, then (inline) size and digest or
  (claim) tenant. Anything but a string or `byte[]` throws
  `IllegalArgumentException`."
  [data]
  (let [raw (parse-json data)
        v (get raw "v")
        _ (when-not (and (integer? v) (== v 1))
            (fail "unsupported_version" nil))
        id (get raw "id")
        _ (check-field! (and (string? id) (not= "" id)) "id")
        source-id (get raw "source_id")
        _ (check-field! (string? source-id) "source_id")
        received-at (get raw "received_at")
        _ (check-field! (integer? received-at) "received_at")
        size (get raw "size")
        _ (check-field! (and (integer? size) (>= size 0)) "size")
        tenant-id (optional-string! raw "tenant_id")
        content-type (optional-string! raw "content_type")
        dedupe-key (optional-string! raw "dedupe_key")
        replay-id (optional-string! raw "replay_id")
        idempotency-key (optional-string! raw "idempotency_key")
        headers (if (contains? raw "headers") (get raw "headers") {})
        _ (check-field! (and (map? headers) (every? string? (vals headers))) "headers")
        sha256 (get raw "sha256")
        _ (check-field! (or (nil? sha256) (and (string? sha256) (re-matches sha256-pattern sha256)))
                        "sha256")
        {:keys [body claim claim-tenant]} (body-fields raw)]
    (if body
      (do (when (not= (alength ^bytes body) size)
            (fail "size_mismatch" nil))
          (when (and sha256 (not= sha256 (digest/sha256-hex body)))
            (fail "integrity" nil)))
      (when (and tenant-id (not= tenant-id claim-tenant))
        (fail "tenant_mismatch" nil)))
    {:v 1
     :id id
     :source-id source-id
     :tenant-id tenant-id
     :received-at received-at
     :content-type content-type
     :size size
     :body body
     :claim claim
     :sha256 sha256
     :dedupe-key dedupe-key
     :replay-id replay-id
     :idempotency-key idempotency-key
     :headers headers}))

(defn ->hook
  "Turn a decoded `message` into the hook a handler takes:

  `{:id :source-id :tenant-id :content-type :body :received-at :size :dedupe-key
  :replay-id :idempotency-key :headers}`

  `claim-check-client` (from `ankusa.sdk.claim-check/client`) is required even
  for an inline message, so a consumer cannot forget to supply one and then fail
  the day a payload outgrows the sink's inline limit. An inline message makes no
  request; a claim is redeemed, and a redemption failure propagates unchanged:
  its `:retryable` is the ack-or-requeue decision.

  Delivery is at-least-once, so the handler still dedupes on
  `ankusa.sdk.idempotency/key`."
  [message claim-check-client]
  (when-not (instance? Client claim-check-client)
    (throw (IllegalArgumentException.
            "a claim-check client from ankusa.sdk.claim-check/client is required")))
  (let [body (or (:body message)
                 (claim-check/redeem claim-check-client (:claim message) (:sha256 message)))]
    {:id (:id message)
     :source-id (:source-id message)
     :tenant-id (:tenant-id message)
     :content-type (:content-type message)
     :body body
     :received-at (:received-at message)
     :size (:size message)
     :dedupe-key (:dedupe-key message)
     :replay-id (:replay-id message)
     :idempotency-key (:idempotency-key message)
     :headers (:headers message)}))
