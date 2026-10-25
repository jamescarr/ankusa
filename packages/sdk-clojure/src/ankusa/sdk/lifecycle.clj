(ns ankusa.sdk.lifecycle
  "Decode a lifecycle event: the CloudEvents 1.0 (structured mode) event an
  Ankusa instance publishes when a source or a route is created, updated or
  deleted, when a tenant is erased, or when the instance is restored from a
  backup.

  It arrives as the body of a queue message from the instance's lifecycle
  sinks (content type `application/cloudevents+json`, `source_id`
  `ankusa:lifecycle`): decode the message with `ankusa.sdk.message/decode`,
  take the body from `ankusa.sdk.message/->hook`, and hand it to `decode`.

  ```clojure
  (let [event (lifecycle/decode (:body hook))]
    (:type event))   ;=> \"io.ankusa.source.created\"
  ```

  Delivery is at least once and in commit order. `:id` is derived from the
  change, so the same event always has the same id, and `:revision` (per
  `:subject`) lets a consumer ignore a stale update.

  Every failure is an `:ankusa.sdk/InvalidMessageError` with a stable `:code`
  (`\"invalid_json\"`, `\"not_an_object\"`, `\"unsupported_version\"` when the
  event's `specversion` is not `\"1.0\"`, or `\"invalid_field\"` and the
  offending `:field`). It is never retryable."
  (:require [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.http :as http]))

(set! *warn-on-reflection* true)

(defn- fail
  [code field]
  (throw (error/error "InvalidMessageError"
                      (str "invalid lifecycle event: " code (when field (str " (" field ")")))
                      {:code code :field field})))

(defn- check-field!
  [ok? field]
  (when-not ok?
    (fail "invalid_field" field)))

(defn- required-string!
  [raw field]
  (let [v (get raw field)]
    (check-field! (and (string? v) (not= "" v)) field)
    v))

(defn- optional-string!
  [raw field]
  (let [v (get raw field)]
    (check-field! (or (nil? v) (string? v)) field)
    v))

(defn- parse-json
  "The event text as a string-keyed map."
  [data]
  (let [text (cond (string? data) data
                   (bytes? data) (http/utf8-string data)
                   :else (throw (IllegalArgumentException.
                                 (str "event must be a string or byte array, got: "
                                      (pr-str (class data))))))
        raw (if text (http/parse-json text identity) ::http/invalid)]
    (when (http/invalid? raw)
      (fail "invalid_json" nil))
    (when-not (map? raw)
      (fail "not_an_object" nil))
    raw))

(defn decode
  "Decode and verify one lifecycle event from `data`, a string or a `byte[]` of
  UTF-8 JSON. Returns

  `{:specversion :id :source :type :subject :time :datacontenttype :data
  :revision :actor :instance :sequence :replayid :traceparent :tracestate}`

  where `:data` is the entity as the admin API shows it (string keys, secrets
  redacted) or `nil`, `:sequence` is the instance's audit sequence number as the
  decimal string it is on the wire, and every optional field the event omitted
  or said `null` is `nil`. Unknown keys and event types are ignored, so a
  consumer keeps working when a newer node announces more.

  The rules run in order and the first failure wins: valid JSON, an object,
  `specversion` 1.0, then the typed fields (`id`, `source`, `type`, `subject`,
  `time`, `datacontenttype`, `data`, `revision`, `actor`, `instance`,
  `sequence`, `replayid`, `traceparent`, `tracestate`). Anything but a string
  or `byte[]` throws `IllegalArgumentException`."
  [data]
  (let [raw (parse-json data)
        _ (when-not (= "1.0" (get raw "specversion"))
            (fail "unsupported_version" nil))
        id (required-string! raw "id")
        source (required-string! raw "source")
        type (required-string! raw "type")
        subject (required-string! raw "subject")
        time (required-string! raw "time")
        datacontenttype (optional-string! raw "datacontenttype")
        body (get raw "data")
        _ (check-field! (or (nil? body) (map? body)) "data")
        revision (get raw "revision")
        _ (check-field! (or (nil? revision) (and (integer? revision) (>= revision 1))) "revision")
        actor (optional-string! raw "actor")
        instance (optional-string! raw "instance")
        sequence (get raw "sequence")
        _ (check-field! (or (nil? sequence) (and (string? sequence) (re-matches #"[0-9]+" sequence)))
                        "sequence")
        replayid (optional-string! raw "replayid")
        traceparent (optional-string! raw "traceparent")
        tracestate (optional-string! raw "tracestate")]
    {:specversion "1.0"
     :id id
     :source source
     :type type
     :subject subject
     :time time
     :datacontenttype datacontenttype
     :data body
     :revision revision
     :actor actor
     :instance instance
     :sequence sequence
     :replayid replayid
     :traceparent traceparent
     :tracestate tracestate}))
