(ns ankusa.sdk.idempotency
  "The idempotency key for one delivery, whichever transport delivered it.

  Delivery is at-least-once: a provider retry, a sink retry, or a requeued
  message can hand the same hook over more than once. Ankusa computes one
  tenant-scoped key per hook and ships it (the message's `idempotency_key`, the
  `x-ankusa-idempotency-key` header); this namespace returns that value. Read
  it, do not rebuild it.

  For a message or delivery from a node that predates the field, the key is
  computed from the hook's own fields, by the same rule core uses:

      tenant:source:dedupe-key   when the dedupe key is non-empty
      id                         otherwise

  where the tenant is `\"default\"` when there is none. A replay of an older
  delivery carries a replay id. The key ignores it by default, so a consumer that
  already processed the original drops the replay; one that must reprocess
  replays passes `{:include-replay true}`, which appends `#replay:<replay-id>`.

  Accepts a decoded message (`ankusa.sdk.message/decode`), a hook
  (`ankusa.sdk.message/->hook`), or parsed headers
  (`ankusa.sdk.webhook/parse-headers`)."
  (:refer-clojure :exclude [key]))

(set! *warn-on-reflection* true)

(defn- non-empty-string?
  [v]
  (and (string? v) (not= "" v)))

(defn key
  "The idempotency key for `m`. With `{:include-replay true}` a delivery that
  carries a replay id gets `#replay:<replay-id>` appended."
  ([m] (key m nil))
  ([m {:keys [include-replay]}]
   (let [hook? (contains? m :source-id)
         tenant (if hook? (:tenant-id m) (:tenant m))
         source (if hook? (:source-id m) (:source m))
         shipped (:idempotency-key m)
         dedupe-key (:dedupe-key m)
         replay-id (:replay-id m)
         base (cond
                (non-empty-string? shipped) shipped
                (non-empty-string? dedupe-key) (str (or tenant "default") ":" source ":" dedupe-key)
                :else (:id m))]
     (if (and include-replay (some? replay-id))
       (str base "#replay:" replay-id)
       base))))
