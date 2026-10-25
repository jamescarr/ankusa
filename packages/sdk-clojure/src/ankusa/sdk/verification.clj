(ns ankusa.sdk.verification
  "How the node verified a hook at ingest, and whether to trust it.

  Every delivery carries `:verification`: `{:status \"ok\" | \"failed\" |
  \"skipped\", :flagged boolean}`, or `nil` from a node older than the field. A
  source with `on_verify_failure: accept_flag` commits a hook whose signature
  check failed, with `:flagged true`; nothing about that hook proves the
  provider sent it, and neither does the tenant it names.

  Accepts a decoded message (`ankusa.sdk.message/decode`), a hook
  (`ankusa.sdk.message/->hook`), parsed headers
  (`ankusa.sdk.webhook/parse-headers`) or a received delivery
  (`ankusa.sdk.webhook/receive-delivery`).")

(set! *warn-on-reflection* true)

(defn trusted?
  "Whether the node verified `m` at ingest and nothing flags it: `true` only for
  `{:status \"ok\" :flagged false}`. A flagged hook (accepted despite a failed
  check), a `\"failed\"` or `\"skipped\"` one, any other status, and a hook with
  no `:verification` (a node older than the field) are untrusted, so the answer
  fails closed."
  [m]
  (let [{:keys [status flagged]} (:verification m)]
    (and (= "ok" status) (false? flagged))))
