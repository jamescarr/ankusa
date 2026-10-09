(ns ankusa.sdk.conformance-test
  "The language-neutral vectors in `conformance/cases/*.json`, run against the
  public `ankusa.sdk.*` namespaces. One test per case, named by its id; nothing
  is filtered or skipped, and an unknown operation fails."
  (:require [ankusa.sdk.admin :as admin]
            [ankusa.sdk.claim-check :as claim-check]
            [ankusa.sdk.claim-ref :as claim-ref]
            [ankusa.sdk.errors :as errors]
            [ankusa.sdk.idempotency :as idempotency]
            [ankusa.sdk.message :as message]
            [ankusa.sdk.routes :as routes]
            [ankusa.sdk.support.gateway :as gateway]
            [ankusa.sdk.webhook :as webhook]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [is]]
            [clojure.walk :as walk])
  (:import (clojure.lang ExceptionInfo)
           (java.util Base64)))

(set! *warn-on-reflection* true)

(def ^:private unreachable-base-url "http://127.0.0.1:1")
(def ^:private injected-base-url "http://gateway.invalid")

;; ---------------------------------------------------------------------------
;; Loading
;; ---------------------------------------------------------------------------

(defn- assert-params-order!
  "A list operation's `params` must keep its input order, which a Clojure map
  only does up to 8 keys (it is an array map until then)."
  [c]
  (let [params (get-in c ["input" "params"])]
    (when (and (map? params) (> (count params) 8))
      (throw (ex-info "params order cannot be preserved" {:case (get c "id")})))))

(defn- load-cases
  []
  (let [dir (io/file "../../conformance/cases")
        files (sort-by #(.getName ^java.io.File %)
                       (filter #(str/ends-with? (.getName ^java.io.File %) ".json")
                               (.listFiles dir)))]
    (into []
          (comp (mapcat #(get (json/read-str (slurp %)) "cases"))
                (map (fn [c] (assert-params-order! c) c)))
          files)))

;; ---------------------------------------------------------------------------
;; Running
;; ---------------------------------------------------------------------------

(defn- base64
  [^bytes bytes]
  (.encodeToString (Base64/getEncoder) bytes))

(defn- open-gateway
  "`{:base-url :transport :close}` for the case's gateway and client."
  [gateway-spec client-spec rec]
  (cond
    (or (nil? gateway-spec) (get gateway-spec "unreachable"))
    {:base-url unreachable-base-url :close (fn [])}

    (= "injected" (get client-spec "transport"))
    {:base-url injected-base-url
     :transport (gateway/injected-transport gateway-spec rec)
     :close (fn [])}

    :else (gateway/start! gateway-spec rec)))

(defn- invoke
  "Call the SDK operation `op` and return what it returned."
  [op input base-url opts]
  (case op
    "parse_claim_ref" (claim-ref/parse (get input "ref"))
    "parse_headers" (webhook/parse-headers (get input "headers"))
    "decode_message" (message/decode (get input "message"))
    "idempotency_key" (let [m (if (contains? input "message")
                                (message/decode (get input "message"))
                                (webhook/parse-headers (get input "headers")))]
                        (idempotency/key m (when (true? (get input "include_replay"))
                                             {:include-replay true})))
    "redeem" (claim-check/redeem (claim-check/client base-url opts)
                                 (get input "ref")
                                 (get input "sha256"))
    "health" (claim-check/health (claim-check/client base-url opts))
    "routes_health" (routes/health (routes/client base-url opts))
    "routes_list" (routes/list-routes (routes/client base-url opts) (get input "params"))
    "routes_create" (routes/create-route (routes/client base-url opts) (get input "input"))
    "routes_get" (routes/get-route (routes/client base-url opts) (get input "id"))
    "routes_replace" (routes/replace-route (routes/client base-url opts)
                                           (get input "id")
                                           (get input "input"))
    "routes_update" (routes/update-route (routes/client base-url opts)
                                         (get input "id")
                                         (get input "patch"))
    "routes_delete" (routes/delete-route (routes/client base-url opts) (get input "id"))
    "routes_ip_rules_get" (routes/get-ip-rules (routes/client base-url opts))
    "routes_ip_rules_put" (routes/put-ip-rules (routes/client base-url opts) (get input "rules"))
    "routes_test" (routes/test-route (routes/client base-url opts) (get input "request"))
    "admin_health" (admin/health (admin/client base-url opts))
    "admin_metrics" (admin/metrics (admin/client base-url opts))
    "admin_config" (admin/config (admin/client base-url opts))
    "admin_dlq_list" (admin/list-dead-letters (admin/client base-url opts) (get input "params"))
    "admin_quarantine" (admin/list-quarantined (admin/client base-url opts) (get input "params"))
    "admin_replay_create" (admin/create-replay (admin/client base-url opts) (get input "spec"))
    "admin_replay_get" (admin/get-replay (admin/client base-url opts) (get input "id"))
    "admin_replay_list" (admin/list-replays (admin/client base-url opts))
    "admin_replay_update" (admin/update-replay (admin/client base-url opts)
                                               (get input "id")
                                               (get input "patch"))
    (throw (ex-info "unknown conformance operation" {:operation op}))))

(defn- normalize
  "`result` as the string-keyed snake_case JSON the vectors expect."
  [op result]
  (case op
    "parse_claim_ref" {"tenant_id" (:tenant-id result)
                       "claim_id" (:claim-id result)
                       "path" (:path result)}
    "parse_headers" {"id" (:id result)
                     "source" (:source result)
                     "tenant" (:tenant result)
                     "content_type" (:content-type result)
                     "dedupe_key" (:dedupe-key result)
                     "replay_id" (:replay-id result)
                     "idempotency_key" (:idempotency-key result)}
    "decode_message" {"v" (:v result)
                      "id" (:id result)
                      "source_id" (:source-id result)
                      "tenant_id" (:tenant-id result)
                      "received_at" (:received-at result)
                      "content_type" (:content-type result)
                      "size" (:size result)
                      "body_base64" (some-> ^bytes (:body result) base64)
                      "claim" (:claim result)
                      "sha256" (:sha256 result)
                      "dedupe_key" (:dedupe-key result)
                      "replay_id" (:replay-id result)
                      "idempotency_key" (:idempotency-key result)
                      "headers" (:headers result)}
    "idempotency_key" {"key" result}
    "redeem" {"body" {"base64" (base64 result)}}
    "admin_metrics" {"text" result}
    "routes_delete" nil
    (walk/stringify-keys result)))

(defn- expected-ok
  "The vector's `ok` value, with a `Body` turned into base64 as `normalize`
  does for the bytes the SDK returns."
  [op ok]
  (if (= "redeem" op)
    {"body" {"base64" (base64 (gateway/body-bytes (get ok "body")))}}
    ok))

(defn- check-error!
  [expected e]
  (is (= (get expected "class") (name (errors/error-type e)))
      (str "error class; got " (ex-message e)))
  (let [data (ex-data e)]
    (doseq [[k v] (dissoc expected "class")
            :let [data-key (keyword (str/replace k "_" "-"))]]
      (is (contains? data data-key) (str "error has " data-key))
      (is (= v (walk/stringify-keys (get data data-key))) (str "error " data-key)))))

(defn- check-requests!
  [expected recorded]
  (is (= (count expected) (count recorded)) "request count")
  (doseq [[want got] (map vector expected recorded)]
    (is (= (get want "method") (get got "method")) "request method")
    (is (= (get want "path") (get got "path")) "request path")
    (doseq [[k v] (get want "headers")]
      (is (= v (get-in got ["headers" k])) (str "request header " k)))
    (when (contains? want "body")
      (is (= (get want "body") (get got "body")) "request body"))))

(defn- run-case
  [c]
  (let [op (get c "operation")
        input (get c "input")
        expect (get c "expect")
        client-spec (get input "client")
        rec (gateway/recorder)
        {:keys [base-url transport close]} (open-gateway (get input "gateway") client-spec rec)]
    (try
      (let [opts (cond-> {:headers (get client-spec "headers")
                          :timeout-ms (or (get client-spec "timeout_ms") 10000)}
                   transport (assoc :transport transport))
            outcome (try
                      {:ok (normalize op (invoke op input base-url opts))}
                      (catch ExceptionInfo e
                        (if (errors/error-type e) {:error e} (throw e))))]
        (if (contains? expect "error")
          (if-let [e (:error outcome)]
            (check-error! (get expect "error") e)
            (is false (str "expected error " (pr-str (get expect "error"))
                           ", got ok " (pr-str (:ok outcome)))))
          (if-let [e (:error outcome)]
            (is false (str "expected ok " (pr-str (get expect "ok"))
                           ", got error " (pr-str (ex-message e) (ex-data e))))
            (is (= (expected-ok op (get expect "ok")) (:ok outcome)))))
        (when-let [requests (get expect "requests")]
          (check-requests! requests @rec)))
      (finally
        (close)))))

;; One test per case, named by its id, registered the way `deftest` does.
(doseq [c (load-cases)]
  (intern *ns* (with-meta (symbol (get c "id")) {:test (fn [] (run-case c))}) nil))
