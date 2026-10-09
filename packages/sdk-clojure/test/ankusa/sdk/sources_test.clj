(ns ankusa.sdk.sources-test
  "The sources client: no conformance vector covers it, so its behaviour is
  pinned here — the spec body, the version latch, error mapping, and input
  validation."
  (:require [ankusa.sdk.errors :as errors]
            [ankusa.sdk.sources :as sources]
            [ankusa.sdk.support.gateway :as gateway]
            [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]])
  (:import (clojure.lang ExceptionInfo)
           (java.net ConnectException)
           (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(def ^:private base-url "http://admin.test")

(def ^:private entry
  {"tenant" "acme"
   "name" "billing"
   "source_id" "acme.billing"
   "ingest_path" "/webhooks/acme.billing"
   "verify" {"type" "hmac" "secret" "[REDACTED]" "signature_header" "X-Sig"}
   "on_verify_failure" "reject"
   "sinks" [{"type" "log"}]})

(def ^:private source
  {:tenant "acme"
   :name "billing"
   :source-id "acme.billing"
   :ingest-path "/webhooks/acme.billing"
   :verify {:type "hmac" :secret "[REDACTED]" :signature_header "X-Sig"}
   :on-verify-failure "reject"
   :sinks [{:type "log"}]})

(def ^:private spec
  {:sinks [{"type" "log"}]
   :verify {"type" "hmac" "secret" "s3cr3t" "signature_header" "X-Sig"}
   :on-verify-failure "reject"})

(def ^:private invalid-ids ["..", "a/b", "a?b=1", "a#b" (apply str (repeat 65 "a")) "" nil 42])

(defn- json-response
  [status value]
  {:status status
   :headers {}
   :body (.getBytes ^String (json/write-str value) StandardCharsets/UTF_8)})

(defn- text-response
  [status text]
  {:status status :headers {} :body (.getBytes ^String text StandardCharsets/UTF_8)})

(defn- client
  "`[client recorder]`, the client answering every request with `(respond record)`."
  ([respond] (client respond nil))
  ([respond opts]
   (let [rec (gateway/recorder)]
     [(sources/client base-url (assoc opts :transport (gateway/recording-transport rec respond)))
      rec])))

(defn- health
  ([record] (health record "0.3.0"))
  ([_record version] (json-response 200 {"status" "ok" "version" version})))

(defn- failure
  "The `ex-info` thrown by calling `f`, or a failed assertion when it did not
  throw."
  [f]
  (try
    (f)
    (is false "expected an error")
    nil
    (catch ExceptionInfo e e)))

(defn- paths
  [rec]
  (mapv #(get % "path") @rec))

(deftest the-spec-body-keeps-every-set-field
  (let [[c rec] (client (fn [_] (json-response 201 entry)))]
    (sources/create-source c "acme" "billing" spec)
    (is (= (assoc {"sinks" [{"type" "log"}]
                   "verify" {"type" "hmac" "secret" "s3cr3t" "signature_header" "X-Sig"}
                   "on_verify_failure" "reject"}
                  "name" "billing")
           (get (first @rec) "body")))))

(deftest the-spec-body-omits-unset-fields
  (let [[c rec] (client (fn [_] (json-response 201 entry)))]
    (sources/create-source c "acme" "billing" {:sinks [{"type" "log"}]})
    (sources/create-source c "acme" "billing" {})
    (is (= [{"sinks" [{"type" "log"}] "name" "billing"}
            {"name" "billing"}]
           (mapv #(get % "body") @rec)))))

(deftest a-version-latch
  (testing "server-version reads the health version"
    (let [[c rec] (client health)]
      (is (= "0.3.0" (sources/server-version c)))
      (is (= [["GET" "/health"]] (mapv (juxt #(get % "method") #(get % "path")) @rec)))))

  (testing "verify-version caches, so later calls make no health request"
    (let [[c rec] (client (fn [record]
                            (if (= "/health" (get record "path"))
                              (health record)
                              (json-response 200 {"tenant" "acme" "entries" [entry]}))))
          verified (sources/verify-version c)]
      (is (= "0.3.0" (sources/server-version verified)))
      (is (= [source] (sources/list-sources verified "acme")))
      (is (= 1 (count (filter #{"/health"} (paths rec)))))))

  (testing "a mismatch fails before the API call"
    (let [[c rec] (client health {:expected-version "9.9.9"})
          e (failure #(sources/list-sources c "acme"))]
      (is (= :ankusa.sdk/VersionMismatchError (errors/error-type e)))
      (is (= {:status nil :body nil} (select-keys (ex-data e) [:status :body])))
      (is (re-find #"9\.9\.9" (ex-message e)))
      (is (re-find #"0\.3\.0" (ex-message e)))
      (is (= ["/health"] (paths rec)))))

  (testing "a matching version passes, after one probe"
    (let [[c rec] (client (fn [record]
                            (if (= "/health" (get record "path"))
                              (health record)
                              (json-response 200 {"tenant" "acme" "entries" [entry]})))
                          {:expected-version "0.3.0"})]
      (is (= "billing" (:name (first (sources/list-sources c "acme")))))
      (is (= 1 (count (filter #{"/health"} (paths rec)))))))

  (testing "without :expected-version no probe is made"
    (let [[c rec] (client (fn [_] (json-response 200 {"tenant" "acme" "entries" []})))]
      (is (= [] (sources/list-sources c "acme")))
      (is (= ["/v1/tenants/acme/sources"] (paths rec)))))

  (testing "a health answer without a version is unavailable"
    (let [[c rec] (client (fn [_] (json-response 200 {"status" "ok"})) {:expected-version "0.3.0"})
          e (failure #(sources/list-sources c "acme"))]
      (is (= :ankusa.sdk/SourcesUnavailableError (errors/error-type e)))
      (is (= 200 (:status (ex-data e))))
      (is (= ["/health"] (paths rec))))))

(deftest list-and-get-parse-sources
  (testing "list-sources parses the entries"
    (let [[c _] (client (fn [_] (json-response 200 {"tenant" "acme" "entries" [entry]})))]
      (is (= [source] (sources/list-sources c "acme")))))

  (testing "a source without verify or sinks gets the defaults"
    (let [[c _] (client (fn [_] (json-response 200 (dissoc entry "verify" "sinks" "on_verify_failure"))))]
      (is (= (assoc source :verify {:type "none"} :sinks [] :on-verify-failure nil)
             (sources/get-source c "acme" "billing")))))

  (testing "a malformed entry is an unavailable error"
    (let [[c _] (client (fn [_] (json-response 200 {"entries" [{"name" "billing"}]})))
          e (failure #(sources/list-sources c "acme"))]
      (is (= :ankusa.sdk/SourcesUnavailableError (errors/error-type e)))
      (is (= "malformed source in response" (ex-message e)))
      (is (= 200 (:status (ex-data e))))))

  (testing "a list body without entries is an unavailable error"
    (let [[c _] (client (fn [_] (json-response 200 {"tenant" "acme"})))]
      (is (= :ankusa.sdk/SourcesUnavailableError
             (errors/error-type (failure #(sources/list-sources c "acme")))))))

  (testing "get-source parses one source"
    (let [[c rec] (client (fn [_] (json-response 200 entry)))]
      (is (= "/webhooks/acme.billing" (:ingest-path (sources/get-source c "acme" "billing"))))
      (is (= ["/v1/tenants/acme/sources/billing"] (paths rec)))))

  (testing "a non-JSON body is unavailable"
    (let [[c _] (client (fn [_] (text-response 200 "<html>")))
          e (failure #(sources/get-source c "acme" "billing"))]
      (is (= :ankusa.sdk/SourcesUnavailableError (errors/error-type e)))
      (is (= "<html>" (:body (ex-data e)))))))

(deftest writes
  (testing "create-source posts the spec plus the name"
    (let [[c rec] (client (fn [_] (json-response 201 entry)))]
      (is (= "acme.billing" (:source-id (sources/create-source c "acme" "billing" spec))))
      (is (= "POST" (get (first @rec) "method")))
      (is (= ["/v1/tenants/acme/sources"] (paths rec)))
      (is (= "billing" (get-in (first @rec) ["body" "name"])))
      (is (= "s3cr3t" (get-in (first @rec) ["body" "verify" "secret"])))))

  (testing "update-source puts the spec without a name"
    (let [[c rec] (client (fn [_] (json-response 200 entry)))]
      (is (= "billing" (:name (sources/update-source c "acme" "billing" spec))))
      (is (= "PUT" (get (first @rec) "method")))
      (is (= ["/v1/tenants/acme/sources/billing"] (paths rec)))
      (is (not (contains? (get (first @rec) "body") "name")))
      (is (= "reject" (get-in (first @rec) ["body" "on_verify_failure"])))))

  (testing "delete-source succeeds with no body"
    (let [[c rec] (client (fn [_] (text-response 204 "")))]
      (is (nil? (sources/delete-source c "acme" "billing")))
      (is (= [{"method" "DELETE" "path" "/v1/tenants/acme/sources/billing" "body" nil}]
             (mapv #(select-keys % ["method" "path" "body"]) @rec))))))

(deftest errors-carry-status-and-body
  (testing "404 is not-found"
    (let [[c _] (client (fn [_] (json-response 404 {"error" "source_not_found"})))
          e (failure #(sources/get-source c "acme" "missing"))]
      (is (= :ankusa.sdk/SourceNotFoundError (errors/error-type e)))
      (is (= {:status 404 :body {:error "source_not_found"}} (select-keys (ex-data e) [:status :body])))
      (is (not (errors/retryable? e)))))

  (testing "409 is a conflict"
    (let [[c _] (client (fn [_] (json-response 409 {"error" "source_exists"})))
          e (failure #(sources/create-source c "acme" "billing" spec))]
      (is (= :ankusa.sdk/SourceConflictError (errors/error-type e)))
      (is (= {:status 409 :body {:error "source_exists"}} (select-keys (ex-data e) [:status :body])))))

  (testing "409 source_store_read_only is its own error"
    (let [[c _] (client (fn [_] (json-response 409 {"error" "source_store_read_only"})))
          e (failure #(sources/update-source c "acme" "billing" spec))]
      (is (= :ankusa.sdk/SourceStoreReadOnlyError (errors/error-type e)))
      (is (= 409 (:status (ex-data e))))))

  (testing "400 carries the server's message"
    (let [[c _] (client (fn [_] (json-response 400 {"error" "invalid_source"
                                                    "message" "sinks must not be empty"})))
          e (failure #(sources/create-source c "acme" "billing" spec))]
      (is (= :ankusa.sdk/SourceInvalidError (errors/error-type e)))
      (is (= 400 (:status (ex-data e))))
      (is (= "sinks must not be empty" (ex-message e)))))

  (testing "400 without a message carries the error code"
    (let [[c _] (client (fn [_] (json-response 400 {"error" "invalid_tenant"})))
          e (failure #(sources/list-sources c "acme"))]
      (is (= "invalid_tenant" (ex-message e)))))

  (testing "400 with neither says so"
    (let [[c _] (client (fn [_] (text-response 400 "nope")))
          e (failure #(sources/list-sources c "acme"))]
      (is (= "invalid source (400): \"nope\"" (ex-message e)))))

  (testing "5xx is unavailable and retryable"
    (let [[c _] (client (fn [_] (json-response 503 {"error" "boom"})))
          e (failure #(sources/list-sources c "acme"))]
      (is (= :ankusa.sdk/SourcesUnavailableError (errors/error-type e)))
      (is (= {:status 503 :body {:error "boom"}} (select-keys (ex-data e) [:status :body])))
      (is (errors/retryable? e))))

  (testing "an unreachable server is unavailable with no status or body"
    (let [[c _] (client (fn [_] (throw (ConnectException. "refused"))))
          e (failure #(sources/get-source c "acme" "billing"))]
      (is (= :ankusa.sdk/SourcesUnavailableError (errors/error-type e)))
      (is (= {:status nil :body nil} (select-keys (ex-data e) [:status :body])))
      (is (instance? ConnectException (ex-cause e))))))

(deftest input-is-validated-before-any-request
  (testing "every call refuses an invalid tenant"
    (doseq [value invalid-ids]
      (let [[c rec] (client (fn [_] (json-response 200 {})))]
        (doseq [call [#(sources/list-sources c value)
                      #(sources/get-source c value "billing")
                      #(sources/create-source c value "billing" spec)
                      #(sources/update-source c value "billing" spec)
                      #(sources/delete-source c value "billing")]
                :let [e (failure call)]]
          (is (= :ankusa.sdk/SourceInvalidError (errors/error-type e)))
          (is (= {:status nil :body nil} (select-keys (ex-data e) [:status :body])))
          (is (= (str "invalid tenant: " (pr-str value)) (ex-message e))))
        (is (= [] @rec) (pr-str value)))))

  (testing "a source call refuses an invalid name"
    (doseq [value invalid-ids]
      (let [[c rec] (client (fn [_] (json-response 200 {})))]
        (doseq [call [#(sources/get-source c "acme" value)
                      #(sources/create-source c "acme" value spec)
                      #(sources/update-source c "acme" value spec)
                      #(sources/delete-source c "acme" value)]
                :let [e (failure call)]]
          (is (= :ankusa.sdk/SourceInvalidError (errors/error-type e)))
          (is (= (str "invalid source name: " (pr-str value)) (ex-message e))))
        (is (= [] @rec) (pr-str value)))))

  (testing "the version latch does not probe for invalid input"
    (let [[c rec] (client health {:expected-version "0.3.0"})]
      (failure #(sources/get-source c ".." "billing"))
      (is (= [] @rec))))

  (testing "dashes and underscores travel as the path"
    (let [[c rec] (client (fn [record]
                            (if (= "DELETE" (get record "method"))
                              (text-response 204 "")
                              (json-response 200 {"tenant" "acme-corp" "entries" []}))))]
      (is (= [] (sources/list-sources c "acme-corp")))
      (is (nil? (sources/delete-source c "acme-corp" "my_source-1")))
      (is (= ["/v1/tenants/acme-corp/sources" "/v1/tenants/acme-corp/sources/my_source-1"]
             (paths rec))))))

(deftest the-expected-version-must-be-a-string
  (is (thrown? IllegalArgumentException
               (sources/client base-url {:expected-version 3})))
  (is (some? (sources/client base-url {:expected-version nil}))))
