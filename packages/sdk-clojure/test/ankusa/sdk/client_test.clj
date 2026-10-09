(ns ankusa.sdk.client-test
  "What every client constructor and the transport promise outside the vectors:
  bad input is refused, credentials never print, and a stalled body is bounded
  by the timeout."
  (:require [ankusa.sdk.admin :as admin]
            [ankusa.sdk.claim-check :as claim-check]
            [ankusa.sdk.errors :as errors]
            [ankusa.sdk.routes :as routes]
            [ankusa.sdk.sources :as sources]
            [ankusa.sdk.support.gateway :as gateway]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]])
  (:import (clojure.lang ExceptionInfo)
           (java.net InetAddress ServerSocket)
           (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(def ^:private constructors
  {"claim-check" claim-check/client
   "routes" routes/client
   "admin" admin/client
   "sources" sources/client})

(deftest every-constructor-rejects-a-bad-base-url
  (doseq [[what construct] constructors
          url ["ftp://x" "/relative" "http://" nil 42]]
    (is (thrown? IllegalArgumentException (construct url))
        (str what " accepted " (pr-str url)))))

(deftest every-constructor-rejects-bad-options
  (doseq [[what construct] constructors
          opts [{:headers {"host" "example.com"}}
                {:headers {"authorization" "a\nb"}}
                {:headers {"bad name" "x"}}
                {:headers {"x-n" 1}}
                {:timeout-ms 0}
                {:timeout-ms -5}
                {:timeout-ms 1.5}
                {:transport "not a function"}
                {:retries 3}]]
    (is (thrown? IllegalArgumentException (construct "http://x" opts))
        (str what " accepted " (pr-str opts)))))

(deftest a-header-value-never-appears-in-the-error
  (let [e (try (claim-check/client "http://x" {:headers {"authorization" "s3cret\nmore"}})
               (catch IllegalArgumentException e e))]
    (is (instance? IllegalArgumentException e))
    (is (not (str/includes? (ex-message e) "s3cret")))))

(deftest a-client-does-not-print-its-credentials
  (doseq [[what construct] constructors
          :let [c (construct "http://x" {:headers {"authorization" "Bearer s3cret"}})]]
    (is (not (str/includes? (pr-str c) "s3cret")) what)
    (is (not (str/includes? (with-out-str (pprint/pprint c)) "s3cret")) what)
    (is (not (str/includes? (str c) "s3cret")) what)
    (is (str/includes? (pr-str c) "authorization") "the header name stays visible")))

(deftest a-trailing-slash-does-not-double-the-separator
  (doseq [base ["http://gateway.invalid/" "http://gateway.invalid///"]]
    (let [rec (gateway/recorder)
          c (claim-check/client base {:transport (gateway/injected-transport
                                                  {"status" 200
                                                   "body" {"json" {"status" "ok"}}}
                                                  rec)})]
      (claim-check/health c)
      (is (= ["/health"] (mapv #(get % "path") @rec))))))

(deftest header-names-are-lowercased-and-a-json-body-owns-its-content-type
  (let [rec (gateway/recorder)
        c (routes/client "http://gateway.invalid"
                         {:headers {:X-Trace "t1" "Content-Type" "text/plain"}
                          :transport (gateway/injected-transport
                                      {"status" 200 "body" {"json" {}}}
                                      rec)})]
    (routes/create-route c {"path" "/x"})
    (routes/health c)
    (is (= [{"x-trace" "t1" "content-type" "application/json"}
            {"x-trace" "t1" "content-type" "text/plain"}]
           (mapv #(get % "headers") @rec)))))

(deftest query-values-are-form-encoded-in-input-order-and-nil-is-dropped
  (let [rec (gateway/recorder)
        c (routes/client "http://gateway.invalid"
                         {:transport (gateway/injected-transport
                                      {"status" 200 "body" {"json" {}}}
                                      rec)})]
    (routes/list-routes c [["cursor" "a b&c"] ["limit" nil] ["enabled" false]])
    (routes/list-routes c {:limit 5})
    (is (= ["/admin/routes?cursor=a+b%26c&enabled=false" "/admin/routes?limit=5"]
           (mapv #(get % "path") @rec)))))

(deftest a-malformed-transport-response-is-a-transport-failure
  (doseq [response [nil {} {:status "200"} {:status 200 :body 42}]]
    (let [c (claim-check/client "http://x" {:transport (fn [_] response)})
          e (try (claim-check/health c) nil (catch ExceptionInfo e e))]
      (is (= :ankusa.sdk/ClaimCheckUnavailableError (errors/error-type e)) (pr-str response))
      (is (= :transport (:reason (ex-data e))))
      (is (some? (ex-cause e))))))

(deftest a-stalled-body-is-bounded-by-the-timeout
  (with-open [server (ServerSocket. 0 1 (InetAddress/getLoopbackAddress))]
    (let [stall (future
                  ;; Promise ten body bytes, send one, then say nothing.
                  (with-open [socket (.accept server)]
                    (doto (.getOutputStream socket)
                      (.write (.getBytes "HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nx"
                                         StandardCharsets/UTF_8))
                      (.flush))
                    (Thread/sleep 10000)))
          c (claim-check/client (str "http://127.0.0.1:" (.getLocalPort server))
                                {:timeout-ms 300})
          started (System/nanoTime)
          e (try (claim-check/health c) nil (catch ExceptionInfo e e))
          elapsed-ms (/ (- (System/nanoTime) started) 1e6)]
      (future-cancel stall)
      (testing "the call fails as a retryable transport error"
        (is (= :ankusa.sdk/ClaimCheckUnavailableError (errors/error-type e)))
        (is (= :transport (:reason (ex-data e))))
        (is (errors/retryable? e)))
      (testing "and returns within the bound, not when the server gives up"
        (is (< elapsed-ms 3000) (str "took " elapsed-ms " ms"))))))
