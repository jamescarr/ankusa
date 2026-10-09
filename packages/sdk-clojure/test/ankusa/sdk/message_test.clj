(ns ankusa.sdk.message-test
  "What the conformance vectors do not reach in `ankusa.sdk.message`: input
  forms `decode` accepts, and turning a message into a hook."
  (:require [ankusa.sdk.claim-check :as claim-check]
            [ankusa.sdk.errors :as errors]
            [ankusa.sdk.message :as message]
            [ankusa.sdk.support.gateway :as gateway]
            [clojure.test :refer [deftest is testing]])
  (:import (clojure.lang ExceptionInfo)
           (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(def ^:private claim "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002")
(def ^:private hello-sha256 "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")

(def ^:private inline-json
  "{\"v\":1,\"id\":\"01a0\",\"source_id\":\"demo\",\"tenant_id\":\"acme\",\"received_at\":1,\"size\":5,\"body_base64\":\"aGVsbG8=\",\"idempotency_key\":\"k1\",\"headers\":{\"x-a\":\"b\"}}")

(def ^:private claim-json
  (str "{\"v\":1,\"id\":\"01a0\",\"source_id\":\"demo\",\"tenant_id\":\"acme\",\"received_at\":1,\"size\":5,"
       "\"claim\":\"" claim "\",\"sha256\":\"" hello-sha256 "\"}"))

(defn- gateway-client
  [rec status body]
  (claim-check/client "http://gateway.invalid"
                      {:transport (gateway/injected-transport
                                   {"status" status "body" {"text" body}}
                                   rec)}))

(defn- failure
  [f]
  (try
    (f)
    (is false "expected an error")
    nil
    (catch ExceptionInfo e e)))

(defn- text
  [^bytes bytes]
  (String. bytes StandardCharsets/UTF_8))

(deftest decode-reads-a-string-or-utf-8-bytes
  (let [from-string (message/decode inline-json)
        from-bytes (message/decode (.getBytes ^String inline-json StandardCharsets/UTF_8))]
    (is (= "hello" (text (:body from-string))))
    (is (= (dissoc from-string :body) (dissoc from-bytes :body)))
    (is (= {"x-a" "b"} (:headers from-string)))))

(deftest decode-rejects-bytes-that-are-not-utf-8-as-invalid-json
  (let [e (failure #(message/decode (byte-array [(unchecked-byte 0xff) (unchecked-byte 0xfe)])))]
    (is (= :ankusa.sdk/InvalidMessageError (errors/error-type e)))
    (is (= "invalid_json" (:code (ex-data e))))))

(deftest decode-treats-input-it-cannot-read-as-caller-misuse
  (doseq [bad [nil 42 {"v" 1} :json]]
    (is (thrown? IllegalArgumentException (message/decode bad)) (pr-str bad))))

(deftest an-inline-hook-makes-no-request
  (let [rec (gateway/recorder)
        hook (message/->hook (message/decode inline-json) (gateway-client rec 500 "unused"))]
    (is (= "hello" (text (:body hook))))
    (is (= {:id "01a0" :source-id "demo" :tenant-id "acme" :received-at 1 :size 5
            :content-type nil :dedupe-key nil :replay-id nil :idempotency-key "k1"
            :headers {"x-a" "b"}}
           (dissoc hook :body)))
    (is (= [] @rec))))

(deftest a-claim-hook-redeems-the-claim
  (let [rec (gateway/recorder)
        hook (message/->hook (message/decode claim-json) (gateway-client rec 200 "hello"))]
    (is (= "hello" (text (:body hook))))
    (is (= ["/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002"] (mapv #(get % "path") @rec)))))

(deftest a-redemption-failure-propagates-with-its-retry-decision
  (testing "a missing claim will not appear: do not retry"
    (let [e (failure #(message/->hook (message/decode claim-json)
                                      (gateway-client (gateway/recorder) 404 "")))]
      (is (= :ankusa.sdk/ClaimNotFoundError (errors/error-type e)))
      (is (not (errors/retryable? e)))))

  (testing "a gateway outage might clear: retry"
    (let [e (failure #(message/->hook (message/decode claim-json)
                                      (gateway-client (gateway/recorder) 503 "")))]
      (is (= :ankusa.sdk/ClaimCheckUnavailableError (errors/error-type e)))
      (is (errors/retryable? e))))

  (testing "bytes that do not match the digest are refused"
    (let [e (failure #(message/->hook (message/decode claim-json)
                                      (gateway-client (gateway/recorder) 200 "HELLO")))]
      (is (= :ankusa.sdk/ClaimIntegrityError (errors/error-type e))))))

(deftest a-claim-check-client-is-required-even-for-an-inline-message
  (doseq [bad [nil "http://gateway.invalid" {:base-url "http://gateway.invalid"}]]
    (is (thrown? IllegalArgumentException (message/->hook (message/decode inline-json) bad))
        (pr-str bad))))
