(ns ankusa.sdk.verification-test
  "What the conformance vectors do not reach in `ankusa.sdk.verification`."
  (:require [ankusa.sdk.claim-check :as claim-check]
            [ankusa.sdk.message :as message]
            [ankusa.sdk.verification :as verification]
            [ankusa.sdk.webhook :as webhook]
            [clojure.test :refer [deftest is]]))

(set! *warn-on-reflection* true)

(deftest trusted-only-for-an-unflagged-ok
  (is (true? (verification/trusted? {:verification {:status "ok" :flagged false}})))
  (is (false? (verification/trusted? {:verification {:status "failed" :flagged true}})))
  (is (false? (verification/trusted? {:verification {:status "ok" :flagged true}})))
  (is (false? (verification/trusted? {:verification {:status "skipped" :flagged false}})))
  (is (false? (verification/trusted? {:verification {:status "attested" :flagged false}}))))

(deftest a-hook-from-a-node-older-than-the-field-is-untrusted
  (is (false? (verification/trusted? {})))
  (is (false? (verification/trusted? {:verification nil}))))

(deftest parsed-headers-answer-the-same-way
  (is (true? (verification/trusted?
              (webhook/parse-headers {"x-ankusa-id" "01a0" "x-ankusa-verification" "ok"}))))
  (is (false? (verification/trusted?
               (webhook/parse-headers {"x-ankusa-id" "01a0"
                                       "x-ankusa-verification" "failed"
                                       "x-ankusa-verification-flagged" "true"})))))

(deftest a-hook-built-from-a-message-keeps-the-verdict
  (let [client (claim-check/client "http://gateway.invalid")
        decode (fn [v]
                 (message/decode
                  (str "{\"v\":1,\"id\":\"01a0\",\"source_id\":\"demo\",\"received_at\":1,"
                       "\"size\":0,\"body_base64\":\"\"" v "}")))
        flagged (message/->hook
                 (decode ",\"verification\":{\"status\":\"failed\",\"flagged\":true}") client)
        ok (message/->hook
            (decode ",\"verification\":{\"status\":\"ok\",\"flagged\":false}") client)]
    (is (= {:status "failed" :flagged true} (:verification flagged)))
    (is (false? (verification/trusted? flagged)))
    (is (true? (verification/trusted? ok)))))
