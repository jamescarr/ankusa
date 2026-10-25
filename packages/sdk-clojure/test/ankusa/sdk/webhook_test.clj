(ns ankusa.sdk.webhook-test
  "What the conformance vectors do not reach in `ankusa.sdk.webhook`."
  (:require [ankusa.sdk.webhook :as webhook]
            [clojure.test :refer [deftest is]]))

(set! *warn-on-reflection* true)

(deftest parse-headers-verification-is-nil-when-absent
  (is (nil? (:verification (webhook/parse-headers {"x-ankusa-id" "01a0"})))))

(deftest parse-headers-verification-flagged-is-true
  (is (= {:status "failed" :flagged true}
         (:verification (webhook/parse-headers
                         {"x-ankusa-id" "01a0"
                          "x-ankusa-verification" "failed"
                          "x-ankusa-verification-flagged" "true"})))))

(deftest parse-headers-reads-delivery-metadata
  (let [sha256 "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        parsed (webhook/parse-headers {"x-ankusa-id" "01a0"
                                       "X-Ankusa-Received-At" "1720000000000"
                                       "x-ankusa-body-sha256" sha256})]
    (is (= 1720000000000 (:received-at parsed)))
    (is (= sha256 (:body-sha256 parsed)))))

(deftest parse-headers-malformed-delivery-metadata-is-nil
  (let [parsed (webhook/parse-headers {"x-ankusa-id" "01a0"
                                       "x-ankusa-received-at" "-1"
                                       "x-ankusa-body-sha256" "ABC"})]
    (is (nil? (:received-at parsed)))
    (is (nil? (:body-sha256 parsed)))))
