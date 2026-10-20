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
