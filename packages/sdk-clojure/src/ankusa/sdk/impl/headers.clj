(ns ankusa.sdk.impl.headers
  "Reading request headers the way every receiver-side helper does."
  (:require [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn- header-name
  [k]
  (cond (string? k) (str/lower-case k)
        (keyword? k) (str/lower-case (name k))
        :else nil))

(defn- header-value
  [v]
  (cond (string? v) v
        (or (number? v) (boolean? v)) (str v)
        (keyword? v) (name v)
        :else nil))

(defn normalize
  "`{lowercased-name value}` from a map or a seq of `[name value]` pairs. In a
  seq the first occurrence of a name wins, as HTTP's rule for repeated headers
  has it."
  [headers]
  (reduce (fn [acc pair]
            (let [n (when (and (sequential? pair) (= 2 (count pair)))
                      (header-name (first pair)))]
              (if (and n (not (contains? acc n)))
                (assoc acc n (header-value (second pair)))
                acc)))
          {}
          (when (or (map? headers) (sequential? headers))
            (seq headers))))
