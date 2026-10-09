(ns ankusa.sdk.signature
  "Verify the Standard Webhooks signature (https://www.standardwebhooks.com/)
  an HTTP sink with a `secret` adds to every delivery.

  `webhook-signature` holds space-separated `v1,<base64>` entries, each an
  HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`. A delivery passes
  when any `v1` entry matches any configured secret (several during a
  rotation), compared in constant time with `MessageDigest/isEqual`, and
  `webhook-timestamp` is within the tolerance of now. Secrets are `whsec_` +
  base64, or any other string used as its own UTF-8 bytes.

  A failure throws `:ankusa.sdk/InvalidSignatureError` with `:code`
  (`\"invalid_secret\"`, `\"missing_header\"`, `\"invalid_timestamp\"`,
  `\"timestamp_out_of_tolerance\"`, `\"no_matching_signature\"`) and `:field`
  (the header at fault, `nil` for `\"invalid_secret\"`). It is never
  retryable: answer `401`."
  (:require [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.headers :as headers]
            [clojure.string :as str])
  (:import (java.nio.charset StandardCharsets)
           (java.security MessageDigest)
           (java.util Base64)
           (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)))

(set! *warn-on-reflection* true)

(def default-tolerance-seconds
  "How far either side of now `webhook-timestamp` may be, by default."
  300)

(defn- fail
  [code field message]
  (throw (error/error "InvalidSignatureError" message {:code code :field field})))

(defn- utf8
  ^bytes [^String s]
  (.getBytes s StandardCharsets/UTF_8))

(defn- decode-base64
  "The decoded bytes, or `nil` when `s` is not padded base64 (the JDK decoder
  also takes unpadded input, which the other SDKs and core refuse)."
  [^String s]
  (when (zero? (mod (count s) 4))
    (try
      (.decode (Base64/getDecoder) s)
      (catch IllegalArgumentException _ nil))))

(defn- key-bytes
  ^bytes [secret]
  (cond
    (not (string? secret))
    (fail "invalid_secret" nil "a secret is not a string")

    (str/starts-with? secret "whsec_")
    (let [^bytes k (decode-base64 (subs secret 6))]
      (if (and k (pos? (alength k)))
        k
        (fail "invalid_secret" nil "a whsec_ secret is not valid base64")))

    (= "" secret)
    (fail "invalid_secret" nil "an empty secret")

    :else (utf8 secret)))

(defn- required
  [h n]
  (let [v (get h n)]
    (if (or (nil? v) (= "" v))
      (fail "missing_header" n (str "missing " n " header"))
      v)))

(defn- hmac
  ^bytes [^bytes k ^bytes prefix ^bytes body]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. k "HmacSHA256"))
    (.update mac prefix)
    (.doFinal mac body)))

(defn verify
  "Verify one delivery and return `{:id :timestamp}` (the `webhook-id`, and
  `webhook-timestamp` as unix seconds).

  `headers` is a map (Ring's `:headers`) or a seq of `[name value]` pairs,
  matched case-insensitively; `body` the raw bytes received (`byte[]` or a
  String, taken as UTF-8); `secrets` one secret or a seq of them. Options:
  `:tolerance-seconds` (default 300) and `:now` (unix seconds; default the
  clock).

  The checks run in this order: `invalid_secret`, `missing_header`
  (`webhook-id`, `webhook-timestamp`, `webhook-signature`),
  `invalid_timestamp`, `timestamp_out_of_tolerance`,
  `no_matching_signature`."
  ([headers body secrets]
   (verify headers body secrets {}))
  ([headers body secrets {:keys [tolerance-seconds now]
                          :or {tolerance-seconds default-tolerance-seconds}}]
   (let [secrets (if (string? secrets) [secrets] (seq secrets))
         _ (when (empty? secrets) (fail "invalid_secret" nil "no secret configured"))
         ks (mapv key-bytes secrets)
         h (headers/normalize headers)
         id (required h "webhook-id")
         raw-timestamp (required h "webhook-timestamp")
         signature (required h "webhook-signature")
         timestamp (when (re-matches #"[0-9]+" raw-timestamp)
                     (try (Long/parseLong raw-timestamp)
                          (catch NumberFormatException _ nil)))
         _ (when-not timestamp
             (fail "invalid_timestamp" "webhook-timestamp" "webhook-timestamp is not a unix time"))
         now (or now (quot (System/currentTimeMillis) 1000))
         _ (when (> (abs (- now timestamp)) tolerance-seconds)
             (fail "timestamp_out_of_tolerance" "webhook-timestamp"
                   "webhook-timestamp is outside the tolerance window"))
         ^bytes body (if (string? body) (utf8 body) body)
         prefix (utf8 (str id "." raw-timestamp "."))
         candidates (keep (fn [^String entry]
                            (when (str/starts-with? entry "v1,")
                              (decode-base64 (subs entry 3))))
                          (str/split signature #" "))]
     (if (some (fn [k]
                 (let [expected (hmac k prefix body)]
                   (some #(MessageDigest/isEqual expected ^bytes %) candidates)))
               ks)
       {:id id :timestamp timestamp}
       (fail "no_matching_signature" "webhook-signature" "no webhook-signature entry matches")))))
