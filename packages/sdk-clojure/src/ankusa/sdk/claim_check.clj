(ns ankusa.sdk.claim-check
  "The claim-check gateway client: fetch the bytes a claim ref names and
  verify them against the digest the queue message carries.

  ```clojure
  (def gateway (claim-check/client \"http://localhost:4004\"))
  (claim-check/redeem gateway (:claim message) (:sha256 message))
  ```

  The gateway never checks the digest itself, so `redeem` always does: the
  reader's own end-to-end check, run before the bytes are returned.

  Failures are `ex-info`s; see `ankusa.sdk.errors` for the types and the
  `:retryable` bit. Every call is one request: no retry, no followed redirect."
  (:require [ankusa.sdk.claim-ref :as claim-ref]
            [ankusa.sdk.impl.digest :as digest]
            [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.http :as http]))

(set! *warn-on-reflection* true)

(defn client
  "Build a client for the gateway at `base-url` (an absolute http(s) URL).

  `opts` may hold:

  * `:headers`: a map of header name (string or keyword) to string value,
    sent on every request.
  * `:timeout-ms`: a positive integer, default `10000`. It bounds the whole
    call, from connecting through the last body byte.
  * `:transport`: a function that performs one request, to replace the JDK
    HTTP client (a different HTTP library, an in-process fake). It receives
    `{:method \"GET\" :url \"http://host/path?q\" :headers {\"name\" \"value\"}
    :body <byte[] or nil> :timeout-ms 10000}` and returns
    `{:status 200 :headers {...} :body <byte[] or nil>}`. Throwing any
    `Exception` is a transport failure. It must not follow redirects.

  Bad input throws `IllegalArgumentException`."
  ([base-url] (client base-url nil))
  ([base-url opts] (http/client base-url opts nil)))

(defn- unavailable
  [message data cause]
  (error/error "ClaimCheckUnavailableError" message data cause))

(defn- transport-failure
  [e]
  (unavailable (str "claim-check gateway unreachable: " (http/failure-text e))
               {:status nil :reason :transport}
               e))

(def ^:private sha256-pattern #"[0-9a-f]{64}")

(defn- validate-sha256!
  [sha256]
  (when-not (and (string? sha256) (re-matches sha256-pattern sha256))
    (throw (error/error "InvalidClaimRefError" (str "invalid claim sha256: " (pr-str sha256)) {}))))

(defn- verify
  [{:keys [tenant-id claim-id]} sha256 body]
  (if (= sha256 (digest/sha256-hex body))
    body
    (throw (error/error "ClaimIntegrityError"
                        (str "claim sha256 mismatch for " tenant-id "/" claim-id)
                        {}))))

(defn redeem
  "Fetch the bytes `ref` names and return them as a `byte[]`, after checking
  that their SHA-256 is `sha256` (64 lowercase hex characters).

  Nothing is requested when `ref` or `sha256` is malformed
  (`:ankusa.sdk/InvalidClaimRefError`). Otherwise: `ClaimNotFoundError` on a
  `404`, `ClaimRejectedError` on any other `4xx` but `408` and `429`,
  `ClaimIntegrityError` when the digest differs, and
  `ClaimCheckUnavailableError` (retryable) for a `408`, a `429` (a front layer
  throttling or timing out), any other status, or an unreachable gateway."
  [client ref sha256]
  (let [parsed (claim-ref/parse ref)
        _ (validate-sha256! sha256)
        {:keys [status body] :as response} (http/request client "GET" (:path parsed) nil)]
    (cond
      (:error response) (throw (transport-failure (:error response)))
      (= 200 status) (verify parsed sha256 body)
      (= 404 status) (throw (error/error "ClaimNotFoundError"
                                         (str "claim not found: " (:tenant-id parsed) "/" (:claim-id parsed))
                                         {}))
      (and (<= 400 status 499) (not (#{408 429} status)))
      (let [decoded (http/error-body body)]
        (throw (error/error "ClaimRejectedError"
                            (str "claim-check rejected redeem (" status "): " (pr-str decoded))
                            {:status status :body decoded})))
      :else (throw (unavailable (str "claim-check gateway error (" status "): "
                                     (pr-str (http/error-body body)))
                                {:status status :reason :status}
                                nil)))))

(defn health
  "Liveness probe: `GET /health`. A `200` with a JSON body returns it decoded
  (keyword keys); anything else, including a `200` with a body that is not
  JSON, throws `:ankusa.sdk/ClaimCheckUnavailableError`."
  [client]
  (let [{:keys [status body] :as response} (http/request client "GET" "/health" nil)]
    (cond
      (:error response) (throw (transport-failure (:error response)))
      (= 200 status) (let [data (http/decode-json body)]
                       (if (http/invalid? data)
                         (throw (unavailable "claim-check gateway health check returned a non-JSON body (200)"
                                             {:status 200 :reason :invalid-json}
                                             nil))
                         data))
      :else (throw (unavailable (str "claim-check gateway health check failed (" status ")")
                                {:status status :reason :status}
                                nil)))))
