(ns ankusa.sdk.sources
  "The tenant-scoped source-management API: the admin listener (`admin.port`,
  default `4002`) serves list/get/create/update/delete for a tenant's ingest
  sources.

  A source is addressed as `<tenant>.<name>`; this client speaks in those terms
  and builds the paths for you. Only `[A-Za-z0-9_-]{1,64}` tenants and names
  are accepted, and both are checked before any path is built, so a
  caller-supplied name cannot escape its tenant through URL normalization.

  A spec is `{:sinks [...] :verify {...} :on-verify-failure \"reject\"}`.
  `:sinks` is required by the server and must be non-empty. `:verify` is
  optional: absent means the server's default (`{\"type\" \"none\"}`), because
  omitting it is not the same as re-sending a secret the API never returns.
  `nil` fields are omitted from the body.

  A source is `{:tenant :name :source-id :ingest-path :verify
  :on-verify-failure :sinks}`. Every value is redacted by the server (secrets,
  passwords, tokens, header values), so a source read back is never useful for
  editing: resending its `:verify` is not the same as resending the stored
  secret.

  ## The version latch

  `:expected-version` is an optional safety latch: when set, every API call
  first makes sure `GET /health` reported that version, and a mismatch is a
  `VersionMismatchError` before the request is sent.

  The client is immutable, so the fetched version is returned rather than
  remembered. Run `verify-version` once at startup and keep the returned
  client; every later call then re-checks the cached version without another
  `/health` request:

  ```clojure
  (def sources (sources/verify-version (sources/client url {:expected-version \"0.3.0\"})))
  (sources/list-sources sources \"acme\")
  ```

  Failures: `SourceNotFoundError` (`404`), `SourceConflictError` (`409`),
  `SourceStoreReadOnlyError` (`409 source_store_read_only`: the deployment's
  source store is a static seed), `SourceInvalidError` (`400`, or an invalid
  tenant or name caught before any request), `VersionMismatchError`, and
  `SourcesUnavailableError` (unreachable, timed out, or `5xx`). Every one
  carries `:status` and `:body` (`nil` when no response was involved)."
  (:require [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.http :as http]))

(set! *warn-on-reflection* true)

;; The same rule the claim-check gateway uses for a tenant. Anything outside it
;; is rejected before a path is built: an unvalidated ".." or "a/b" would
;; escape the tenant scope through URL normalization.
(def ^:private safe-id #"[A-Za-z0-9_-]{1,64}")

(defn client
  "Build a client for the admin listener at `base-url` (an absolute http(s)
  URL).

  `opts` may hold:

  * `:headers`: a map of header name (string or keyword) to string value,
    sent on every request.
  * `:timeout-ms`: a positive integer, default `10000`. It bounds the whole
    call, from connecting through the last body byte.
  * `:expected-version`: a version string to latch against, or `nil` for no
    latch.
  * `:transport`: a function that performs one request, to replace the JDK
    HTTP client (a different HTTP library, an in-process fake). It receives
    `{:method \"GET\" :url \"http://host/path?q\" :headers {\"name\" \"value\"}
    :body <byte[] or nil> :timeout-ms 10000}` and returns
    `{:status 200 :headers {...} :body <byte[] or nil>}`. Throwing any
    `Exception` is a transport failure. It must not follow redirects.

  Bad input throws `IllegalArgumentException`."
  ([base-url] (client base-url nil))
  ([base-url opts]
   (let [http-client (http/client base-url opts [:expected-version])
         expected (:expected-version opts)]
     (when-not (or (nil? expected) (string? expected))
       (throw (IllegalArgumentException.
               (str ":expected-version must be a string, got: " (pr-str expected)))))
     (assoc http-client :expected-version expected :server-version nil))))

(defn- unavailable
  [message status body cause]
  (error/error "SourcesUnavailableError" message {:status status :body body} cause))

(defn- transport-failure
  [e]
  (unavailable (str "ankusa admin API unreachable: " (http/failure-text e)) nil nil e))

;; ---------------------------------------------------------------------------
;; The version latch
;; ---------------------------------------------------------------------------

(defn- fetch-version
  "`client`, with `:server-version` filled from `GET /health` unless it is."
  [client]
  (if (string? (:server-version client))
    client
    (let [response (http/request client "GET" "/health" nil)
          {:keys [status body]} response]
      (cond
        (:error response) (throw (transport-failure (:error response)))

        (= 200 status)
        (let [data (http/decode-json body)]
          (if (and (map? data) (string? (:version data)))
            (assoc client :server-version (:version data))
            (let [decoded (http/error-body body)]
              (throw (unavailable (str "ankusa admin API health check returned no version (200): "
                                       (pr-str decoded))
                                  200 decoded nil)))))

        :else
        (let [decoded (http/error-body body)]
          (throw (unavailable (str "ankusa admin API health check failed (" status "): "
                                   (pr-str decoded))
                              status decoded nil)))))))

(defn- check-version!
  [{:keys [expected-version server-version]}]
  (when (and (some? expected-version) (not= expected-version server-version))
    (throw (error/error "VersionMismatchError"
                        (str "expected Ankusa version " (pr-str expected-version)
                             ", server reports " (pr-str server-version))
                        {:status nil :body nil}))))

(defn verify-version
  "Fetch `GET /health` when the version is not cached yet, enforce
  `:expected-version`, and return the client with the version cached.

  Run it once at startup and keep the returned client; later calls then skip the
  probe."
  [client]
  (let [verified (fetch-version client)]
    (check-version! verified)
    verified))

(defn server-version
  "The deployment's Ankusa version: `verify-version`, then the cached
  `\"version\"` string."
  [client]
  (:server-version (verify-version client)))

(defn- latch
  "`client`, verified when it carries an `:expected-version`."
  [client]
  (if (nil? (:expected-version client))
    client
    (verify-version client)))

;; ---------------------------------------------------------------------------
;; Requests
;; ---------------------------------------------------------------------------

(defn- invalid-input
  [message]
  (error/error "SourceInvalidError" message {:status nil :body nil}))

(defn- validate-tenant!
  [tenant]
  (when-not (and (string? tenant) (re-matches safe-id tenant))
    (throw (invalid-input (str "invalid tenant: " (pr-str tenant))))))

(defn- validate-name!
  [name]
  (when-not (and (string? name) (re-matches safe-id name))
    (throw (invalid-input (str "invalid source name: " (pr-str name))))))

(defn- invalid-message
  [decoded]
  (let [message (when (map? decoded) (or (:message decoded) (:error decoded)))]
    (if (string? message)
      message
      (str "invalid source (400): " (pr-str decoded)))))

(defn- classify
  "The body bytes of a 2xx response, or the error the status classifies as."
  [status body]
  (if (<= 200 status 299)
    body
    (let [decoded (http/error-body body)
          data {:status status :body decoded}]
      (throw
       (case (long status)
         404 (error/error "SourceNotFoundError" (str "source not found (404): " (pr-str decoded)) data)
         400 (error/error "SourceInvalidError" (invalid-message decoded) data)
         409 (if (and (map? decoded) (= "source_store_read_only" (:error decoded)))
               (error/error "SourceStoreReadOnlyError"
                            (str "source store is read-only (409): " (pr-str decoded))
                            data)
               (error/error "SourceConflictError"
                            (str "source already exists (409): " (pr-str decoded))
                            data))
         (unavailable (str "ankusa admin API error (" status "): " (pr-str decoded))
                      status decoded nil))))))

(defn- call
  [client method path opts]
  (let [response (http/request client method path opts)]
    (if (:error response)
      (throw (transport-failure (:error response)))
      (classify (:status response) (:body response)))))

;; ---------------------------------------------------------------------------
;; Decoding
;; ---------------------------------------------------------------------------

(defn- source-from-json
  "A source from a decoded API object, or `nil` when a required field is
  missing or malformed."
  [data]
  (when (and (map? data)
             (every? #(string? (get data %)) [:tenant :name :source_id :ingest_path]))
    {:tenant (:tenant data)
     :name (:name data)
     :source-id (:source_id data)
     :ingest-path (:ingest_path data)
     :verify (or (:verify data) {:type "none"})
     :on-verify-failure (:on_verify_failure data)
     :sinks (or (:sinks data) [])}))

(defn- malformed-source
  [body]
  (unavailable "malformed source in response" 200 (http/error-body body) nil))

(defn- decode-source
  [body]
  (let [data (http/decode-json body)]
    (if (http/invalid? data)
      (throw (unavailable "ankusa admin API returned a non-JSON body (200)"
                          200 (http/error-body body) nil))
      (or (source-from-json data)
          (throw (malformed-source body))))))

(defn- decode-entries
  [body]
  (let [data (http/decode-json body)]
    (when-not (and (map? data) (vector? (:entries data)))
      (let [decoded (http/error-body body)]
        (throw (unavailable (str "ankusa admin API returned a malformed list body (200): "
                                 (pr-str decoded))
                            200 decoded nil))))
    (mapv #(or (source-from-json %) (throw (malformed-source body))) (:entries data))))

(defn- spec-json
  "The JSON body of a create or update, omitting unset (`nil`) fields."
  [spec]
  (when-not (map? spec)
    (throw (IllegalArgumentException. (str "spec must be a map, got: " (pr-str spec)))))
  (cond-> {}
    (some? (:sinks spec)) (assoc "sinks" (:sinks spec))
    (some? (:verify spec)) (assoc "verify" (:verify spec))
    (some? (:on-verify-failure spec)) (assoc "on_verify_failure" (:on-verify-failure spec))))

;; ---------------------------------------------------------------------------
;; The API
;; ---------------------------------------------------------------------------

(defn list-sources
  "A tenant's sources, as a vector: `GET /v1/tenants/{tenant}/sources`."
  [client tenant]
  (validate-tenant! tenant)
  (decode-entries (call (latch client) "GET" (str "/v1/tenants/" tenant "/sources") nil)))

(defn get-source
  "One source: `GET /v1/tenants/{tenant}/sources/{name}`."
  [client tenant name]
  (validate-tenant! tenant)
  (validate-name! name)
  (decode-source (call (latch client) "GET" (str "/v1/tenants/" tenant "/sources/" name) nil)))

(defn create-source
  "Create a source: `POST /v1/tenants/{tenant}/sources`. The name travels in the
  body alongside the spec; the tenant comes from the URL."
  [client tenant name spec]
  (validate-tenant! tenant)
  (validate-name! name)
  (let [body (assoc (spec-json spec) "name" name)]
    (decode-source (call (latch client) "POST" (str "/v1/tenants/" tenant "/sources") {:json body}))))

(defn update-source
  "Replace a source: `PUT /v1/tenants/{tenant}/sources/{name}`. The name comes
  from the URL; the spec carries none."
  [client tenant name spec]
  (validate-tenant! tenant)
  (validate-name! name)
  (let [body (spec-json spec)]
    (decode-source (call (latch client) "PUT" (str "/v1/tenants/" tenant "/sources/" name)
                         {:json body}))))

(defn delete-source
  "Delete a source: `DELETE /v1/tenants/{tenant}/sources/{name}`. Returns `nil`
  on `2xx` (`204` with an empty body). A source that still has undelivered
  hooks on the server's node raises `SourceConflictError` whose body's `error`
  is `source_has_deliveries` (with `pending`/`inflight` counts); this never
  sends the admin API's `?deliveries=dead_letter`."
  [client tenant name]
  (validate-tenant! tenant)
  (validate-name! name)
  (call (latch client) "DELETE" (str "/v1/tenants/" tenant "/sources/" name) nil)
  nil)
