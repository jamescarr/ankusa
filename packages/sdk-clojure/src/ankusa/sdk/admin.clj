(ns ankusa.sdk.admin
  "The admin listener (`admin.port`, default `4002`): health, metrics, the
  effective configuration, the dead-letter queue, the quarantine, and replay
  jobs that re-deliver from either (or from the archive).

  Responses are decoded JSON with keyword keys (`metrics` returns the
  Prometheus text). Failures carry `:retryable`:

  | Error | `:retryable` | Cause |
  | --- | --- | --- |
  | `RoleNotEnabledError` | false | `409 role_not_enabled`: ask another node (`:role` names it) |
  | `AdminRejectedError` | false | any other `4xx` (`:status`, `:code`) |
  | `AdminUnavailableError` | true | `5xx`, a redirect, a non-JSON body, or unreachable |

  Every call is one request: no retry, no followed redirect."
  (:require [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.http :as http])
  (:import (java.nio.charset StandardCharsets)))

(set! *warn-on-reflection* true)

(defn client
  "Build a client for the listener at `base-url` (an absolute http(s) URL).

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
  (error/error "AdminUnavailableError" message data cause))

(defn- rejected
  [status decoded]
  (error/error "AdminRejectedError"
               (str "admin listener rejected the request (" status "): " (pr-str decoded))
               {:status status :code (when (map? decoded) (:error decoded))}))

(defn- response-body
  "The body bytes of a 2xx response, or the error `response` classifies as."
  [response]
  (if-let [e (:error response)]
    (throw (unavailable (str "admin listener unreachable: " (http/failure-text e))
                        {:status nil :reason :transport}
                        e))
    (let [{:keys [status body]} response]
      (cond
        (<= 200 status 299) body

        (= 409 status)
        (let [decoded (http/error-body body)]
          (if (and (map? decoded) (= "role_not_enabled" (:error decoded)))
            (throw (error/error "RoleNotEnabledError"
                                (str "role not enabled on this node: " (pr-str (:role decoded)))
                                {:role (:role decoded)}))
            (throw (rejected status decoded))))

        (<= 400 status 499) (throw (rejected status (http/error-body body)))

        :else (throw (unavailable (str "admin listener error (" status "): "
                                       (pr-str (http/error-body body)))
                                  {:status status :reason :status}
                                  nil))))))

(defn- call-json
  [client method path opts]
  (let [data (http/decode-json (response-body (http/request client method path opts)))]
    (if (http/invalid? data)
      (throw (unavailable "admin listener returned a non-JSON body"
                          {:status nil :reason :invalid-json}
                          nil))
      data)))

(defn- replay-path
  [id]
  (when-not (string? id)
    (throw (IllegalArgumentException.
            (str "replay id must be a string, got: " (pr-str id)))))
  (str "/v1/replays/" (http/path-segment id)))

(defn health
  "Liveness probe: `GET /health` -> `{:status :instance :roles}`."
  [client]
  (call-json client "GET" "/health" nil))

(defn metrics
  "The Prometheus text exposition body: `GET /metrics`, as a string."
  [client]
  (String. ^bytes (response-body (http/request client "GET" "/metrics" nil))
           StandardCharsets/UTF_8))

(defn config
  "The effective, redacted configuration: `GET /v1/config`."
  [client]
  (call-json client "GET" "/v1/config" nil))

(defn list-dead-letters
  "A page of dead-lettered hooks, newest first: `GET /v1/dlq`.

  `params` may carry `:source_id`, `:since` and `:limit` (a map, or a seq of
  `[k v]` pairs when the order must be exact); `nil` values are left out of the
  query string rather than sent as `=`."
  ([client] (list-dead-letters client nil))
  ([client params] (call-json client "GET" "/v1/dlq" {:query params})))

(defn create-replay
  "Create a replay job: `POST /v1/replays` with `spec` as the JSON body.

  `spec` names the job: `{\"kind\" \"dlq\"}`, `{\"kind\" \"archive\"}` or
  `{\"kind\" \"quarantine\"}`, plus the kind's filter (`source_id`/`id`/`since`/
  `until` for `dlq` and `quarantine`, `from`/`to`/`sinks` for `archive`) and the
  optional `rate` and `max_lag_ms`. The response is the full Replay object; a
  retry of the same spec answers the existing running or paused job instead of
  creating a second."
  [client spec]
  (call-json client "POST" "/v1/replays" {:json spec}))

(defn get-replay
  "One replay job by id: `GET /v1/replays/{id}`. A `404` is an
  `AdminRejectedError` with `:code` `\"replay_not_found\"`."
  [client id]
  (call-json client "GET" (replay-path id) nil))

(defn list-replays
  "Every replay job, newest first: `GET /v1/replays` -> `{:replays [...]}`."
  [client]
  (call-json client "GET" "/v1/replays" nil))

(defn update-replay
  "Change a replay job: `PATCH /v1/replays/{id}` with `patch` as the JSON body.

  `patch` may set `state` (`\"running\"`, `\"paused\"`, `\"cancelled\"`), `rate`,
  and `max_lag_ms`. A job that already finished (`done`, `cancelled`, `failed`)
  is a `replay_finished` rejection."
  [client id patch]
  (let [path (replay-path id)]
    (call-json client "PATCH" path {:json patch})))

(defn list-quarantined
  "Recent quarantined hooks, newest first: `GET /v1/quarantine`.

  `params` may carry `:limit`; `nil` values are left out of the query string."
  ([client] (list-quarantined client nil))
  ([client params] (call-json client "GET" "/v1/quarantine" {:query params})))
