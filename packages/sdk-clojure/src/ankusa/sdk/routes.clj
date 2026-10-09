(ns ankusa.sdk.routes
  "The route-management listener (`routes.admin.port`, default `4003`): the
  route table the edge enforces, the global IP rules, and a dry run that
  replays the guard's decision.

  The listener performs no authentication of its own; `:headers` is for
  whatever a deployer's boundary (a service mesh, an API gateway) expects in
  front of it.

  Route ids are percent-encoded as one path segment, so `/`, `?`, `#` and `%`
  in an id cannot reshape the URL. An id that is not a string, is empty, or is
  `.` or `..` is refused with `:ankusa.sdk/InvalidRouteIdError` before any
  request is sent: URL parsers normalize those away, so `(get-route c \"..\")`
  would quietly hit `/admin/` and return the list page as if it were a route.

  Responses are decoded JSON with keyword keys. Failures carry `:retryable`:

  | Error | `:retryable` | Cause |
  | --- | --- | --- |
  | `InvalidRouteIdError` | false | unusable id, caught before the request |
  | `RouteNotFoundError` | false | `404` |
  | `RoutesRejectedError` | false | any other `4xx` |
  | `RoutesUnavailableError` | true | `5xx`, a redirect, a non-JSON body, or unreachable |

  Every call is one request: no retry, no followed redirect."
  (:require [ankusa.sdk.impl.error :as error]
            [ankusa.sdk.impl.http :as http]))

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
  (error/error "RoutesUnavailableError" message data cause))

(defn- field
  [decoded k]
  (when (map? decoded)
    (get decoded k)))

(defn- rejected
  [status body]
  (let [decoded (http/error-body body)
        code (field decoded :error)
        message (field decoded :message)]
    (error/error "RoutesRejectedError"
                 (str "route request rejected (" status (when code (str " " code)) ")"
                      (when message (str ": " message)))
                 {:status status
                  :code code
                  :field (field decoded :field)
                  :message message
                  :conflicting-id (field decoded :conflicting_id)
                  :max-routes (field decoded :max_routes)})))

(defn- response-body
  "The body bytes of a 2xx response, or the error `response` classifies as."
  [response]
  (if-let [e (:error response)]
    (throw (unavailable (str "routes listener unreachable: " (http/failure-text e))
                        {:status nil :reason :transport}
                        e))
    (let [{:keys [status body]} response]
      (cond
        (<= 200 status 299) body
        (= 404 status) (throw (error/error "RouteNotFoundError" "route not found (404)" {}))
        (<= 400 status 499) (throw (rejected status body))
        :else (throw (unavailable (str "routes listener error (" status "): "
                                       (pr-str (http/error-body body)))
                                  {:status status :reason :status}
                                  nil))))))

(defn- call-json
  [client method path opts]
  (let [data (http/decode-json (response-body (http/request client method path opts)))]
    (if (http/invalid? data)
      (throw (unavailable "routes listener returned a non-JSON body"
                          {:status nil :reason :invalid-json}
                          nil))
      data)))

(defn- route-path
  [id]
  (cond
    (not (string? id)) (throw (error/error "InvalidRouteIdError"
                                           (str "route id must be a string, got: " (pr-str id))
                                           {}))
    (contains? #{"" "." ".."} id) (throw (error/error "InvalidRouteIdError"
                                                      (str "invalid route id " (pr-str id))
                                                      {}))
    :else (str "/admin/routes/" (http/path-segment id))))

(defn- ensure-map
  [what x]
  (when-not (map? x)
    (throw (IllegalArgumentException. (str what " must be a map, got: " (pr-str x)))))
  x)

(defn health
  "Liveness probe: `GET /health` -> `{:status :routes}`."
  [client]
  (call-json client "GET" "/health" nil))

(defn list-routes
  "A page of route definitions: `GET /admin/routes`.

  `params` may carry `:enabled`, `:limit` and `:cursor` (a map, or a seq of
  `[k v]` pairs when the order must be exact); `nil` values are left out of the
  query string rather than sent as `=`, and the rest go out in the order given."
  ([client] (list-routes client nil))
  ([client params] (call-json client "GET" "/admin/routes" {:query params})))

(defn create-route
  "Store a route: `POST /admin/routes` with `input` as the JSON body. Returns
  the route, timestamps included."
  [client input]
  (call-json client "POST" "/admin/routes" {:json (ensure-map "route input" input)}))

(defn get-route
  "Fetch one route: `GET /admin/routes/{id}`."
  [client id]
  (call-json client "GET" (route-path id) nil))

(defn replace-route
  "Replace a route: `PUT /admin/routes/{id}` with `input` as the JSON body."
  [client id input]
  (let [path (route-path id)]
    (call-json client "PUT" path {:json (ensure-map "route input" input)})))

(defn update-route
  "Patch a route: `PATCH /admin/routes/{id}` with `patch` as the JSON body."
  [client id patch]
  (let [path (route-path id)]
    (call-json client "PATCH" path {:json (ensure-map "route patch" patch)})))

(defn delete-route
  "Delete a route: `DELETE /admin/routes/{id}`. Returns `nil` on `2xx`; the body
  is not parsed (`204` carries none)."
  [client id]
  (response-body (http/request client "DELETE" (route-path id) nil))
  nil)

(defn get-ip-rules
  "The global IP rules: `GET /admin/ip-rules`."
  [client]
  (call-json client "GET" "/admin/ip-rules" nil))

(defn put-ip-rules
  "Replace the global IP rules: `PUT /admin/ip-rules` with `rules` as the JSON
  body."
  [client rules]
  (call-json client "PUT" "/admin/ip-rules" {:json (ensure-map "IP rules" rules)}))

(defn test-route
  "The dry-run decision for one request: `POST /admin/routes/test` with
  `request` as the JSON body. Nothing is captured; the response replays what
  the guard would have decided."
  [client request]
  (call-json client "POST" "/admin/routes/test" {:json (ensure-map "test request" request)}))
