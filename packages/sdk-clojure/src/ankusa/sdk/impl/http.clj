(ns ankusa.sdk.impl.http
  "The one place the SDK issues requests.

  Conventions every caller relies on, because the conformance vectors count
  requests and bound latency:

  * One request per call. There is no retry.
  * A redirect is never followed: a `3xx` is classified as `unavailable`.
  * A non-2xx status is a value each client maps to its own error, never an
    exception.
  * Bodies stay bytes; the caller decides whether they are claim bytes, JSON,
    or Prometheus text."
  (:require [clojure.data.json :as json]
            [clojure.string :as str])
  (:import (java.io PushbackReader StringReader Writer)
           (java.net URI URISyntaxException URLEncoder)
           (java.net.http HttpClient HttpClient$Redirect HttpClient$Version
                          HttpRequest HttpRequest$Builder HttpRequest$BodyPublishers
                          HttpResponse HttpResponse$BodyHandlers HttpTimeoutException)
           (java.nio ByteBuffer)
           (java.nio.charset CharacterCodingException CodingErrorAction StandardCharsets)
           (java.time Duration)
           (java.util.concurrent CompletableFuture ExecutionException TimeUnit TimeoutException)))

(set! *warn-on-reflection* true)

(def ^:private empty-body (byte-array 0))

;; ---------------------------------------------------------------------------
;; The client
;; ---------------------------------------------------------------------------

;; A header value is a credential more often than not. It is held as a
;; `Secret`, which prints as a placeholder, so no way of printing a client
;; (`pr`, `pprint`, a log line, an exception message) can show it.
(deftype Secret [^String value]
  Object
  (toString [_] "<redacted>")
  (equals [_ other] (and (instance? Secret other) (= value (.-value ^Secret other))))
  (hashCode [_] (.hashCode value)))

(defmethod print-method Secret [_ ^Writer w]
  (.write w "#ankusa.sdk/redacted"))

(defn- reveal
  "The header value a `Secret` holds."
  [^Secret secret]
  (.-value secret))

;; `headers` is a vector of `[lowercase-name Secret]`.
(defrecord Client [base-url headers timeout-ms transport])

(defn- iae
  "Throw `IllegalArgumentException` with `message`."
  [message]
  (throw (IllegalArgumentException. ^String message)))

(defn- validate-base-url!
  [base-url]
  (let [^URI uri (when (string? base-url)
                   (try
                     (URI. ^String base-url)
                     (catch URISyntaxException _ nil)))
        scheme (some-> uri .getScheme str/lower-case)]
    (when-not (and uri (contains? #{"http" "https"} scheme) (.getHost uri))
      (iae (str "base URL must be an absolute http(s) URL, got: " (pr-str base-url))))))

(defn- validate-opts!
  [opts extra-keys]
  (when-not (or (nil? opts) (map? opts))
    (iae (str "options must be a map, got: " (pr-str opts))))
  (let [allowed (into #{:headers :timeout-ms :transport} extra-keys)
        unknown (first (remove allowed (keys opts)))]
    (when (some? unknown)
      (iae (str "unknown option " (pr-str unknown)
                " — expected one of " (str/join ", " (map pr-str (sort allowed))))))))

(def ^:private header-name-re #"[!#$%&'*+.^_`|~0-9A-Za-z-]+")

;; The JDK client refuses these, and a caller-set value would fight the one it
;; computes itself.
(def ^:private restricted-headers #{"connection" "content-length" "expect" "host" "upgrade"})

(defn- header-pair
  [[k v]]
  (let [lname (cond (string? k) (str/lower-case k)
                    (keyword? k) (str/lower-case (name k))
                    :else (iae (str "header name must be a string or keyword, got: " (pr-str k))))]
    (when-not (re-matches header-name-re lname)
      (iae (str "invalid header name: " (pr-str lname))))
    (when (contains? restricted-headers lname)
      (iae (str "header " (pr-str lname) " is set by the HTTP client and cannot be overridden")))
    ;; The value is a credential more often than not: it stays out of the message.
    (when-not (and (string? v) (not (re-find #"[\r\n\u0000]" v)))
      (iae (str "value of header " (pr-str lname) " must be a string without CR, LF, or NUL")))
    [lname (Secret. v)]))

(defn- normalize-headers
  [headers]
  (cond
    (nil? headers) []
    (map? headers) (mapv header-pair headers)
    :else (iae (str "headers must be a map, got: " (pr-str headers)))))

(defn- validate-timeout!
  [timeout-ms]
  (if (and (int? timeout-ms) (pos? timeout-ms))
    timeout-ms
    (iae (str ":timeout-ms must be a positive integer, got: " (pr-str timeout-ms)))))

(declare jdk-transport)

(defn client
  "Build the `Client` record every public client wraps.

  `opts` is `nil` or a map with `:headers`, `:timeout-ms` (default 10000),
  `:transport` (default the JDK HTTP client), plus any of `extra-keys`.
  Anything else throws `IllegalArgumentException`, as does a base URL that is
  not an absolute http(s) URL."
  [base-url opts extra-keys]
  (validate-base-url! base-url)
  (validate-opts! opts extra-keys)
  (let [transport (get opts :transport jdk-transport)]
    (when-not (or (fn? transport) (var? transport))
      (iae (str ":transport must be a function, got: " (pr-str transport))))
    (->Client (str/replace ^String base-url #"/+$" "")
              (normalize-headers (:headers opts))
              (validate-timeout! (get opts :timeout-ms 10000))
              transport)))

;; ---------------------------------------------------------------------------
;; The default transport
;; ---------------------------------------------------------------------------

;; HTTP/1.1 on purpose: the Ankusa listeners speak plain HTTP/1.1, and pinning
;; it keeps the JDK client from sending an h2c upgrade header they would have
;; to ignore.
(def ^:private jdk-client
  (delay (-> (HttpClient/newBuilder)
             (.version HttpClient$Version/HTTP_1_1)
             (.followRedirects HttpClient$Redirect/NEVER)
             (.build))))

(defn- jdk-transport
  "The default transport: see `request` for the request and response maps."
  [{:keys [method url headers body timeout-ms]}]
  (let [^HttpClient http @jdk-client
        timeout (Duration/ofMillis (long timeout-ms))
        publisher (if body
                    (HttpRequest$BodyPublishers/ofByteArray ^bytes body)
                    (HttpRequest$BodyPublishers/noBody))
        ^HttpRequest$Builder builder (-> (HttpRequest/newBuilder (URI/create url))
                                         (.timeout timeout)
                                         (.method ^String method publisher))]
    (doseq [[k v] headers]
      (.header builder ^String k ^String v))
    ;; `.get` with a timeout, not just `HttpRequest.timeout`: the request
    ;; timeout stops at the response headers, while this bounds connect through
    ;; the last body byte.
    (let [^CompletableFuture future (.sendAsync http (.build builder) (HttpResponse$BodyHandlers/ofByteArray))
          ^HttpResponse response (try
                                   (.get future (long timeout-ms) TimeUnit/MILLISECONDS)
                                   (catch TimeoutException _
                                     (.cancel future true)
                                     (throw (HttpTimeoutException.
                                             (str "no complete response within " timeout))))
                                   (catch InterruptedException e
                                     (.cancel future true)
                                     (throw e))
                                   (catch ExecutionException e
                                     (throw ^Throwable (or (.getCause e) e))))]
      {:status (.statusCode response)
       :headers (into {} (map (fn [[k vs]] [k (first vs)])) (.map (.headers response)))
       :body (.body response)})))

;; ---------------------------------------------------------------------------
;; Requests
;; ---------------------------------------------------------------------------

(defn- encode
  [s]
  (URLEncoder/encode ^String s StandardCharsets/UTF_8))

(defn- query-key
  [k]
  (cond (string? k) k
        (keyword? k) (name k)
        :else (iae (str "invalid query parameter name: " (pr-str k)))))

(defn- query-value
  [v]
  (if (keyword? v) (name v) (str v)))

(defn- query-pair
  [pair]
  (when-not (and (sequential? pair) (= 2 (count pair)))
    (iae (str "invalid query parameter: " (pr-str pair))))
  (let [[k v] pair]
    (when (some? v)
      (str (encode (query-key k)) "=" (encode (query-value v))))))

(defn- query-string
  [params]
  (let [pairs (cond (or (nil? params) (map? params) (sequential? params)) (seq params)
                    :else (iae (str "invalid query parameters: " (pr-str params))))
        encoded (into [] (keep query-pair) pairs)]
    (if (seq encoded) (str "?" (str/join "&" encoded)) "")))

(defn- response-body
  [body]
  (cond (nil? body) empty-body
        (bytes? body) body
        (string? body) (.getBytes ^String body StandardCharsets/UTF_8)
        :else (throw (IllegalStateException.
                      (str "transport returned a " (.getName (class body)) " body, not bytes")))))

(defn failure-text
  "A one-line description of a transport failure: its class, then its message
  when it has one."
  [^Throwable e]
  (let [message (.getMessage e)]
    (str (.getName (class e)) (when message (str ": " message)))))

(defn request
  "Issue one request and return `{:status int :body bytes}`, or `{:error e}`
  when the transport threw (an unreachable server, a timeout, a refused
  connection).

  `method` is an upper-case string and `path` starts with `/`. `opts` may hold
  `:query` (`nil`, a map, or a seq of `[k v]` pairs; `nil` values are dropped
  and input order is kept) and `:json` (encoded as the request body).

  The transport is called with
  `{:method :url :headers {name value} :body <bytes or nil> :timeout-ms}` and
  must return `{:status int :headers {...} :body <bytes or nil>}` without
  following redirects."
  [^Client client method path {:keys [query] :as opts}]
  (let [body (when (some? (:json opts))
               (.getBytes ^String (json/write-str (:json opts) :escape-slash false)
                          StandardCharsets/UTF_8))
        headers (let [base (into {} (map (fn [[k secret]] [k (reveal secret)])) (:headers client))]
                  (if body (assoc base "content-type" "application/json") base))
        req {:method method
             :url (str (:base-url client) path (query-string query))
             :headers headers
             :body body
             :timeout-ms (:timeout-ms client)}]
    (try
      (let [response ((:transport client) req)
            status (:status response)]
        (when-not (int? status)
          (throw (IllegalStateException. "transport returned no integer :status")))
        {:status status :body (response-body (:body response))})
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (throw e))
      (catch Exception e
        {:error e}))))

;; ---------------------------------------------------------------------------
;; Bodies
;; ---------------------------------------------------------------------------

(defn invalid?
  "True when `v` is what `parse-json` returns for text that is not JSON."
  [v]
  (identical? v ::invalid))

(defn utf8-string
  "Strictly decode `bytes` as UTF-8, or return `nil` when they are not."
  [^bytes bytes]
  (try
    (str (.decode (doto (.newDecoder StandardCharsets/UTF_8)
                    (.onMalformedInput CodingErrorAction/REPORT)
                    (.onUnmappableCharacter CodingErrorAction/REPORT))
                  (ByteBuffer/wrap bytes)))
    (catch CharacterCodingException _ nil)))

(defn parse-json
  "Parse `s` as exactly one JSON value (only whitespace may follow it), with
  object keys passed through `key-fn`. Returns `::invalid` when `s` is not JSON,
  so a literal JSON `null` (which is `nil`) stays distinguishable."
  [^String s key-fn]
  (try
    (with-open [reader (PushbackReader. (StringReader. s) 64)]
      (let [value (json/read reader :key-fn key-fn)]
        (loop []
          (let [c (.read reader)]
            (cond (neg? c) value
                  (let [c (long c)] (or (== c 32) (== c 9) (== c 10) (== c 13))) (recur)
                  :else ::invalid)))))
    (catch Exception _ ::invalid)
    (catch StackOverflowError _ ::invalid)))

(defn decode-json
  "Parse the response `bytes` as JSON with keyword keys, or return `::invalid`."
  [bytes]
  (if-let [s (utf8-string bytes)]
    (parse-json s keyword)
    ::invalid))

(defn error-body
  "The body as JSON (keyword keys) when it parses, else its text (`\"\"` when
  empty)."
  [^bytes bytes]
  (let [value (decode-json bytes)]
    (if (invalid? value)
      (String. bytes StandardCharsets/UTF_8)
      value)))

(def ^:private ^String hex-digits "0123456789ABCDEF")

(defn- unreserved?
  "RFC 3986 unreserved: the only bytes a path segment leaves unescaped."
  [^long c]
  (or (<= 65 c 90) (<= 97 c 122) (<= 48 c 57) (== c 45) (== c 46) (== c 95) (== c 126)))

(defn path-segment
  "Percent-encode `s` as exactly one path segment: every UTF-8 byte outside
  RFC 3986 unreserved becomes `%XX`, so `/`, `?`, `#` and `%` cannot reshape
  the URL (and a space is `%20`, not `+`)."
  [^String s]
  (let [bytes (.getBytes s StandardCharsets/UTF_8)
        sb (StringBuilder. (alength bytes))]
    (dotimes [i (alength bytes)]
      (let [c (bit-and (long (aget bytes i)) 0xFF)]
        (if (unreserved? c)
          (.append sb (char c))
          (doto sb
            (.append \%)
            (.append (.charAt hex-digits (bit-shift-right c 4)))
            (.append (.charAt hex-digits (bit-and c 0xF)))))))
    (str sb)))
