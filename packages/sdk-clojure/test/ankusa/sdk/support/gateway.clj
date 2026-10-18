(ns ankusa.sdk.support.gateway
  "The mock a client under test talks to: a real HTTP server on a free loopback
  port, or an in-process transport that never touches the network.

  Either way every request is recorded, so a test can assert the method, path,
  headers and body independent of whether a socket was involved. A record is
  `{\"method\" \"path\" \"headers\" \"body\"}` with `path` including the query
  string exactly as sent, headers lowercased, and `body` the parsed JSON, the
  text when it is not JSON, or `nil` when there was none."
  (:require [clojure.data.json :as json]
            [clojure.string :as str])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.io IOException)
           (java.net InetAddress InetSocketAddress URI)
           (java.nio.charset StandardCharsets)
           (java.util Base64)
           (java.util.concurrent ExecutorService Executors)))

(set! *warn-on-reflection* true)

(defn recorder
  "A fresh, empty request log."
  []
  (atom []))

(defn body-bytes
  "A vector Body in bytes: text as UTF-8, base64 decoded, json as compact JSON
  (what `JSON.stringify` writes); an absent body is empty."
  ^bytes [spec]
  (cond
    (nil? spec) (byte-array 0)
    (contains? spec "text") (.getBytes ^String (get spec "text") StandardCharsets/UTF_8)
    (contains? spec "base64") (.decode (Base64/getDecoder) ^String (get spec "base64"))
    (contains? spec "json") (.getBytes ^String (json/write-str (get spec "json")
                                                               :escape-slash false
                                                               :escape-unicode false)
                                       StandardCharsets/UTF_8)
    :else (byte-array 0)))

(defn decode-request-body
  "A recorded body: `nil` when empty, parsed JSON (string keys) when it is JSON,
  else the text."
  [^bytes bytes]
  (when (and bytes (pos? (alength bytes)))
    (let [text (String. bytes StandardCharsets/UTF_8)]
      (try
        (json/read-str text)
        (catch Exception _ text)))))

(defn- request-path
  [^URI uri]
  (let [query (.getRawQuery uri)]
    (str (.getRawPath uri) (when query (str "?" query)))))

(defn- record!
  "Append the request to `rec` and return the record."
  [rec method path headers body]
  (let [record {"method" method
                "path" path
                "headers" headers
                "body" (decode-request-body body)}]
    (swap! rec conj record)
    record))

(defn recording-transport
  "An SDK transport that records each request in `rec` and answers it with
  `(respond record)`, where `record` is the request as recorded and the result
  is `{:status int :headers {...} :body <bytes, a string, or nil>}`. A throw from
  `respond` is a transport failure."
  [rec respond]
  (fn [{:keys [method url headers body]}]
    (respond (record! rec method (request-path (URI/create ^String url))
                      (into {} (map (fn [[k v]] [(str/lower-case k) v])) headers)
                      body))))

(defn injected-transport
  "An SDK transport that serves the vector gateway `spec` in-process and records
  requests in `rec`, with the `content-length` a real server would send."
  [spec rec]
  (let [payload (body-bytes (get spec "body"))]
    (recording-transport
     rec
     (fn [_record]
       {:status (get spec "status")
        :headers (assoc (into {} (get spec "headers")) "content-length" (str (alength payload)))
        :body payload}))))

(defn- first-values
  [headers]
  (into {}
        (keep (fn [[k vs]] (when (seq vs) [(str/lower-case k) (first vs)])))
        headers))

(defn- answer
  [^HttpExchange exchange spec ^bytes payload rec]
  (let [request-body (with-open [in (.getRequestBody exchange)] (.readAllBytes in))]
    ;; Record before sleeping: a vector whose client abandons the response
    ;; still asserts the request that was made.
    (record! rec
             (.getRequestMethod exchange)
             (request-path (.getRequestURI exchange))
             (first-values (.getRequestHeaders exchange))
             request-body))
  (let [delay-ms (long (get spec "delay_ms" 0))
        slept? (if (pos? delay-ms)
                 (try
                   (Thread/sleep delay-ms)
                   true
                   (catch InterruptedException _
                     (.interrupt (Thread/currentThread))
                     false))
                 true)]
    (when slept?
      (doseq [[k v] (get spec "headers")]
        (.set (.getResponseHeaders exchange) ^String k ^String v))
      ;; -1 sends content-length: 0; 0 would switch to chunked encoding.
      (.sendResponseHeaders exchange (long (get spec "status"))
                            (if (zero? (alength payload)) -1 (alength payload)))
      (try
        (with-open [out (.getResponseBody exchange)]
          (.write out payload))
        (catch IOException _
          ;; The client already gave up and closed the connection.
          nil)))))

(defn start!
  "Start a real HTTP server answering every request with the vector gateway
  `spec`, recording requests in `rec`. Returns `{:base-url :close}`; call
  `close` (a function) when done: it drops in-flight connections rather than
  draining them."
  [spec rec]
  (let [payload (body-bytes (get spec "body"))
        ^ExecutorService executor (Executors/newCachedThreadPool)
        server (HttpServer/create (InetSocketAddress. (InetAddress/getLoopbackAddress) 0) 0)]
    (.setExecutor server executor)
    (.createContext server "/" (reify HttpHandler
                                 (handle [_ exchange] (answer exchange spec payload rec))))
    (.start server)
    (let [bound (.getAddress server)
          host (.getHostAddress (.getAddress bound))]
      {:base-url (str "http://" (if (str/includes? host ":") (str "[" host "]") host)
                      ":" (.getPort bound))
       :close (fn []
                (.stop server 0)
                (.shutdownNow executor))})))
