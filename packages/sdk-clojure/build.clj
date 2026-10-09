(ns build
  "Builds and publishes the jar: `clojure -T:build jar`, then
  `clojure -T:build deploy` (needs CLOJARS_USERNAME and CLOJARS_PASSWORD)."
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(set! *warn-on-reflection* true)

;; Column-0 on purpose: .mise/lib/pkg.sh and .mise/lib/release.exs read these
;; two forms with regexes.
(def lib 'io.github.jamescarr/ankusa-clj)
(def version "0.0.0")

(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean
  "Delete the build output."
  [_]
  (b/delete {:path "target"}))

(defn jar
  "Build `target/<artifact>-<version>.jar` and its POM."
  [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis (b/create-basis {:project "deps.edn"})
                :src-dirs ["src"]
                :scm {:url "https://github.com/jamescarr/ankusa"
                      :connection "scm:git:https://github.com/jamescarr/ankusa.git"
                      :developerConnection "scm:git:ssh://git@github.com/jamescarr/ankusa.git"
                      :tag (str "sdk-clojure-v" version)}
                :pom-data [[:description "Clojure client for Ankusa: decode queue messages, redeem claim checks, parse HTTP-sink headers, and drive the routes/admin/sources APIs."]
                           [:url "https://github.com/jamescarr/ankusa"]
                           [:licenses
                            [:license
                             [:name "Apache-2.0"]
                             [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]})
  (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
  (b/jar {:class-dir class-dir :jar-file jar-file}))

(defn deploy
  "Upload the jar `jar` built to Clojars. It never rebuilds, so the jar that was
  attested is the jar that is uploaded."
  [_]
  (when-not (.exists (java.io.File. ^String jar-file))
    (throw (ex-info "run clojure -T:build jar first" {:jar jar-file})))
  (dd/deploy {:installer :remote
              :artifact (b/resolve-path jar-file)
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))
