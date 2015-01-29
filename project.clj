(defproject kixi.nhs.application "0.1.0-SNAPSHOT"
  :description "NHS dashbaord data transformation and storage."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :source-paths ["src"]

  :dependencies [[org.clojure/clojure        "1.6.0"]

                 [com.stuartsierra/component "0.2.2"]

                 [kixi/pipe                  "0.17.12"]
                 [kixi/ckan                  "0.1.1-SNAPSHOT"]

                 ;; data
                 [cheshire                   "5.4.0"]

                 ;; logging
                 [org.clojure/tools.logging  "0.3.0"]

                 [clj-time                   "0.9.0"]]

  :min-lein-version "2.5.0"
  :uberjar-name "kixi-nhs-application-%s.jar"
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.namespace "0.2.8"]]}
             :uberjar {:main kixi.nhs.application.main
                       :aot [kixi.nhs.application.main]}})
