(ns kixi.nhs.application
  (:require [kixi.ckan                                 :refer (new-ckan-client-session)]
            [kixi.nhs.application.pipeline             :refer (new-pipeline)]
            [com.stuartsierra.component                :as component]
            [clojure.tools.logging                     :as log]
            [clojure.pprint                            :refer (pprint)]
            [clojure.tools.reader.reader-types         :refer (indexing-push-back-reader
                                                               source-logging-push-back-reader)]
            [clojure.java.io                           :as io]
            kixipipe.scheduler
            clojure.tools.reader))

(defn combine
  "Merge maps, recursively merging nested maps whose keys collide."
  ([] {})
  ([m] m)
  ([m1 m2]
   (reduce (fn [m1 [k2 v2]]
             (if-let [v1 (get m1 k2)]
               (if (and (map? v1) (map? v2))
                 (assoc m1 k2 (combine v1 v2))
                 (assoc m1 k2 v2))
               (assoc m1 k2 v2)))
           m1 m2))
  ([m1 m2 & more]
   (apply combine (combine m1 m2) more)))

(defn config []
  (let [f (io/file (System/getProperty "user.home") ".nhs.edn")]
    (when (.exists f)
      (combine
       (clojure.tools.reader/read
        (indexing-push-back-reader
         (java.io.PushbackReader. (io/reader "resources/default.nhs.edn"))))
       (clojure.tools.reader/read
        (indexing-push-back-reader
         (java.io.PushbackReader. (io/reader f))))))))

(defn new-system []
  (let [cfg (config)]
    (-> (component/system-map
         :ckan-client (new-ckan-client-session (:ckan-client cfg))
         :pipeline    (new-pipeline)
         :scheduler   (kixipipe.scheduler/mk-session cfg))
        (component/system-using
         {:pipeline  {:ckan-client :ckan-client}
          :scheduler {:pipeline :pipeline}}))))
