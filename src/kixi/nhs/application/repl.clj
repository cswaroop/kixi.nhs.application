(ns kixi.nhs.application.repl
  "Useful functions for interacting with the pipeline from the repl."
  (:require [kixipipe.scheduler       :as s]
            [kixipipe.pipeline        :refer [submit-item shutdown-pipe]]
            [modular                  :refer (system)]))

(defmacro defreplmethods
  [name & options]
  `(let [options# (apply hash-map '~options)]
     (defn
       ~name
       ([~'type] (submit-item (-> system :pipeline :head)
                    (merge {:type ~'type}
                           options#)))
       ([] (~name nil)))))

(defreplmethods merge-datasets :dest :combined-datasets :type :merged)
