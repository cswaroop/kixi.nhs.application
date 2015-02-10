(ns kixi.nhs.application.repl
  "Useful functions for interacting with the pipeline from the repl."
  (:require [kixipipe.scheduler       :as s]
            [kixipipe.pipeline        :refer [submit-item shutdown-pipe]]
            [user                     :refer (system)]))

(defmacro defreplmethods
  [name & options]
  `(let [options# (apply hash-map '~options)]
     (defn
       ~name
       ([~'resource-id] (submit-item (-> system :pipeline :head)
                                     (assoc options# :resource-id ~'resource-id)))
       ([] (~name nil)))))

(defreplmethods update-board-report :dest :board-report :type :update)
(defreplmethods insert-board-report :dest :board-report :type :insert)
