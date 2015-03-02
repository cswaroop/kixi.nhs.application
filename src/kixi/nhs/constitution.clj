(ns kixi.nhs.constitution
  (:require [kixi.nhs.xls  :as xls]))



(defn process-recipe [ckan-client recipe]
  (let [data (xls/process-xls ckan-client recipe)]
    data
    ;; TODO calculate indicator values
    ))

(defn analysis [ckan-client recipes]
  (mapcat #(process-recipe ckan-client %) recipes))
