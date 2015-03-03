(ns kixi.nhs.friends-and-family
  (:require [kixi.nhs.xls :as xls]
            [kixi.nhs.data.transform :as transform]))


(defn process-friends-and-family
  "Retrieves Friends & Family Test value
  for England, including Independent
  Sector Providers."
  [ckan-client recipe]
  (let [field (:field recipe)]
    (->> (xls/process-xls ckan-client recipe)
         first
         (transform/filter-dataset recipe)
         (transform/enrich-dataset recipe)
         (map #(-> %
                   (dissoc :area-team)
                   (update-in [field] str)
                   (clojure.set/rename-keys {field :value}))))))

(defn analysis
  "Receives a sequence of F&F recipes.
  Returns a sequences of all results from those recipes combined."
  [ckan-client recipes]
  (mapcat #(process-friends-and-family ckan-client %) recipes))
