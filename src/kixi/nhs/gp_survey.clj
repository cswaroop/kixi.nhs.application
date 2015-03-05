(ns kixi.nhs.gp-survey
  (:require [kixi.nhs.xls :as xls]
            [kixi.nhs.data.transform :as transform]))

(defn sum
  "Add to cells."
  [data]
  (-> data
      first
      (select-keys [:overall_experience_of_making_an_appointment_very_good_percentage
                    :overall_experience_of_making_an_appointment_fairly_good_percentage])
      vals
      transform/add-when-not-empty))

(defn access-to-gp-services
  "Retrieves GP Survey results and
  returns sum of columns:
  :overall_experience_of_making_an_appointment_very_poor
  :overall_experience_of_making_an_appointment_very_good_percentage."
  [ckan-client recipe]
  (let [field (:field recipe)]
    (->> (xls/process-xls ckan-client recipe)
         first ;; we just work on a single worksheet
         (transform/filter-dataset recipe)
         sum
         str
         (assoc {} :value)
         (conj [])
         (transform/enrich-dataset recipe))))
