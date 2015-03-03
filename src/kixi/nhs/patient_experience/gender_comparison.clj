(ns kixi.nhs.patient-experience.gender-comparison
  "Patient experience of primary care - GP Services - Gender comparison."
  (:require [kixi.nhs.data.transform :as transform]
            [kixi.nhs.data.storage   :as storage]))

(defn subtract-males-from-females
  "Takes away the indicator value for males
  from the indicator value for females for each
  year."
  [data]
  (let [female (->> (filter #(= (:level %) "Female") data) first :indicator_value transform/parse-number)
        male   (->> (filter #(= (:level %) "Male") data) first :indicator_value transform/parse-number)
        {:keys [year period_of_coverage]} (first data)]
    {:year year
     :period_of_coverage period_of_coverage
     :value (when (and (transform/not-nil? female)
                       (transform/not-nil? male))
              (str (- female male)))}))

(defn gender-analysis
  "Filter data according to the recipe, splits
  by year and calculates indicator for each year."
  [recipe data]
  (->> (transform/filter-dataset recipe data)
       (remove #(= (:level %) "Unknown"))
       (transform/split-by-key :year)
       (map subtract-males-from-females)
       (map #(assoc % :level "Female - Male" :breakdown "Gender"))
       (transform/enrich-dataset recipe)))

(defn process-gender-analysis
  "Retrieves resource's data and performs analysis.
  Returns a sequence that can be combined with the
  rest of board report resource."
  [ckan-client recipe]
  (let  [resource_id (:resource-id recipe)
         data        (storage/get-resource-data ckan-client resource_id)]
    (gender-analysis recipe data)))

(defn analysis
  "Receives a sequence of gender analysis recipes.
  Returns a sequences of all results from those recipes combined."
  [ckan-client recipes]
  (mapcat #(process-gender-analysis ckan-client %) recipes))
