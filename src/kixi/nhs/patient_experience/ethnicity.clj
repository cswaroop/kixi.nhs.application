(ns kixi.nhs.patient-experience.ethnicity
  "Patient experience of primary care - GP Services - White British compared to
  213: Asian or Asian British
  214: Black or Black British
  215: Other Ethnic Group
  216: Mixed
  217: Other White ethnicity."
  (:require [kixi.nhs.data.transform :as transform]
            [kixi.nhs.data.storage   :as storage]))

(defn divide-maps
  "Gets two maps, divides their entries for key :sum
  and returns a new map with :division-result containing
  that result."
  [d1 d2]
  (let [{:keys [year period_of_coverage]} d1
        sum1 (:sum d1)
        sum2 (:sum d2)]
    {:year year :period_of_coverage period_of_coverage
     :division-result (when (and (transform/not-nil? sum1) (transform/not-nil? sum2))
                        (float (/ sum1 sum2)))}))

(defn subtract-indicator-value
  "Gets a map with division result and indicator value and
  subtracts the latter from the former. Returns a new map with
  the value key containing the result."
  [d]
  (let [{:keys [year period_of_coverage division-result indicator_value]} d]
    {:year year :period_of_coverage period_of_coverage
     :value (when (and (transform/not-nil? division-result) (transform/not-nil? indicator_value))
              (str (float (- division-result (/ (transform/parse-number indicator_value) 100)))))}))

(defn sums-for-field
  "Gets a field name by which it filters the data,
  key for which id adds the values and a sequence of
  maps that it works on. Returns a sequence of maps
  with the entries summed up."
  [field k data]
  (->> (transform/filter-dataset field data)
       (transform/split-by-key :year)
       (map #(transform/sum-sequence k [:numerator :denominator] %))))

(defn divide-sums
  "Gets a sequence of numerator sums and denominator sums
  and divides one by another. Returns a sequence of results."
  [numerator-sums denominator-sums]
  (map divide-maps numerator-sums denominator-sums))

(defn final-dataset
  "Performs calculations and creates a final form of the
  resulting dataset. Enriches each map with indicator id."
  [indicator-id numerator-sums denominator-sums indicator-values]
  (->> (divide-sums numerator-sums denominator-sums)
       (clojure.set/join indicator-values)
       (map subtract-indicator-value)
       (map #(assoc % :level (str "\""(-> numerator-sums first :level)
                                  "\" / \""
                                  (-> indicator-values first :level) "\"")
                    :breakdown (-> indicator-values first :breakdown)))
       (transform/enrich-dataset {:indicator-id indicator-id})))

(defn ethnicity-analysis
  "Patient experience of primary care - GP Services using
  ethnicities factor."
  [recipe data]
  (let [indicator-values     (transform/filter-dataset (:indicator-values recipe) data)
        numerator-sums       (sums-for-field (:numerators recipe) :numerator data)
        denominator-sums     (sums-for-field (:denominators recipe) :denominator data)]

    (final-dataset (:indicator-id recipe) numerator-sums denominator-sums indicator-values)))

(defn process-ethnicity-analysis [ckan-client recipe]
  (let  [resource_id (:resource-id recipe)
         data        (storage/get-resource-data ckan-client resource_id)]
    (ethnicity-analysis recipe data)))

(defn analysis [ckan-client recipes]
  (mapcat #(process-ethnicity-analysis ckan-client %) recipes))
