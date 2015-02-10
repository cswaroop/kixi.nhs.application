(ns kixi.nhs.patient-experience.deprivation
  "Patient experience of primary care - GP Services - Deprivation analysis."
  (:require [incanter.stats          :as stats]
            [kixi.nhs.data.transform :as transform]
            [kixi.nhs.data.storage   :as storage]))

(defn slope [y]
  (let [{:keys [year period_of_coverage]} (first y)
        x [0.1 0.3 0.5 0.7 0.9]
        [_ slope] (:coefs (stats/linear-model (map :indicator_value y) x))]
    {:year year
     :period_of_coverage period_of_coverage
     :slope slope}))

(defn deprivation-groups-avg
  "Average the deprivation values two by two so that you end up with 5 deprivation groups.
  i.e. average the values for deprivation groups 1 and 2 to get a new value for a new
  deprivation group 1. Then average the values for deprivation groups 3 and 4 to get a valu
  e for a new deprivation group 2 etc until averaging values for deprivation groups 9 and 10
  to get a value for a new deprivation group 5. then proceed with the steps."
  [data]
  (->> (remove #(= (:level %) "Unknown") data)
       (map #(update-in % [:level] transform/parse-number))
       (sort-by :level)
       (partition 2)
       (map-indexed (fn [idx level-partition]
                      {:level (inc idx)
                       :period_of_coverage (-> level-partition first :period_of_coverage)
                       :year (-> level-partition first :year)
                       :indicator_value (stats/mean (->> (map :indicator_value level-partition)
                                                         (map transform/parse-number)))}))))
(defn get-year-x
  "Filter value for the latest date minus x years."
  [x data]
  (let [sorted (sort-by :year data)]
    (if (< x (count data))
      (->> sorted (drop-last x) last)
      (first sorted))))

(defn get-median-indicators
  "Returns a sequence of indicators for deprivation group 3
  for all years."
  [data]
  (keep #(when (= (:level %) 3)
           {:year (:year %)
            :period_of_coverage (:period_of_coverage %)
            :median_indicator_value (:indicator_value %)}) data))

(defn divide-slope-by-median
  "Divide the slope value found in step 2 by the indicator value of deprivation group 3."
  [data]
  (assoc data :division (float (/ (:slope data) (:median_indicator_value data)))))

(defn deprivation-year-1
  "Calculate the difference between the latest value calculated on step 3
  and the same value from 1 year previously to the latest year.
  Divide this difference by the value calculated on step 3 for
  1 year previously to the latest year"
  [most-recent-value data]
  (let [division-1-year (:division (get-year-x 1 data))]
    (float (/ (- most-recent-value
                 division-1-year)
              division-1-year))))

(defn deprivation-year-5
  "Calculate the difference between the latest value calculated on step 3
  and the same value from 5 years previously to the latest year.
  Divide this difference by the value calculated on step 3 for
  five years previously to the latest year"
  [most-recent-value data]
  (let [division-5-years (:division (get-year-x 5 data))]
    (float (/ (- most-recent-value
                 division-5-years)
              division-5-years))))

(defn deprivation-analysis
  "Calculates indicators 212 and 212a."
  [data]
  (let [deprivation-groups-median   (mapcat get-median-indicators data)
        slopes                      (map slope data)
        joined                      (into [] (clojure.set/join deprivation-groups-median slopes))
        slope-median-division       (map divide-slope-by-median joined)
        ;; most recent value
        {:keys [division year period_of_coverage]} (get-year-x 0 (sort-by :year slope-median-division))]

    [{:indicator_id "212"  :year year :period_of_coverage period_of_coverage
      :value (str (deprivation-year-1 division slope-median-division))}
     {:indicator_id "212a" :year year :period_of_coverage period_of_coverage
      :value (str (deprivation-year-5 division slope-median-division))}]))

(defn process-deprivation-analysis
  "Retrieves data from CKAN, filters it out according to conditions
  in the recipe, performs deprivation analysis. Returns a sequence of two
  indicators with values for 5 and 1 years from the latest entry."
  [ckan-client recipe-map]
  (let [resource_id (:resource-id recipe-map)
        data        (storage/get-resource-data ckan-client resource_id)]
    (->> (transform/filter-dataset recipe-map data)
         (transform/split-by-key :year)
         (map deprivation-groups-avg)
         (deprivation-analysis))))

(defn patient-experience-deprivation-analysis
  "Receives a sequence of deprivation recipes.
  Returns a sequences of all results from those recipes combined."
  [ckan-client recipes]
  (mapcat #(process-deprivation-analysis ckan-client %) recipes))
