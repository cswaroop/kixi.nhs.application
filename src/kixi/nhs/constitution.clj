(ns kixi.nhs.constitution
  (:require [kixi.nhs.data.transform :as transform]
            [kixi.nhs.data.storage   :as storage]))

(defn total
  [k data]
  (->> data
       (map k)
       (keep #(when-not (empty? %)
               (Integer/parseInt (clojure.string/replace % #"," ""))))
       (apply +)))

(defn percentage-seen-within-14-days
  "Calculates percentage seen within 14 days.
  Returns a map with the result, area team code,
  year and period of coverage."
  [k v data]
  {k v
   :year "2014/2015" :period_of_coverage "q1"
   :value (float (/ (total :within_14_days data)
                    (total :total data)))})

(defn per-team-area
  "Splits data by area team code and calculates
  the percentage of patients seen within 14 days.
  Returns a sequence of maps."
  [data]
  (->> data
       (transform/split-by-key :area_team_code_1)
       (map #(percentage-seen-within-14-days :area_team_code (:area_team_code_1 (first %)) %))))

(defn per-region
  "Returns total for region (England),
  sums up data for all CCGs."
  [data]
  (percentage-seen-within-14-days :region "England" data))

(defn scrub
  "Removes empty rows from the data,
  or the rows that do not contain
  required information."
  [data]
  (->> data
       (remove #(empty? (:area_team_code_1 %)))))

(defn process-recipe [ckan-client recipe]
  (let [data           (scrub (storage/get-resource-data ckan-client (:resource-id recipe)))
        region-data    (per-region data)
        team-area-data (per-team-area data)]
    (->> (conj team-area-data
               region-data)
         (transform/enrich-dataset recipe))))

(defn analysis [ckan-client recipes]
  (mapcat #(process-recipe ckan-client %) recipes))
