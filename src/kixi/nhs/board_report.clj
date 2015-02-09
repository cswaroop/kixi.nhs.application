(ns kixi.nhs.board-report
  (:require [kixi.nhs.data.storage   :as storage]
            [clojure.tools.logging   :as log]
            [clojure.edn             :as edn]
            [clj-time.core           :as t]
            [clj-time.format         :as tf]
            [cheshire.core           :as json]
            [kixi.ckan.data          :as data]
            [kixi.nhs.data.transform :as transform]
            [incanter.stats          :as stats]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers                                                                              ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def custom-formatter (tf/formatter "yyyyMMddHHmmss"))

(defn now->str
  "Formats the current timestamp into a string
  with date and time."
  []
  (let [now (t/now)]
    (tf/unparse custom-formatter now)))

(defn not-nil? [x] (not (nil? x)))

(defn all-fields-exist? [fields row]
  (let [headers (into #{} (keys row))]
    (every? #(contains? headers %) fields)))

(defn filter-dataset
  "Filters dataset according to the given recipe."
  [recipe-map data]
  (let [{:keys [conditions indicator-id fields-to-extract]} recipe-map]
    (when (all-fields-exist? fields-to-extract (first data))
      (keep (fn [d] (when (every? (fn [condition] (let [{:keys [field values]} condition]
                                                    ;; values is a set
                                                    (some values #{(get d field)})))
                                  conditions)
                      (select-keys d fields-to-extract)))
            data))))

(defn enrich-dataset
  "Enrichs dataset with indicator-id."
  [recipe-map data]
  (let [{:keys [indicator-id]} recipe-map]
    (mapv (fn [d]
            (-> d
                ;; period_of_coverage is a PK so cannot be null. Using year if it's empty
                (cond-> (empty? (:period_of_coverage d)) (assoc :period_of_coverage (:year d)))
                (assoc :indicator_id indicator-id))) data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Internal calculations - indicator: 212                                               ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn split-by-key
  "Turns a sequence of maps into a sequence of sequences,
  where each nested sequence corresponds to a single group."
  [k data]
  (->> (group-by k data)
       vals))

(defn get-slope [y]
  (let [{:keys [year period_of_coverage]} (first y)
        x [0.1 0.3 0.5 0.7 0.9]
        [_ slope] (:coefs (stats/linear-model (map :indicator_value y) x))]
    {:year year
     :period_of_coverage period_of_coverage
     :slope slope}))

(defn average
  "Calculates average of a collection."
  [coll]
  (float (/ (reduce + coll) (count coll))))

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
                       :indicator_value (average (->> (map :indicator_value level-partition)
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
  and the same value from § years previously to the latest year.
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
  "Calculates indicators 212 and 212a"
  [data]
  (let [deprivation-groups-median   (mapcat get-median-indicators data)
        slopes                      (map get-slope data)
        joined                      (into [] (clojure.set/join deprivation-groups-median slopes))
        slope-median-division       (map divide-slope-by-median joined)
        ;; most recent value
        {:keys [division year period_of_coverage]} (get-year-x 0 (sort-by :year slope-median-division))]

    [{:indicator_id "212"  :year year :period_of_coverage period_of_coverage
      :value (str (deprivation-year-1 division slope-median-division))}
     {:indicator_id "212a" :year year :period_of_coverage period_of_coverage
      :value (str (deprivation-year-5 division slope-median-division))}]))

(defn process-deprivation-analysis [ckan-client recipe-map]
  (let [resource_id (:resource-id recipe-map)
        data        (storage/get-resource-data ckan-client resource_id)]
    (->> (filter-dataset recipe-map data)
         (split-by-key :year)
         (map deprivation-groups-avg)
         (deprivation-analysis))))

(defn patient-experience-deprivation-analysis [ckan-client recipes]
  (mapcat #(process-deprivation-analysis ckan-client %) recipes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Internal calculations - indicators: 213, 214, 215, 216, 217                          ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn add-when-not-empty
  "Sums values in a sequence if it's not empty.
  Otherwise returns nil."
  [data]
  (when (seq data)
    (apply + (map transform/parse-number data))))

(defn sum-sequence [data k]
  (let [{:keys [year period_of_coverage]} (first data)]
    {:year year :period_of_coverage period_of_coverage
     :sum (->> (map k data)
               (remove #(not (seq %)))
               add-when-not-empty)}))

(defn divide-seqs [d1 d2]
  (let [{:keys [year period_of_coverage]} d1
        sum1 (:sum d1)
        sum2 (:sum d2)]
    {:year year :period_of_coverage period_of_coverage
     :division-result (when (and (not-nil? sum1) (not-nil? sum2))
                        (float (/ (:sum d1) (:sum d2))))}))

(defn subtract-seqs [d]
  (let [{:keys [year period_of_coverage division-result indicator_value]} d]
    {:year year :period_of_coverage period_of_coverage
     :value (when (and (not-nil? division-result) (not-nil? indicator_value))
              (str (float (- division-result (/ (transform/parse-number indicator_value) 100)))))}))

(defn sums-for-field [field k data]
  (->> (filter-dataset field data)
       (split-by-key :year)
       (map #(sum-sequence % k))))

(defn divide-sums [numerator-sums denominator-sums]
  (map divide-seqs numerator-sums denominator-sums))

(defn final-dataset [indicator-id numerator-sums denominator-sums indicator-values]
  (->> (divide-sums numerator-sums denominator-sums)
       (clojure.set/join indicator-values)
       (map subtract-seqs)
       (enrich-dataset {:indicator-id indicator-id})))

(defn patient-experience-of-gp-services
  "Patient experience of primary care - GP Services.
  White British compared to Asian or Asian British."
  [recipe data]
  (let [indicator-values     (filter-dataset (:indicator-values recipe) data)
        numerator-sums       (sums-for-field (:numerators recipe) :numerator data)
        denominator-sums     (sums-for-field (:denominators recipe) :denominator data)]

    (final-dataset (:indicator-id recipe) numerator-sums denominator-sums indicator-values)))

(defn patient-experience [ckan-client recipe]
  (let  [resource_id (:resource-id recipe)
         data        (storage/get-resource-data ckan-client resource_id)]
    (patient-experience-of-gp-services recipe data)))

(defn patient-experience-ethnicity-analysis [ckan-client recipes]
  (mapcat #(patient-experience ckan-client %) recipes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Simple datasets                                                                      ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn read-dataset
  "Reads data from CKAN for a given resource-id,
  filters on conditions and outputs a vector of
  maps where each map is enriched with indicator-id."
  [ckan-client recipe-map resource_id]
  (->> (storage/get-resource-data ckan-client resource_id)
       (filter-dataset recipe-map)
       (enrich-dataset recipe-map)
       (map #(clojure.set/rename-keys % {:indicator_value :value :indicator_value_rate :value}))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Process all recipes and update board report resource                                 ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn read-config
  "Reads the config file and returns it as a string."
  [url]
  (-> (slurp url) edn/read-string))

(defn create-boardreport-dataset
  "Creates a sequence of maps containing the info
  needed for the board report."
  [ckan-client config-url]
  (let [config                (read-config config-url)
        internal-calculations (:internal-calculations config)]
    (concat (patient-experience-ethnicity-analysis ckan-client (:enthicity internal-calculations))
            (patient-experience-deprivation-analysis ckan-client (:deprivation internal-calculations))
            (mapcat (fn [dataset-config]
                      (read-dataset ckan-client dataset-config
                                    (:resource-id dataset-config)))
                    (:datasets config)))))

(defn insert-board-report-dataset
  "Calls create-boardreport-dataset and insert new
  dataset into ckan."
  [ckan-client config-url]
  (let [now             (now->str)
        new-dataset     (json/encode {:owner_org "kixi"
                                      :title (str "Board report data TEST")
                                      :name (str "board_report_dataset_test")
                                      :author "Kixi"})
        new-dataset-id  (storage/create-new-dataset ckan-client new-dataset)
        new-resource    (json/encode {:package_id new-dataset-id
                                      :url "http://fix-me" ;; url is mandatory
                                      :description "Board report resource"})
        new-resource-id (storage/create-new-resource ckan-client new-dataset-id new-resource)
        records         (create-boardreport-dataset ckan-client config-url)
        fields          [{"id" "indicator_id" "type" "text"}
                         {"id" "value" "type" "text"}
                         {"id" "year" "type" "text"}
                         {"id" "period_of_coverage" "type" "text"}]
        data            (data/prepare-resource-for-insert new-dataset-id new-resource-id
                                                          {"records" records
                                                           "fields"  fields
                                                           "primary_key" "indicator_id,year,period_of_coverage"})]
    (storage/insert-new-resource ckan-client new-dataset-id data)))


(defn update-board-report-dataset
  "Update existing rows in the table and append any new ones. Primary key is:
  (indicator_id, year, period_of_coverage)"
  [ckan-client resource-id config-url]
  (let [records         (create-boardreport-dataset ckan-client config-url)
        data            (json/encode {"records" records
                                      "method" "upsert"
                                      "force" true
                                      "resource_id" resource-id})]

    (storage/update-existing-resource ckan-client resource-id data)))

;; To insert new board report resource:
;; (insert-board-report-dataset (:ckan-client system) "resources/config.edn")
;; To update existing board resource (preferrable):
;; TEST
;; (update-board-report-dataset (:ckan-client system) "68d5438a-e4d3-4be0-8e34-3ccd40930dae" "resources/config.edn")
;; USED BY UI:
;; (update-board-report-dataset (:ckan-client system) "ed59dfc4-3076-4e84-806e-7a47d2321f36" "resources/config.edn")
