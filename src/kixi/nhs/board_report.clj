(ns kixi.nhs.board-report
  (:require [kixi.nhs.data.storage :as storage]
            [clojure.tools.logging :as log]
            [clojure.edn           :as edn]
            [clj-time.core         :as t]
            [clj-time.format       :as tf]
            [cheshire.core         :as json]
            [kixi.ckan.data        :as data]
            [kixi.nhs.data.transform :as transform]))

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

(defn filter-dataset
  "Filters dataset according to the given recipe."
  [recipe-map data]
  (let [{:keys [indicator-field conditions indicator-id]} recipe-map]
    ;; Go through the data sequence and 1.Check indicator field,
    ;; 2.Check the conditions, 3.Keep "Year" and indicator value.
    (when (contains? (first data) indicator-field)
      (keep (fn [d] (when (every? (fn [condition] (let [{:keys [field values]} condition]
                                                    ;; values is a set
                                                    (some values #{(get d field)})))
                                  conditions)
                      (select-keys d [:year :period_of_coverage indicator-field])))
            data))))

(defn enrich-dataset
  "Enrichs dataset with indicator-id."
  [recipe-map data]
  (let [{:keys [indicator-field indicator-id]} recipe-map]
    ;; Go through the data sequence and 1.Change the key map indicator-field
    ;; for value, 2.Add the indicator-id.
    (mapv (fn [d]
            (-> d
                (clojure.set/rename-keys {indicator-field :value})
                ;; period_of_coverage is a PK so cannot be null. Using year if it's empty
                (cond-> (empty? (:period_of_coverage d)) (assoc :period_of_coverage (:year d)))
                (assoc :indicator_id indicator-id))) data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Internal calculations                                                                ;;
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
     :result (when (and (not-nil? division-result) (not-nil? indicator_value))
               (str (float (- division-result (/ (transform/parse-number indicator_value) 100)))))}))

(defn split-by-key [k data]
  (->> (group-by k data)
       vals))

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
       (enrich-dataset {:indicator-id indicator-id
                        :indicator-field :result})))

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

(defn process-patient-experience-recipes [ckan-client recipes]
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
       (enrich-dataset recipe-map)))


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
  (let [config (read-config config-url)]
    (concat (process-patient-experience-recipes ckan-client (:internal-calculations config))
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
        fields          [{"id" "indicator_id" "type" "text"}
                         {"id" "value" "type" "text"}
                         {"id" "year" "type" "text"}
                         {"id" "period_of_coverage" "type" "text"}]
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
