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
                      (select-keys d [:year indicator-field])))
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
                (assoc :indicator_id indicator-id))) data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Internal calculations                                                                ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn sum-sequence [data k]
  (let [timestamp    (-> data first :year)]
    {:year timestamp
     :sum (->> (map k data)
               (map transform/parse-number)
               (apply +))}))

(defn divide-seqs [d1 d2]
  (let [timestamp (:year d1)]
    {:year timestamp
     :division-result (/ (:sum d1) (:sum d2))}))

(defn subtract-seqs [d]
  (let [timestamp (:year d)]
    {:year timestamp
     :result (str (- (:division-result d) (/ (transform/parse-number (:indicator_value d)) 100)))}))

(defn split-by-key [k data]
  (->> (group-by k data)
       vals))

(defn patient-experience-of-gp-services
  "Patient experience of primary care - GP Services.
  White British compared to Asian or Asian British."
  [recipe data]
  (let [numerators           (filter-dataset (:numerators recipe) data)
        denominators         (filter-dataset (:denominators recipe) data)
        indicator-values     (filter-dataset (:indicator-values recipe) data)
        numerators-by-year   (split-by-key :year numerators)
        denominators-by-year (split-by-key :year denominators)
        numerator-sums       (map #(sum-sequence % :numerator) numerators-by-year)
        denominator-sums     (map #(sum-sequence % :denominator) denominators-by-year)
        division-result      (map divide-seqs numerator-sums denominator-sums)
        combined-data        (lazy-seq (clojure.set/join division-result indicator-values))
        final-dataset        (map subtract-seqs combined-data)]

    (enrich-dataset {:indicator-id (:indicator-id recipe)
                     :indicator-field :result}
                    final-dataset)))

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


(defn read-config
  "Reads the config file and returns it as a string."
  [url]
  (-> (slurp url) edn/read-string))

(defn create-boardreport-dataset
  "Creates a sequence of maps containing the info
  needed for the board report."
  [ckan-client config-url]
  (let [config (read-config config-url)]
    (lazy-cat (process-patient-experience-recipes ckan-client (:internal-calculations config))
              (mapcat (fn [dataset-config]
                        (read-dataset ckan-client dataset-config
                                      (:resource-id dataset-config)))
                      (:datasets config)))))

(defn insert-boardreport-dataset
  "Calls create-boardreport-dataset and insert new
  dataset into ckan."
  [ckan-client config-url]
  (let [now             (now->str)
        new-dataset     (json/encode {:owner_org "kixi"
                                      :title (str "Board report data" " - " now)
                                      :name (str "board_report_data" "_" now)
                                      :author "Kixi"})
        new-dataset-id  (storage/create-new-dataset ckan-client new-dataset)
        new-resource    (json/encode {:package_id new-dataset-id
                                      :url "http://fix-me" ;; url is mandatory
                                      :description "Board report resource"})
        new-resource-id (storage/create-new-resource ckan-client new-dataset-id new-resource)
        records         (create-boardreport-dataset ckan-client config-url)
        fields          [{"id" "indicator_id" "type" "text"}
                         {"id" "value" "type" "text"}
                         {"id" "year" "type" "text"}]
        data            (data/prepare-resource-for-insert new-dataset-id new-resource-id
                                                          {"records" records
                                                           "fields"  fields})]
    (storage/insert-new-resource ckan-client new-dataset-id data)))


;; Run:
;; (insert-boardreport-dataset (:ckan-client system) "resources/config.edn")
