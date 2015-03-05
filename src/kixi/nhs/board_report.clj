(ns kixi.nhs.board-report
  (:require [kixi.nhs.data.storage                         :as storage]
            [clojure.tools.logging                         :as log]
            [clojure.edn                                   :as edn]
            [cheshire.core                                 :as json]
            [kixi.ckan.data                                :as data]
            [kixi.nhs.data.transform                       :as transform]
            [kixi.nhs.patient-experience.deprivation       :as deprivation]
            [kixi.nhs.patient-experience.ethnicity         :as ethnicity]
            [kixi.nhs.patient-experience.gender-comparison :as gender]
            [clj-time.format                               :as tf]
            [clj-time.core                                 :as t]
            [clj-time.coerce                               :as tc]
            [kixi.nhs.constitution                         :as constitution]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Indicator 65 & 66: Incidence of MRSA and C difficile                                 ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO this formatter will be changed when bug in CKAN is fixed and data re-uploaded
(def custom-formatter (tf/formatter "yyyy-MM-dd'T'HH:mm:ss"))

(defn latest-month
  "Finds the latest month in data.
  It looks for a date using provided key.
  Returns a datetime."
  [k data]
  (->> data
       (map #(tf/parse custom-formatter (k %)))
       t/latest))

(defn enrich
  "Use latest month timestamp to enrich the map
  with year and period_of_coverage which are PKs in
  datastore.
  Stringifies the sum value."
  [timestamp m]
  (-> m
      (assoc :year (str (t/year timestamp))
             :period_of_coverage (tf/unparse custom-formatter timestamp))
      (update-in [:sum] str)))

(defn incidence
  "Reads data from CKAN for a given resource_id,
  filters data for latest month, sums up values for all
  CCGs.
  Returns a sequence of maps."
  [ckan-client recipe-map]
  (let [data           (storage/get-resource-data ckan-client (:resource-id recipe-map))]
    (when (seq data)
      (let [timestamp      (latest-month :reporting_period data)
            updated-recipe (update-in recipe-map [:conditions] conj {:field :reporting_period
                                                                     :values #{(tf/unparse custom-formatter timestamp)}})]
        (->> data
             (transform/filter-dataset updated-recipe)
             (transform/sum-sequence (:sum-field recipe-map)) ;; returns a single map since it's summing up all maps
             (enrich timestamp) ;; this dataset has uncommon field names, updating it to match others
             (conj []) ;; dataset returned should be a sequence of maps
             (transform/enrich-dataset recipe-map)
             (map #(clojure.set/rename-keys % {:sum :value})))))))

(defn incidence-datasets [ckan-client recipes]
  (mapcat #(incidence ckan-client %) recipes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Idicator 57: Bereaved carers' views on the quality of care                           ;;
;;              in the last 3 months of life                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn end-of-life-care
  "Reads data from CKAN for a given resource_id,
  filters on conditions, sums up three indicator
  values with reference to Outstanding, Excellent and Good
  (that are already filtered) and returns a sequence of
  results for each period."
  [ckan-client recipe-map]
  (->> (storage/get-resource-data ckan-client (:resource-id recipe-map))
       (transform/filter-dataset recipe-map)
       (transform/split-by-key :period_of_coverage)
       (map #(transform/sum-sequence :indicator_value [:question_response :indicator_value] %))
       (map #(update-in % [:sum] str))
       (transform/enrich-dataset recipe-map)
       (map #(clojure.set/rename-keys % {:sum :value}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Simple datasets                                                                      ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn read-dataset
  "Reads data from CKAN for a given resource-id,
  filters on conditions and outputs a vector of
  maps where each map is enriched with indicator-id."
  [ckan-client recipe-map resource_id]
  (->> (storage/get-resource-data ckan-client resource_id)
       (transform/filter-dataset recipe-map)
       (transform/enrich-dataset recipe-map)
       (map #(clojure.set/rename-keys % {:indicator_value :value :indicator_value_rate :value
                                         :average_health_gain :value}))))


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
    (concat (ethnicity/analysis ckan-client (:ethnicity internal-calculations))
            (deprivation/analysis ckan-client (:deprivation internal-calculations))
            (gender/analysis ckan-client (:gender internal-calculations))
            (end-of-life-care ckan-client (:end-of-life-care internal-calculations))
            (incidence-datasets ckan-client (:incidence internal-calculations))
            (mapcat (fn [dataset-config]
                      (read-dataset ckan-client dataset-config
                                    (:resource-id dataset-config)))
                    (:datasets config))
            (constitution/analysis ckan-client (:constitution config)))))

(defn insert-board-report-dataset
  "Calls create-boardreport-dataset and insert new
  dataset into ckan."
  [ckan-client config-url]
  (let [now             (transform/now->str)
        new-dataset     (json/encode {:owner_org "nhsebr"
                                      :title (str "Board report data")
                                      :name (str "board_report_dataset_0")
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
                         {"id" "period_of_coverage" "type" "text"}
                         {"id" "breakdown" "type" "text"}
                         {"id" "level" "type" "text"}
                         {"id" "level_description" "type" "text"}
                         {"id" "upper_ci" "type" "text"}
                         {"id" "lower_ci" "type" "text"}]
        data            (data/prepare-resource-for-insert new-dataset-id new-resource-id
                                                          {"records" records
                                                           "fields"  fields
                                                           "primary_key" "indicator_id,level,year,period_of_coverage"})]
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
;; (insert-board-report-dataset (:ckan-client system) "resources/staging_config.edn")
;; To update existing board resource (preferrable):
;; TEST
;; (update-board-report-dataset (:ckan-client system) "eb600f7a-95cb-4de4-add4-62c687fe0958" "resources/staging_config.edn")
;; USED BY UI:
;; (update-board-report-dataset (:ckan-client system) "ed59dfc4-3076-4e84-806e-7a47d2321f36" "resources/staging_config.edn")
;; (update-board-report-dataset (:ckan-client system) "ace36977-89f5-42ea-b95b-949da2c135cc" "resources/config.edn")
