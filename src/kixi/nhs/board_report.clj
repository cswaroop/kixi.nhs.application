(ns kixi.nhs.board-report
  (:require [kixi.nhs.data.storage :as storage]
            [clojure.tools.logging :as log]
            [clojure.edn           :as edn]
            [clj-time.core         :as t]
            [clj-time.format       :as tf]
            [cheshire.core         :as json]
            [kixi.ckan.data        :as data]
            [kixi.nhs.data.transform :as transform]))

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
;; indicator 213

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
       vals
       (into [])))

(defn patient-experience-of-gp-services
  "Patient experience of primary care - GP Services.
  White British compared to Asian or Asian British."
  [recipe data]
  (let [numerators           (filter-dataset (:numerators recipe) data)
        denominators         (filter-dataset (:denominators recipe) data)
        indicator-values     (filter-dataset (:indicator-values recipe) data)
        numerators-by-year   (split-by-key :year numerators)
        denominators-by-year (split-by-key :year denominators)
        numerator-sums       (mapv #(sum-sequence % :numerator) numerators-by-year)
        denominator-sums     (mapv #(sum-sequence % :denominator) denominators-by-year)
        division-result      (map divide-seqs numerator-sums denominator-sums)
        combined-data        (into [] (clojure.set/join division-result indicator-values))
        final-dataset        (mapv subtract-seqs combined-data)]

    (enrich-dataset {:indicator-id (:indicator-id recipe)
                     :indicator-field :result}
                    final-dataset)))

(defn patient-experience [ckan-client recipe]
  (let  [resource_id (:resource-id recipe)
         data        (storage/get-resource-data ckan-client resource_id)]
    (patient-experience-of-gp-services recipe data)))

(defn process-patient-experience-recipes [ckan-client recipes]
  (mapcat #(patient-experience ckan-client %) recipes))


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
    (concat (process-patient-experience-recipes ckan-client (:internal-calculations config))
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

(comment
  ;; 23
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level description" :value "England"}]}
                                      "dd24350b-0e0d-48ca-93c6-d2ece5b1ac4e")
  ;; 45
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level description" :value "England"}]}
                                      "7cb803a1-5c88-46e0-9e61-cf4c47ffadcb")
  ;; 46
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level description" :value "England"}]}
                                      "9963285f-752f-4dbb-8c02-07868ae52905")
  ;; 47
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level description" :value "England"}]}
                                      "02d26183-1a69-4540-8254-00216622124e")
  ;; 48
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level description" :value "England"}]}
                                      "db911491-ac1b-4148-ab8c-a2e8596d5257")
  ;; 52
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "81c8e579-cdeb-401e-bf1a-2e3bde98fc39")
  ;; 56
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Breakdown" :value "England"}]}
                                      "eb192498-8c9c-4746-a40a-a098dff69e46")
  ;; 58
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "2102f836-072a-42a6-b4ca-ac6aa2f96562")
  ;; 22
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "2b10e7b4-c799-44f9-81d6-3a42f4260893")
  ;; 24
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "8b2dbabb-9b0a-4ea6-b357-74200ae4f311")
  ;; 27
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "e2386960-b9e1-4d9f-ba8b-6d79c5c62d94")
  ;; 28
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "bf9fff22-599e-4d58-8b78-961bc6773d62")
  ;; 43
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "e1ec7ee1-d387-45e3-a0dd-3c8e3162e0e0")
  ;; 65
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "e39649c2-e4dd-49b3-bb9d-ae7d7b69bd2d")
  ;; 66
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "5c932116-a89c-4efc-9db9-f4b36e812ef7")
  ;; 44
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "f581825b-cf3e-4f5d-a358-190dcc3f8a0e")
  ;; 61
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "f581825b-cf3e-4f5d-a358-190dcc3f8a0e")
  ;; 62
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value (rate)"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "aaa57c54-c747-4eb8-aa9e-f3da798372f3")
  ;; 54
  (kixi.nhs.board-report/read-dataset (:ckan-client system)
                                      {:indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]}
                                      "3cb3fc90-3944-455a-97f0-50c9680184c7")

  ;; 211
  ;; This is an internal calculation. Recipe on "internal calculations.xlsx": Take away the indicator value
  ;; (on column F) for males (gender referenced on column C) from the
  ;; indicator value (on column F) for females (gender referenced on column C)

  )
