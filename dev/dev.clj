(ns dev
  "Collection of functons to test CKAN connectivity and data munging."
  (:require [kixi.nhs.data.transform :as transform]
            [kixi.nhs.data.storage   :as storage]
            [cheshire.core           :as json]
            [kixi.ckan.data          :as data]))

(defn get-and-transform-multiple-datasets
  "Filters data for CCGs from 2013."
  [ckan-client resource_ids]
  (map (fn [id]
         (->> (storage/get-resource-data ckan-client id)
              (keep #(when (and (= "CCG" (get % "Breakdown"))
                                (= "2013" (get % "Year")))
                       (let [total-patients (transform/get-value "Registered patients" %)
                             observed       (transform/get-value "Observed" %)]
                         (hash-map :ccg (get % "Level")
                                   :resource_id id
                                   :observed_percentage (if (and total-patients observed)
                                                          (str (float (* (/ observed total-patients) 100)))
                                                          "N/A")))))))
       resource_ids))

(defn combine-and-transform [ckan-client & ids]
  (->> (get-and-transform-multiple-datasets ckan-client ids)
       (transform/outer-join :ccg (fn [k data] (let [d (get data k)]
                                                 (when (seq d)
                                                   ;; could use transform/resource_id->description but description of the files
                                                   ;; in this case is "Indicator data ()" - not unique. Using resource_id instead.
                                                   (hash-map (:resource_id d) (:observed_percentage d)
                                                             :ccg (:ccg d))))))
       (into [])))

(defn combine-and-store-multiple-datasets [system]
  (let [new-dataset        (json/encode {:owner_org "kixi"
                                         :title "testing_combining_multiple_datasets"
                                         :name (str "combined_muliple_datasets" "_" (quot (System/currentTimeMillis) 1000))
                                         :author "Kixi"
                                         :notes "Testing Clojure CKAN client: combining multiple datasets into one."})
        new-package_id     (storage/create-new-dataset (:ckan-client system) new-dataset)
        new-resource       (json/encode {:package_id new-package_id
                                         :url "foo"
                                         :description "Combined datasets."})
        new-resource_id    (storage/create-new-resource (:ckan-client system) new-package_id new-resource)
        resource-to-store  (combine-and-transform (:ckan-client system)
                                                  "7a69bc84-fffd-4750-b22b-fc66a5ea0728"
                                                  "7381b851-7a50-4b8c-b64e-155eadbe5694")
        data               (data/prepare-resource-for-insert new-package_id new-resource_id {"records" resource-to-store})]

    (storage/insert-new-resource (:ckan-client system) new-package_id data)))

(defn list-all-datasets [system]
  (storage/all-datasets-names (:ckan-client system)))

(defn combine-multiple-datasets
  "Returns the number of created rows (211). It's a result of
  extracting data for 2013 for CCGs from specified datasets
  and grouping it by CCG."
  [system]
  (count (combine-multiple-datasets (:ckan-client system)
                                    "7a69bc84-fffd-4750-b22b-fc66a5ea0728"
                                    "7381b851-7a50-4b8c-b64e-155eadbe5694")))

(defn output-multiple-datasets
  "Splits dataset by year and outputs data into separate resources."
  [system field resource_id title]
  (let [dataset-to-split (storage/get-resource-data (:ckan-client system) resource_id)
        cleaned-dataset  (transform/remove-ids dataset-to-split)
        split-resources  (transform/split-by-field-value field cleaned-dataset)
        new-dataset      (json/encode {:owner_org "kixi"
                                       :title title
                                       :name (str "output_muliple_datasets" "_" (quot (System/currentTimeMillis) 1000))
                                       :author "Kixi"
                                       :notes "Testing Clojure CKAN client: outputting multiple datasets."})
        new-package_id   (storage/create-new-dataset (:ckan-client system) new-dataset)]
    (doseq [[k records] split-resources]
      (let [new-resource       (json/encode {:package_id new-package_id
                                             :url "http://fix-me" ;; url is mandatory
                                             :description (str "Dataset " k)})
            new-resource_id    (storage/create-new-resource (:ckan-client system) new-package_id new-resource)
            ;; provide fields to ensure future inserts don't fail because of wrong types. assume all strings.
            fields             (mapv #(hash-map "id" % "type" "text") (keys (first records)))
            data               (data/prepare-resource-for-insert new-package_id new-resource_id {"records" records
                                                                                                 "fields"  fields})]
        (storage/insert-new-resource (:ckan-client system) new-package_id data)))))


;; Examples

;; (combine-multiple-datasets system)

;;(list-all-datasets system)


;;(output-multiple-datasets system "Year" "0e73fe0d-0b16-4270-9026-f8fd8a75e684" "testing_outputting_multiple_datasets (per year)")

;; (output-multiple-datasets system "Gender" "0e73fe0d-0b16-4270-9026-f8fd8a75e684" "testing_outputting_multiple_datasets (per gender)")
;; (output-multiple-datasets system "Breakdown" "0e73fe0d-0b16-4270-9026-f8fd8a75e684" "testing_outputting_multiple_datasets (per area)")
