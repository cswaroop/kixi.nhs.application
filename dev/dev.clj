(ns dev
  "Collection of functons to test CKAN connectivity and data munging."
  (:require [kixi.nhs.data.transform :as transform]
            [kixi.nhs.data.storage   :as storage]
            [cheshire.core           :as json]
            [kixi.ckan.data          :as data]))

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
        resource-to-store  (transform/combine-multiple-datasets (:ckan-client system)
                                                                "7a69bc84-fffd-4750-b22b-fc66a5ea0728"
                                                                "0e73fe0d-0b16-4270-9026-f8fd8a75e684"
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
  (count (transform/combine-multiple-datasets (:ckan-client system)
                                              "7a69bc84-fffd-4750-b22b-fc66a5ea0728"
                                              "0e73fe0d-0b16-4270-9026-f8fd8a75e684")))


;; Examples

;; (combine-multiple-datasets system)
;; (list-all-datasets system)
