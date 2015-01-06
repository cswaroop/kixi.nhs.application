(ns kixi.nhs.data.storage
  (:require [kixi.ckan :as ckan]
            [kixi.ckan.data :as data]
            [clojure.tools.logging :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datasets                                                    ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn all-datasets-names
  "Return a list of the names of the site’s datasets (packages)."
  [system]
  (let [response (ckan/package-list (:ckan-client system))]
    (:result response)))

(defn all-datasets-and-resources
  "Return a list of the site’s datasets (packages) and their resources."
  [system]
  (let [response (ckan/package-list-with-resources (:ckan-client system))]
    (:result response)))

(defn dataset-metadata
  "Return the metadata of a dataset (package) and its resources."
  [system package_id]
  (let [response (ckan/package-show (:ckan-client system) package_id)]
    (:result response)))

(defn create-new-package
  "Create a new dataset (package). Returns id of the newly created dataset."
  [system]
  (let [dataset (data/parse {:owner_org "kixi"
                             :title "testing_transformation_kixi"
                             :name "transformation_test_kixi_5"
                             :author "Kixi"
                             :notes "Testing Clojure CKAN client: transformation of existing data and creation of new datasets."})]
    (:id (ckan/package-create (:ckan-client system) dataset))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Resources                                                   ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-resource-metadata
  "Return the metadata of a resource."
  [system resource_id]
  (let [response (ckan/resource-show (:ckan-client system) resource_id)]
    (:result response)))

(defn create-new-resource
  "Appends a new resource to a datasets list of resources.
  Returns id of the newly created resource."
  [system package_id]
  (let [resource (data/parse {:package_id package_id
                              :url "foo"
                              :description "Transformed copy of a resource."})]
    (ckan/resource-create (:ckan-client system) package_id resource)))

(defn delete-resource
  "Delete a resource from a dataset. Returns true if deleted successfully."
  [system resource_id]
  (ckan/resource-delete (:ckan-client system) resource_id))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datastore                                                   ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-resource-data
  "Searches a DataStore resource. Returns a sequence of records."
  [system resource_id]
  (ckan/datastore-search (:ckan-client system) resource_id))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Organization                                                ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn organization-details
  "Return the details of a organization and its datasets.
  Only its first 1000 datasets are returned."
  [system id]
  (let [response (ckan/organization-show (:ckan-client system) id)]
    (:result response)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tags                                                        ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tag-and-datasets
  "Returns the details of the tag, including a list of all
  of the tag’s datasets and their details."
  [system id]
  (let [response (ckan/tag-show (:ckan-client system) id)]
    (:result response)))
