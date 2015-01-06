(ns kixi.nhs.data.transform
  "Collections of functions to transform data."
  (:require [kixi.ckan             :as ckan]
            [kixi.nhs.data.storage :as storage]
            [clojure.tools.logging :as log]))

(defn parse-number
  "Reads a number from a string. Returns nil if not a number."
  [s]
  (let [parsed (clojure.string/replace s #"," "")]
    (when (re-find #"^-?\d+\.?\d*$" parsed)
      (read-string parsed))))

(defn get-value [k m]
  (-> (get m k)
      parse-number))

(defn get-and-transform-multiple-datasets
  "Filters data for CCGs from 2013."
  [ckan-client resource_ids]
  (map (fn [id]
         (->> (storage/get-resource-data ckan-client id)
              (keep #(when (and (= "CCG" (get % "Breakdown"))
                                (= "2013" (get % "Year")))
                       (let [total-patients (get-value "Registered patients" %)
                             observed       (get-value "Observed" %)]
                         (hash-map :ccg (get % "Level")
                                   :resource_id id
                                   :observed_percentage (if (and total-patients observed)
                                                          (str (float (* (/ observed total-patients) 100)))
                                                          "N/A")))))))
       resource_ids))

(defn outer-join
  "Combines data using specified field. Returns a sequence of maps, where each map respresents unique field
  and contains data combined from multiple datasets."
  ([field colls]
   (let [lookup #(get % field)
         indexed (for [coll colls]
                   (into {} (map (juxt lookup identity) coll)))]
     (for [key (distinct (mapcat keys indexed))]
       (into {} (keep #(let [data (get % key)] (when (seq data) data)) indexed)))))
  ([field data-fn colls]
   (let [lookup #(get % field)
         indexed (for [coll colls]
                   (into {} (map (juxt lookup identity) coll)))]
     (for [key (distinct (mapcat keys indexed))]
       (into {} (map data-fn indexed))))))

(defn resource_id->field-name [ckan-client resource_id]
  (let [metadata (storage/get-resource-metadata ckan-client resource_id)]
    ))

(defn combine-multiple-datasets [ckan-client & ids]
  (->> (get-and-transform-multiple-datasets ckan-client ids)
       (outer-join :ccg (fn [data] (let [d (get data key)]
                                     (when (seq d)
                                       (hash-map (:resource_id d) (:observed_percentage d)
                                                 :ccg (:ccg d))))))
       (into [])))

(defn output-multiple-datasets [ckan-client]
  )
