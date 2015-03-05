(ns kixi.nhs.data.transform
  "Collections of functions to transform data."
  (:require [kixi.ckan             :as ckan]
            [kixi.nhs.data.storage :as storage]
            [clojure.tools.logging :as log]
            [clj-time.core         :as t]
            [clj-time.format       :as tf]
            [clojure.edn           :as edn]))

(defn not-nil? [x] (not (nil? x)))

(defn parse-number
  "Reads a number from a string. Returns nil if not a number
  or when the value passed is nil."
  [s]
  (when (not-nil? s)
    (let [parsed (clojure.string/replace s #"," "")]
      (when (re-find #"^-?\d+\.?\d*$" parsed)
        (edn/read-string parsed)))))

(defn get-value [k m]
  (-> (get m k)
      parse-number))

(defn outer-join
  "Combines data using specified field and function that acts on data with the same field value.
  Returns a sequence of maps, where each map respresents unique field
  and contains data combined from multiple datasets."
  [field data-fn colls]
  (let [lookup #(get % field)
        indexed (for [coll colls]
                  (into {} (map (juxt lookup identity) coll)))]
    (for [key (distinct (mapcat keys indexed))]
      (into {} (map #(data-fn key %) indexed)))))

(defn resource_id->description
  "Returns description of a resource with a specified id."
  [ckan-client resource_id]
  (:description (storage/get-resource-metadata ckan-client resource_id)))

(defn split-by-field-value
  "Creates a map of sequences, where key is a unique field, and value
  is a sequence containing all maps with that field. e.g.
  collection [{:v 5, :year 2010} {:v 4, :year 2010} {:v 0, :year 2009}]
  being split by :year results in:
  {2009 [{:v 0, :year 2009}], 2010 [{:v 5, :year 2010} {:v 4, :year 2010}]}"
  [field coll]
  (let [unique-values (distinct (map #(get % field) coll))
        accumulator   (zipmap unique-values (for [i unique-values] []))]
    (reduce (fn [acc m]
              (update-in acc [(get m field)] (fn [c] (conj c m))))
            accumulator coll)))

(defn remove-ids
  "Removes _id fields from the data that are generated by DataStore."
  [resource]
  (mapv #(dissoc % "_id") resource))

(def custom-formatter (tf/formatter "yyyyMMddHHmmss"))

(defn now->str
  "Formats the current timestamp into a string
  with date and time."
  []
  (let [now (t/now)]
    (tf/unparse custom-formatter now)))

(defn all-fields-exist?
  "Checks whether all fields are present in the first row
  of the table."
  [fields row]
  (let [headers (into #{} (keys row))]
    (every? #(contains? headers %) fields)))

(defn filter-dataset
  "Filters dataset according to the given recipe."
  [recipe-map data]
  (let [{:keys [conditions indicator-id fields-to-extract]} recipe-map]
    (keep (fn [d] (when (every? (fn [condition] (let [{:keys [field values]} condition]
                                                  ;; values is a set
                                                  (some values #{(get d field)})))
                                conditions)
                    (select-keys d fields-to-extract)))
          data)))

(defn enrich-map
  "Enriches map with indicator_id
  and makes sure period_coverage is not nil
  as it's a PK in CKAN."
  [recipe-map m]
  (let [{:keys [indicator-id metadata]} recipe-map]
    (-> m
        ;; period_of_coverage is a PK so cannot be null. Using year if it's empty
        (cond-> (empty? (:period_of_coverage m)) (assoc :period_of_coverage (:year m)))
        (merge metadata)
        (assoc :indicator_id indicator-id))))

(defn enrich-dataset
  "Enrichs dataset with indicator-id."
  [recipe-map data]
  (mapv #(enrich-map recipe-map %) data))

(defn split-by-key
  "Turns a sequence of maps into a sequence of sequences,
  where each nested sequence corresponds to a single group."
  [k data]
  (->> (group-by k data)
       vals))

(defn add-when-not-empty
  "Sums values in a sequence if it's not empty.
  Otherwise returns nil."
  [data]
  (when (seq data)
    (apply + (map parse-number data))))

(defn remove-fields
  [m fields]
  (apply dissoc m fields))

(defn sum-sequence
  "Retrieves all values for key k from a sequence
  and adds them up. Returns a map containing key 'sum'
  that contains the result of this calculation
  with other key value pairs coming from the first
  item in the sequence."
  [k fields-to-dissoc data]
  (-> data
      first
      (assoc :sum (->> (map k data)
                       (remove #(not (seq %)))
                       add-when-not-empty))
      (remove-fields fields-to-dissoc)))

(defn divide
  "Divides two numbers. Guards against
  vivision by zero and nil values.
  Returns a numeric value."
  [n m]
  (when (and (not (nil? n))
             (not (nil? m))
             (not (zero? m)))
    (float (/ n m))))
