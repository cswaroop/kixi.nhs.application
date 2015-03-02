(ns kixi.nhs.xls
  (:require [kixi.nhs.data.storage  :as storage]
            [clj-excel.core         :as xls]
            [clojure.edn            :as edn]))

(defn retrieve-xls-url
  "Returns URL of the xls from the resource metadata."
  [ckan-client resource_id]
  (:url (storage/get-resource-metadata ckan-client resource_id)))

(defn read-in-xls
  "Retrieves xls spreadsheet from the given URL.
  Returns a lazy sequence with maps of seqs
  that represent worksheets."
  [url]
  (xls/lazy-workbook (xls/workbook-xssf url)))

(defn add-headers
  "Enriches each sequence (row) of data
  with headers."
  [headers data]
  (zipmap headers data))

(defn scrub
  "Skips x number of rows that make up an unparseable
  header in the spreadsheet."
  [scrub-details data]
  (->> data
       (drop (:drop scrub-details))
       (take 1) ;; TODO remove when done testing
       (into [])))

(defn process-worksheet
  "Scrubs and enriches with headers.
  Returns a map containing title
  and scrubbed data for a given
  worksheet."
  [title scrub-details headers data]
  {:title title
   :data (->> data
              (scrub scrub-details)
              (mapv #(add-headers headers %)))})

(defn process-xls
  ""
  [ckan-client recipe]
  (let [{:keys [resource_id headers
                scrub-details
                worksheets]} recipe
        headers              (edn/read-string (slurp headers))
        spreadsheet          (-> (retrieve-xls-url ckan-client resource_id)
                                 read-in-xls)]
    (map #(process-worksheet
           %
           scrub-details
           (get headers %)
           (get spreadsheet %)) worksheets)))

(defn process-xls-recipes [ckan-client]
  (let [recipes (:recipes (edn/read-string (slurp "resources/xls/recipes.edn")))]
    (map #(process-xls ckan-client %) recipes)))
