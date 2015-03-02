(ns kixi.nhs.xls
  (:require [kixi.nhs.data.storage   :as storage]
            [clj-excel.core          :as xls]
            [clojure.edn             :as edn]
            [kixi.nhs.data.transform :as transform]))

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

(defn parse-formula
  "Finds cells and operation in a formula.
  Returns a sequence of [cell1 op cell2]."
  [formula]
  (rest (re-find #"^(\w+\d+)(\/|\-|\+|\*)(\w+\d+)$" formula)))

(defmulti parse-value (fn [v xs] (first (keys v))))

(defmethod parse-value :error [_ _]
  nil)

(defn col->int
  "Convert each character to its positional number in alphabet.
  For example the character A has ascii value of 65. If we subtract
  65 it becomes 0 which means A is the first column."
  [x]
  (- (int (char (first (seq x)))) 65))

(defn get-row [idx data]
  (->> data (drop idx) first))

(defn cell->value
  "Find a specific cell in data.
  Returns a value of that cell, either string,
  number or nil."
  [cell data]
  (when cell
    (let [column (col->int (re-find #"[A-Za-z]*" cell))
          row    (get-row (dec (Integer/parseInt (re-find #"\d+" cell))) data)
          v      (nth row column)]
      v)))

(defmethod parse-value :formula [v data]
  (let [formula    (:formula v)
        [c1 op c2] (parse-formula formula)
        cell1      (cell->value c1 data)
        cell2      (cell->value c2 data)
        operation  (condp = op
                     "/" /
                     "*" *
                     "-" -
                     "+" +
                     nil)]
    (when operation
      (operation cell1 cell2))))

(defmethod parse-value :default [_ _] nil)

(defn parse-values
  "Iterates over data and parses any
  map values encountered.
  If the value contains formula, calculates
  its result."
  [data row]
  (mapv #(if (map? %)
           (parse-value % data)
           %)
        row))

(defn scrub
  "Skips x number of rows that make up an unparseable
  header in the spreadsheet."
  [{:keys [offset last-row]} data]
  (->> data
       (take last-row)
       (drop offset)
       (mapv #(parse-values data %))
       (into [])))

(defn process-worksheet
  "Scrubs and enriches with headers.
  Returns a map containing title
  and scrubbed data for a given
  worksheet."
  [recipe headers data]
  (->> data
       (scrub (:scrub-details recipe))
       (mapv #(add-headers headers %))
       (transform/enrich-dataset recipe)))

(defn process-xls
  "Retrieves spreadsheet and its headers,
  scrubs the data and prepares a data structure
  suitable for inserting to CKAN."
  [ckan-client recipe]
  (let [{:keys [resource_id headers
                scrub-details
                worksheets]} recipe
        headers              (edn/read-string (slurp headers))
        spreadsheet          (-> (retrieve-xls-url ckan-client resource_id)
                                 read-in-xls)]
    (map #(process-worksheet
           recipe
           (get headers %)
           (get spreadsheet %)) worksheets)))
