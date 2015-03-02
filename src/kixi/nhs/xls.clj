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

(defn parse-formula
  "Finds cells and operation in a formula.
  Returns a sequence of [cell1 op cell2]."
  [formula]
  (rest (re-find #"^(\w+\d+)(\/|\-|\+|\*)(\w+\d+)$" formula)))

(defmulti parse-value (fn [v offset xs] (first (keys v))))

(defmethod parse-value :error [_ _ _]
  nil)

(defn col->int
  "Convert each character to its positional number in alphabet.
  For example the character A has ascii value of 65. If we subtract
  65 it becomes 0 which means A is the first column."
  [x]
  (- (int (char (first (seq x)))) 65))

(defn get-row [offset idx data]
  (->> data (drop idx) first))

(defn cell->value
  "Find a specific cell in data.
  Returns a value of that cell, either string,
  number or nil."
  [offset cell data]
  (when cell
    (let [column (col->int (re-find #"[A-Za-z]*" cell))
          row    (get-row offset (dec (Integer/parseInt (re-find #"\d+" cell))) data)
          v      (nth row column)]
      v)))

(defmethod parse-value :formula [v offset data]
  (let [formula    (:formula v)
        [c1 op c2] (parse-formula formula)
        cell1      (cell->value offset c1 data)
        cell2      (cell->value offset c2 data)
        operation  (condp = op
                     "/" /
                     "*" *
                     "-" -
                     "+" +
                     nil)]
    (when operation
      (operation cell1 cell2))))

(defmethod parse-value :default [_ _ _ ] nil)

(defn parse-values
  ""
  [offset data row]
  (mapv #(if (map? %)
           (parse-value % offset data)
           %)
        row))

(defn scrub
  "Skips x number of rows that make up an unparseable
  header in the spreadsheet."
  [{:keys [offset last-row]} data]
  (->> data
       (take last-row)
       (drop offset)
       (mapv #(parse-values offset data %))
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

(defn process-constitution-recipes [ckan-client]
  (let [recipes (-> (slurp "resources/xls/recipes.edn") edn/read-string :recipes :constitution)]
    (mapcat #(process-xls ckan-client %) recipes)))
