(ns kixi.nhs.exploration-split
  (:require
   [cheshire.core           :as json]
   [kixi.nhs.data.storage   :as storage]
   [kixi.nhs.data.transform :as transform]
   [kixi.nhs.board-report   :as breport]
   [dev                     :as dev]))

;; Use functions in dev and board-report
;; to split data on locations and dates.

;; 1) Filter according to data of interest
;; specified in config
;; 2) Check the format of the parameter
;; 3) Re-format parameter if needed
;; 4) Maybe enrich with spreadsheet indicator or resource_id
;; 5) Join datasets on the paraneter
;; 6) Insert a new dataset in ckan

;____________________________________________________________________________
;; List of column names and formats:
(def date-formats
  ;; Different column headers and formats for dates in NHS ckan spreadsheets.
  ;; To be re-written with regex.
  {:year ["YYYY" ;; 2011
          "YYYY/YY" ;; 2013/14
          "YYYY-MM-eeTHH:mm:ss" ;; 2011-12-19T00:00:00
          ]
   :period ["M YYYY to M YYYY (x)" ;; January 2013 to December 2013 (Provisional)
            ]
   :year_of_diagnosis ["YYYY"]
   :period_of_coverage ["M YYYY to M YYYY" ;; July 2013 to March 2014
                        ]
   :reporting_period ["Quarter X YYYY/YY" ;; Quarter 4 2013/14
                      "M - M YYYY" ;; April - Sept 2013
                      "YYYY-MM-eeTHH:mm:ss" ;; 2011-12-19T00:00:00
                      ]})

(def location-formats
  ;; Different locations/CCG in NHS ckan spreadsheets.
  ;; To be re-written with regex.
  {:ccg_code "XXX" ;; 00C
   :ccg_name "NHS x CCG" ;; NHS Darlington CCG
   :breakdown {:level "XXX" ;; 00L (if (= breakdown "CCG"))
               :level_description "NHS xxxxxxxxx CCG"
               ;; NHS Darlington CCG (if (= breakdown "CCG"))
               }})
;;___________________________________________________________________________

;; Example of ckan dataset: 4.2 Patient experience of hospital care
'({:_better_information_more_choice_domain_ "66.1", :_id 19, :_weighted_average_score_ "75.5",
   :breakdown "CCG", :level_description "NHS East Lancashire CCG", :level "01A",
   :_safe_high_quality_co-ordinator_care_domain_ "64.8", :_access__waiting_domain_ "83.9",
   :_clean_friendly_comfortable_place_to_be_domain_ "78.8", :period "2013/14",
   :_building_closer_relationships_domain_ "84.1"}
  {:_better_information_more_choice_domain_ "69.2", :_id 20, :_weighted_average_score_ "77.4",
   :breakdown "CCG", :level_description "NHS Eastern Cheshire CCG", :level "01C",
   :_safe_high_quality_co-ordinator_care_domain_ "65.5", :_access__waiting_domain_ "87.1",
   :_clean_friendly_comfortable_place_to_be_domain_ "79.3", :period "2013/14",
   :_building_closer_relationships_domain_ "86.2"})

;; What the config info would look like:
{:indicator-id "x"
 :indicator-fields [:_weighted_average_score_ :period]
 :params [{:split-on "location" :value :level_description}]
 :resource-id "c9315cd1-7679-4c26-8279-ee7d64660390"}

;; Should be filtered like this:
[{:_weighted_average_score_ "75.5", :level_description "NHS East Lancashire CCG" :period "2013/14"}
 {:_weighted_average_score_ "77.4", :level_description "NHS Eastern Cheshire CCG" :period "2013/14"}]

;;I want to end up with this:
[{:indicator-id "x" :period "2013/14" "NHS East Lancashire CCG" "75.5"
  "NHS Eastern Cheshire CCG" "77.4"}]
;; Info for next year will be in another map:
;; [{:indicator-id "x" :period "2013/14" "NHS East Lancashire CCG" "75.5"
;;   "NHS Eastern Cheshire CCG" "77.4"}
;;  {:indicator-id "x" :period "2014/15" "NHS East Lancashire CCG" "76.8"
;;   "NHS Eastern Cheshire CCG" "74.3"}]

;;_____________________________________________________

(defn contains-indicator-fields?
  "Checks whether all indicator fields needed
  are present in the dataset."
  [config-info data]
  (every? true? (for [x (:indicator-fields config-info)
                      :let [y (contains? (first data) x)]]
                  y)))

(defn contains-params?
  "Checks whether all parameters to split on
  are present in the dataset."
  [config-info data]
  (every? true? (for [x (:params config-info)
                      :let [y (contains? (first data) (:value x))]]
                  y)))

(defn filter-on-params-indicators
  "If the indicator and parameter fields are present,
  Only keep those fields in a vector of maps."
  [config-info data]
  (when (and (contains-indicator-fields? config-info data)
             (contains-params? config-info data))
    (let [split-params (into [] (for [x (:params config-info)] (:value x)))]
      (mapv (fn [d] (select-keys d
                                 (concat (:indicator-fields config-info)
                                         split-params)))
            data))))

(defn split-dataset-by-CCGs
  "Builds a new data structure with a map per
  period and data for each CCG in each map."
  [config-info data]
  (into {} (let [{:keys [indicator-fields params indicator-id]} config-info]
             (map (fn [d]
                    (let [new-keys [:period :indicator-id (:level_description d)]
                          new-values [(:period d) indicator-id (:_weighted_average_score_ d)]
                          m (zipmap new-keys new-values)]
                      m))
                  data))))

(defn split-dataset-by-periods
  "Builds a new data structure with a map per
  CCG and data for each period in each map."
  [config-info data]
  (into {} (let [{:keys [indicator-fields params indicator-id]} config-info]
             (map (fn [d]
                    (let [new-keys [:level_description :indicator-id (:period d)]
                          new-values [(:level_description d) indicator-id
                                      (:_weighted_average_score_ d)]
                          m (zipmap new-keys new-values)]
                      m))
                  data))))

(defn create-exploration-dataset
  "Creates a new CKAN dataset split on CCGs or periods."
  [ckan-client config-info resource-id]
  (when (= '(true) (and (= 1 (count (:params config-info)))
                         (for [x (:params config-info)]
                           (= (:split-on x) "location"))))
    (storage/get-resource-data ckan-client resource-id)
    ;;(filter-on-params-indicators config-info)
    ;;(split-dataset-by-CCGs config-info)
         
    )
  (when (= '(true) (and (= 1 (count (:params config-info)))
                         (for [x (:params config-info)]
                           (= (:split-on x) "period"))))
    ;;(storage/get-resource-data ckan-client resource-id)
    ;;(filter-on-params-indicators config-info)
    ;;(split-dataset-by-periods config-info)
    ))

;; (defn create-exploration-dataset
;;   "Creates a new CKAN dataset split on CCGs or periods."
;;   [ckan-client config-info resource-id]
;;   (->> (storage/get-resource-data ckan-client resource-id)
;;        (filter-on-params-indicators config-info)
;;        (if (and (= 1 (count (:params config-info)))
;;                 (for [x (:params config-info)]
;;                   (= (:split-on x) "location")))
;;          (split-dataset-by-CCGs config-info))
;;        (if (and (= 1 (count (:params config-info)))
;;                 (for [x (:params config-info)]
;;                   (= (:split-on x) "period")))
;;          (split-dataset-by-periods config-info))))

;;________________________________________________________
;;Trying out functions:
(comment
  (filter-on-params-indicators
   {:indicator-id "x"
    :indicator-fields [:_weighted_average_score_ :period]
    :params [{:split-on "location" :value :level_description}]
    :resource-id "c9315cd1-7679-4c26-8279-ee7d64660390"}
   '({:_better_information_more_choice_domain_ "66.1", :_id 19, :_weighted_average_score_ "75.5",
      :breakdown "CCG", :level_description "NHS East Lancashire CCG", :level "01A",
      :_safe_high_quality_co-ordinator_care_domain_ "64.8", :_access__waiting_domain_ "83.9",
      :_clean_friendly_comfortable_place_to_be_domain_ "78.8", :period "2013/14",
      :_building_closer_relationships_domain_ "84.1"}
     {:_better_information_more_choice_domain_ "69.2", :_id 20, :_weighted_average_score_ "77.4",
      :breakdown "CCG", :level_description "NHS Eastern Cheshire CCG", :level "01C",
      :_safe_high_quality_co-ordinator_care_domain_ "65.5", :_access__waiting_domain_ "87.1",
      :_clean_friendly_comfortable_place_to_be_domain_ "79.3", :period "2013/14",
      :_building_closer_relationships_domain_ "86.2"}))
  ;; Returns:
  ;;[{:level_description "NHS East Lancashire CCG", :period "2013/14", :_weighted_average_score_ "75.5"} {:level_description "NHS Eastern Cheshire CCG", :period "2013/14", :_weighted_average_score_ "77.4"}]

  (split-dataset-by-CCGs
   {:indicator-id "x"
    :indicator-fields [:_weighted_average_score_ :period]
    :params [{:split-on "location" :value :level_description}]
    :resource-id "c9315cd1-7679-4c26-8279-ee7d64660390"}
   '({:_better_information_more_choice_domain_ "66.1", :_id 19, :_weighted_average_score_ "75.5",
      :breakdown "CCG", :level_description "NHS East Lancashire CCG", :level "01A",
      :_safe_high_quality_co-ordinator_care_domain_ "64.8", :_access__waiting_domain_ "83.9",
      :_clean_friendly_comfortable_place_to_be_domain_ "78.8", :period "2013/14",
      :_building_closer_relationships_domain_ "84.1"}
     {:_better_information_more_choice_domain_ "69.2", :_id 20, :_weighted_average_score_ "77.4",
      :breakdown "CCG", :level_description "NHS Eastern Cheshire CCG", :level "01C",
      :_safe_high_quality_co-ordinator_care_domain_ "65.5", :_access__waiting_domain_ "87.1",
      :_clean_friendly_comfortable_place_to_be_domain_ "79.3", :period "2013/14",
      :_building_closer_relationships_domain_ "86.2"}))
  ;; Returns:
  ;; {"NHS East Lancashire CCG" "75.5", :indicator-id "x", :period "2013/14", "NHS Eastern Cheshire CCG" "77.4"}
  )
