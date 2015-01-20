(ns kixi.nhs.board-report
  (:require [kixi.nhs.data.storage :as storage]
            [clojure.tools.logging :as log]))

(defn read-dataset [ckan-client recipe-map resource_id]
  (let [{:keys [indicator-field conditions]} recipe-map
        data (storage/get-resource-data ckan-client resource_id)]
    (filter (fn [d] (every? (fn [condition] (let [{:keys [field value]} condition]
                                              (= (get d field) value)))
                            conditions))
            data)))

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
                                      "2102f836-072a-42a6-b4ca-ac6aa2f96562"))


