(ns kixi.nhs.application.pipeline
  (:require [kixipipe.pipeline          :refer [defnconsumer produce-item produce-items submit-item] :as p]
            [pipejine.core              :as pipejine :refer [new-queue producer-of]]
            [clojure.tools.logging      :as log]
            [com.stuartsierra.component :as component]
            [kixi.nhs.board-report      :as board-report]))

(defn build-pipeline [ckan-client]
  (let [fanout-q               (new-queue {:name "fanout-q" :queue-size 50})
        board-report-update-q  (new-queue {:name "board-report-update-q" :queue-size 5})
        board-report-insert-q  (new-queue {:name "board-report-insert-q" :queue-size 5})]

    (defnconsumer fanout-q [{:keys [dest type] :as item}]
      (let [item (dissoc item :dest)]
        (condp = dest
          :board-report (condp = type
                          :insert (produce-item item board-report-insert-q)
                          :update (produce-item item board-report-update-q)))))

    (defnconsumer board-report-update-q [item]
      (let [{:keys [resource-id]} item]
        (log/info "Updating board report resource.")
        (board-report/update-board-report-dataset ckan-client resource-id "resources/config.edn")
        (log/info "Finished updating board report.")))

    (defnconsumer board-report-insert-q [item]
      (log/info "Inserting new board report dataset/resource.")
      (board-report/insert-board-report-dataset ckan-client "resources/config.edn")
      (log/info "Finished inserting new board report dataset/resource."))

    (producer-of fanout-q board-report-update-q board-report-insert-q)

    (list fanout-q #{board-report-update-q board-report-insert-q})))

(defrecord Pipeline []
  component/Lifecycle
  (start [this]
    (log/info "Pipeline starting")
    (let [ckan-client (-> this :ckan-client)
          [head others] (build-pipeline ckan-client)]
      (-> this
          (assoc :head head)
          (assoc :others others))))
  (stop [this] this))


(defn new-pipeline []
  (->Pipeline))
