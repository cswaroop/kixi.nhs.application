(ns kixi.nhs.xls
  (:require [kixi.nhs.data.storage  :as storage]
            [clj-excel.core         :as xls]))

(def headers
  [:area_team_code
   :area_team_name
   :ccg_code
   :ccg_name

   :total_survey_forms_distributed
   :total_completed_forms_receive
   :response_rate

   :know_how_to_contact_an_out-of-hours_gp_service_-_total_responses
   :know_how_to_contact_an_out-of-hours_gp_service_-_yes
   :know_how_to_contact_an_out-of-hours_gp_service_-_no
   :know_how_to_contact_an_out-of-hours_gp_service_-_%_yes
   :know_how_to_contact_an_out-of-hours_gp_service_-_%_no
   :know_how_to_contact_an_out-of-hours_gp_service_-_ci_yes
   :know_how_to_contact_an_out-of-hours_gp_service_-_ci_lower_limit
   :know_how_to_contact_an_out-of-hours_gp_service_-_ci_upper_limit

   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_total_responses
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_yes_for_myself
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_yes_for_someone_else
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_no
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_%_yes_for_myself
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_%_yes_for_someone_else
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_%_no
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_ci_yes_total
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_ci_lower_limit
   :tried_to_call_an_out-of-hours_gp_service_in_past_6_months_-_ci_upper_limit

   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_total_responses
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_very_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_fairly_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_not_very_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_not_at_all_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_dont_know_didnt_make_contact
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_%_very_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_%_fairly_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_%_not_very_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_%_not_at_all_easy
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_%_dont_know_didnt_make_contact
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_ci_easy_total
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_ci_lower_limit
   :ease_of_contacting_the_out-of-hours_gp_service_by_telephone_-_ci_upper_limit

   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_total_responses
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_it_was_about_right
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_it_took_too_long
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_dont_know_doesnt_apply
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_%_it_was_about_right
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_%_it_took_too_long
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_%_dont_know_doesnt_apply
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_ci_was_about_right
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_ci_lower_limit
   :impression_of_how_quickly_care_from_out-of-hours_gp_service_received_-_ci_upper_limit

   :confidence_and_trust_in_out-of-hours_clinician_-_total_responses
   :confidence_and_trust_in_out-of-hours_clinician_-_yes_definitely
   :confidence_and_trust_in_out-of-hours_clinician_-_yes_to_some_extent
   :confidence_and_trust_in_out-of-hours_clinician_-_no_not_at_all
   :confidence_and_trust_in_out-of-hours_clinician_-_dont_know_cant_say
   :confidence_and_trust_in_out-of-hours_clinician_-_%_yes_definitely
   :confidence_and_trust_in_out-of-hours_clinician_-_%_yes_to_some_extent
   :confidence_and_trust_in_out-of-hours_clinician_-_%_no_not_at_all
   :confidence_and_trust_in_out-of-hours_clinician_-_%_dont_know_cant_say
   :confidence_and_trust_in_out-of-hours_clinician_-_ci_yes_total
   :confidence_and_trust_in_out-of-hours_clinician_-_ci_lower_limit
   :confidence_and_trust_in_out-of-hours_clinician_-_ci_upper_limit

   :overall_experience_of_out-of-hours_gp_services_-_total_responses
   :overall_experience_of_out-of-hours_gp_services_-_very_good
   :overall_experience_of_out-of-hours_gp_services_-_fairly_good
   :overall_experience_of_out-of-hours_gp_services_-_neither_good_nor_poor
   :overall_experience_of_out-of-hours_gp_services_-_fairly_poor
   :overall_experience_of_out-of-hours_gp_services_-_very_poor
   :overall_experience_of_out-of-hours_gp_services_-_%_very_good
   :overall_experience_of_out-of-hours_gp_services_-_%_fairly_good
   :overall_experience_of_out-of-hours_gp_services_-_%_neither_good_nor_poor
   :overall_experience_of_out-of-hours_gp_services_-_%_fairly_poor
   :overall_experience_of_out-of-hours_gp_services_-_%_very_poor
   :overall_experience_of_out-of-hours_gp_services_-_ci_good_total
   :overall_experience_of_out-of-hours_gp_services_-_ci_lower_limit
   :overall_experience_of_out-of-hours_gp_services_-_ci_upper_limit])

(defn retrieve-xls-url
  ""
  [ckan-client resource_id]
  (:url (storage/get-resource-metadata ckan-client resource_id)))

(defn read-in-xls [url]
  (xls/lazy-workbook (xls/workbook-xssf url)))

(defn add-fields [data]
  (mapv #(hash-map %1 %2) headers data))

(defn scrub [data]
  (println (count (first data)))
  (->> data
       (drop 10)
       (take 1)
       (into [])))

(defn process-worksheet
  [data]
  {:title "OUT OF HOURS"
   :data (->> data
              scrub
              (mapv add-fields))})

(defn process-xls [ckan-client resource_id]
  (-> (retrieve-xls-url ckan-client resource_id)
      read-in-xls
      (get "OUT OF HOURS")
      process-worksheet)
  )


;; url "https://nhsenglandfilestore.s3.amazonaws.com/gp-survey/9bd132c481ea8215b9bcfca501b99fd3.xls"

;; (process-xls (:ckan-client system) "3a3ec969-bdfd-4df3-983a-bdeafd15ee59")
