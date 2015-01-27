(ns kixi.nhs.board-report-test
  (:use clojure.test)
  (:require [kixi.nhs.board-report :as board-report]))

(deftest filter-dataset-test
  (testing "Testing filtering dataset.")
  ;; Checks return a sequence of maps, filters on indicator field
  ;; and conditions, keeps indicator value & year:
  (is (= '({:indicator_value "85.7", :year "2013/14"} {:indicator_value "86.7", :year "2012/13"})
         (board-report/filter-dataset
          {:indicator-id "22"
           :indicator-field :indicator_value
           :conditions [{:field :level :value "England"}]
           :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
          [{:level "England", :denominator "883852.0", :breakdown "England",
            :question_response_rate "97.9", :_id 1, :indicator_id nil,
            :year "2013/14", :level_description "England", :indicator_value "85.7",
            :period_of_coverage "July 2013 to March 2014", :numerator "757456.1"}
           {:level "England", :denominator "948507.5", :breakdown "England",
            :question_response_rate "97.7", :_id 2, :indicator_id nil, :year "2012/13",
            :level_description "England", :indicator_value "86.7", :period_of_coverage
            "July 2012 to March 2013", :numerator "822728.3"}
           {:level "Wales", :denominator "1009576.2", :breakdown "England",
            :question_response_rate "97.3", :_id 3, :indicator_id nil, :year "2011/12",
            :level_description "England", :indicator_value "88.3",
            :period_of_coverage "July 2011 to March 2012", :numerator "891213.9"}])))
  ;; Checks the sequence returned is empty when indicator field not found:
  (is (empty? (board-report/filter-dataset
               {:indicator-id "22"
                :indicator-field :value
                :conditions [{:field :level :value "England"}]
                :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
               [{:level "England", :denominator "883852.0", :breakdown "England",
                 :question_response_rate "97.9", :_id 1, :indicator_id nil,
                 :year "2013/14", :level_description "England", :indicator_value "85.7",
                 :period_of_coverage "July 2013 to March 2014", :numerator "757456.1"}
                {:level "England", :denominator "948507.5", :breakdown "England",
                 :question_response_rate "97.7", :_id 2, :indicator_id nil,
                 :year "2012/13", :level_description "England", :indicator_value "86.7",
                 :period_of_coverage "July 2012 to March 2013", :numerator "822728.3"}
                {:level "Wales", :denominator "1009576.2", :breakdown "England",
                 :question_response_rate "97.3", :_id 3, :indicator_id nil,
                 :year "2011/12", :level_description "England", :indicator_value "88.3",
                 :period_of_coverage "July 2011 to March 2012", :numerator "891213.9"}])))
  ;; Checks the sequence returned is empty when conditions not met:
  (is (empty (board-report/filter-dataset
              {:indicator-id "22"
               :indicator-field :indicator_value
               :conditions [{:field :level :value "Scotland"}]
               :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
              [{:level "England", :denominator "883852.0", :breakdown "England",
                :question_response_rate "97.9", :_id 1, :indicator_id nil,
                :year "2013/14", :level_description "England", :indicator_value "85.7",
                :period_of_coverage "July 2013 to March 2014", :numerator "757456.1"}
               {:level "England", :denominator "948507.5", :breakdown "England",
                :question_response_rate "97.7", :_id 2, :indicator_id nil,
                :year "2012/13", :level_description "England", :indicator_value "86.7",
                :period_of_coverage "July 2012 to March 2013", :numerator "822728.3"}
               {:level "Wales", :denominator "1009576.2", :breakdown "England",
                :question_response_rate "97.3", :_id 3, :indicator_id nil,
                :year "2011/12", :level_description "England", :indicator_value "88.3",
                :period_of_coverage "July 2011 to March 2012", :numerator "891213.9"}])))
  ;; Checks the sequence returned is empty when no dataset:
  (is (empty? 
       (board-report/filter-dataset {:indicator-id "22"
                                     :indicator-field :indicator_value
                                     :conditions [{:field :level :value "England"}]
                                     :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                    []))))

(deftest enrich-dataset-test
  (testing "Testing enriching dataset.")
  ;; Checks the :indicator_value keyword is renamed to :value,
  ;; the "Indicator id" is added, and the result is a vector:
  (is (= [{:_id 1, :question_response_rate "97.9", :indicator_id "22",
           :value "85.7", :breakdown "England", :level_description "England",
           :level "England", :numerator "757456.1", :year "2013/14",
           :denominator "883852.0", :period_of_coverage "July 2013 to March 2014"}
          {:_id 2, :question_response_rate "97.7", :indicator_id "22",
           :value "86.7", :breakdown "England", :level_description "England",
           :level "England", :numerator "822728.3", :year "2012/13",
           :denominator "948507.5", :period_of_coverage "July 2012 to March 2013"}
          {:_id 3, :question_response_rate "97.3", :indicator_id "22",
           :value "88.3", :breakdown "England", :level_description "England",
           :level "Wales", :numerator "891213.9", :year "2011/12",
           :denominator "1009576.2", :period_of_coverage "July 2011 to March 2012"}]
        (board-report/enrich-dataset
         {:indicator-id "22"
          :indicator-field :indicator_value
          :conditions [{:field :level :value "Scotland"}]
          :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
         [{:level "England", :denominator "883852.0", :breakdown "England",
           :question_response_rate "97.9", :_id 1, :indicator_id nil,
           :year "2013/14", :level_description "England", :indicator_value "85.7",
           :period_of_coverage "July 2013 to March 2014", :numerator "757456.1"}
          {:level "England", :denominator "948507.5", :breakdown "England",
           :question_response_rate "97.7", :_id 2, :indicator_id nil,
           :year "2012/13", :level_description "England", :indicator_value "86.7",
           :period_of_coverage "July 2012 to March 2013", :numerator "822728.3"}
          {:level "Wales", :denominator "1009576.2", :breakdown "England",
           :question_response_rate "97.3", :_id 3, :indicator_id nil,
           :year "2011/12", :level_description "England", :indicator_value "88.3",
           :period_of_coverage "July 2011 to March 2012", :numerator "891213.9"}])))
  (is (=[{:indicator_id "22", :value "85.7", :year "2013/14"} {:indicator_id "22", :value "86.7", :year "2012/13"}]
        (board-report/enrich-dataset
         {:indicator-id "22"
          :indicator-field :indicator_value
          :conditions [{:field :level :value "England"}]
          :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
         (board-report/filter-dataset
          {:indicator-id "22"
           :indicator-field :indicator_value
           :conditions [{:field :level :value "England"}]
           :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
          [{:level "England", :denominator "883852.0",
            :breakdown "England", :question_response_rate "97.9",
            :_id 1, :indicator_id nil, :year "2013/14",
            :level_description "England", :indicator_value "85.7",
            :period_of_coverage "July 2013 to March 2014", :numerator "757456.1"}
           {:level "England", :denominator "948507.5", :breakdown "England",
            :question_response_rate "97.7", :_id 2, :indicator_id nil,
            :year "2012/13", :level_description "England", :indicator_value "86.7",
            :period_of_coverage "July 2012 to March 2013", :numerator "822728.3"}
           {:level "Wales", :denominator "1009576.2", :breakdown "England",
            :question_response_rate "97.3", :_id 3, :indicator_id nil,
            :year "2011/12", :level_description "England", :indicator_value "88.3",
            :period_of_coverage "July 2011 to March 2012", :numerator "891213.9"}])))))
