(ns kixi.nhs.board-report-test
  (:use clojure.test)
  (:require [kixi.nhs.board-report :as board-report]))

(deftest filter-dataset-test
  (testing "Testing filtering dataset.")
  ;; Checks return a sequence of maps, filters on indicator field
  ;; and conditions, keeps indicator value & year:
  (is (= '({"Indicator value" "85.7", "Year" "2013/14"} {"Indicator value" "86.7", "Year" "2012/13"})
         (board-report/filter-dataset {:indicator-id "22"
                                       :indicator-field "Indicator value"
                                       :conditions [{:field "Level" :value "England"}]
                                       :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                      [{"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}])))
  ;; Checks the sequence returned is empty when indicator field not found:
  (is (empty? (board-report/filter-dataset {:indicator-id "22"
                                            :indicator-field "value"
                                            :conditions [{:field "Level" :value "England"}]
                                            :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                           [{"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}])))
  ;; Checks the sequence returned is empty when conditions not met:
  (is (empty (board-report/filter-dataset {:indicator-id "22"
                                           :indicator-field "Indicator value"
                                           :conditions [{:field "Level" :value "Scotland"}]
                                           :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                          [{"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}])))
  ;; Checks the sequence returned is empty when no dataset:
  (is (empty? 
       (board-report/filter-dataset {:indicator-id "22"
                                     :indicator-field "Indicator value"
                                     :conditions [{:field "Level" :value "England"}]
                                     :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                    []))))

(deftest enrich-dataset-test
  (testing "Testing enriching dataset.")
  ;; Checks the "Indicator value" keyword is renamed to "Value",
  ;; the "Indicator id" is added, and the result is a vector:
  (is (=[{"Level" "England", "Denominator" "883852.0", "Indicator id" "22", "Breakdown" "England", "Value" "85.7", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Indicator id" "22", "Breakdown" "England", "Value" "86.7", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Indicator id" "22", "Breakdown" "England", "Value" "88.3", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}]
        (board-report/enrich-dataset {:indicator-id "22"
                         :indicator-field "Indicator value"
                         :conditions [{:field "Level" :value "Scotland"}]
                         :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                        [{"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}])))
  (is (=[{"Indicator id" "22", "Value" "85.7", "Year" "2013/14"} {"Indicator id" "22", "Value" "86.7", "Year" "2012/13"}]
        (board-report/enrich-dataset {:indicator-id "22"
                         :indicator-field "Indicator value"
                         :conditions [{:field "Level" :value "England"}]
                         :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                        (board-report/filter-dataset {:indicator-id "22"
                                         :indicator-field "Indicator value"
                                         :conditions [{:field "Level" :value "England"}]
                                         :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
					[{"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}])))))



