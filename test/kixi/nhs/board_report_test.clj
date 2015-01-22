(ns kixi.nhs.board-report-test
  (:use clojure.test)
  (:require [kixi.nhs.board-report :as board-report]))


(deftest filter-and-enrich-dataset-test
  (testing "Testing filtering and enriching dataset")
  ; Checks if the sequence is returned with "Year", "Indicator id" and "Value"
  ; and that the filter works properly.
  (is (= '({"Year" "2013/14" "Indicator id" "22" "Value" "85.7"} {"Year" "2012/13" "Indicator id" "22" "Value" "86.7"})
       (board-report/filter-and-enrich-dataset {:indicator-id "22"
                                   :indicator-field "Indicator value"
                                   :conditions [{:field "Level" :value "England"}]
                                   :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                               '({"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}))))
  ; Checks an empty resource returns an empty sequence.
  (is (= '()
         (board-report/filter-and-enrich-dataset {:indicator-id "22"
                                                  :indicator-field "Indicator value"
                                                  :conditions [{:field "Level" :value "England"}]
                                                  :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                                 '())))
  ; Checks a condition not met in the resource returns an empty sequence.
  (is (= '()
         (board-report/filter-and-enrich-dataset {:indicator-id "22"
                                                  :indicator-field "Indicator value"
                                                  :conditions [{:field "Level" :value "England"}]
                                                  :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                                 '({"Level" "Scotland", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "N Ireland", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "Wales", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}))))
  ; Checks an indicator-field not found in the resource returns an empty sequence.
  (is (= '()
         (board-report/filter-and-enrich-dataset {:indicator-id "22"
                                                  :indicator-field "Indicator"
                                                  :conditions [{:field "Level" :value "England"}]
                                                  :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                                 '({"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"} {"Level" "England", "Denominator" "1009576.2", "Breakdown" "England", "Question response rate" "97.3", "_id" 3, "indicator_id" nil, "Year" "2011/12", "Level description" "England", "Indicator value" "88.3", "Period of coverage" "July 2011 to March 2012", "Numerator" "891213.9"}))))
  ; Checks when conditions are empty all the data is returned.
  (is (= '({"Year" "2013/14" "Indicator id" "22" "Value" "85.7"} {"Year" "2012/13" "Indicator id" "22" "Value" "86.7"})
         (board-report/filter-and-enrich-dataset {:indicator-id "22"
                                                  :indicator-field "Indicator value"
                                                  :conditions []
                                                  :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                                 '({"Level" "England", "Denominator" "883852.0", "Breakdown" "England", "Question response rate" "97.9", "_id" 1, "indicator_id" nil, "Year" "2013/14", "Level description" "England", "Indicator value" "85.7", "Period of coverage" "July 2013 to March 2014", "Numerator" "757456.1"} {"Level" "England", "Denominator" "948507.5", "Breakdown" "England", "Question response rate" "97.7", "_id" 2, "indicator_id" nil, "Year" "2012/13", "Level description" "England", "Indicator value" "86.7", "Period of coverage" "July 2012 to March 2013", "Numerator" "822728.3"})))))
