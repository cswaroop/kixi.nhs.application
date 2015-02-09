(ns kixi.nhs.board-report-test
  (:use clojure.test)
  (:require [kixi.nhs.board-report :as board-report]))

(deftest filter-dataset-test
  (testing "Testing filtering dataset."
    ;; Checks return a sequence of maps, filters on indicator field
    ;; and conditions, keeps indicator value & year:
    (is (= [{:indicator_value "85.7", :year "2013/14" :period_of_coverage "July 2013 to March 2014"}
            {:indicator_value "86.7", :year "2012/13" :period_of_coverage "July 2012 to March 2013"}]
           (board-report/filter-dataset
            {:indicator-id "22"
             :fields-to-extract [:indicator_value :year :period_of_coverage]
             :conditions [{:field :level :values #{"England"}}]
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
                  :fields-to-extract [:value :year :period_of_coverage]
                  :conditions [{:field :level :values #{"England"}}]
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
                 :fields-to-extract [:indicator_value :year :period_of_coverage]
                 :conditions [{:field :level :values #{"Scotland"}}]
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
                                       :fields-to-extract [:indicator_value :year :period_of_coverage]
                                       :conditions [{:field :level :values #{"England"}}]
                                       :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
                                      [])))))

(deftest enrich-dataset-test
  (testing "Testing enriching dataset."
    ;; Checks the :indicator_value keyword is renamed to :value,
    ;; the "Indicator id" is added, and the result is a vector:
    (is (= [{:_id 1, :question_response_rate "97.9", :indicator_id "22",
             :indicator_value "85.7", :breakdown "England", :level_description "England",
             :level "England", :numerator "757456.1", :year "2013/14",
             :denominator "883852.0", :period_of_coverage "July 2013 to March 2014"}
            {:_id 2, :question_response_rate "97.7", :indicator_id "22",
             :indicator_value "86.7", :breakdown "England", :level_description "England",
             :level "England", :numerator "822728.3", :year "2012/13",
             :denominator "948507.5", :period_of_coverage "July 2012 to March 2013"}
            {:_id 3, :question_response_rate "97.3", :indicator_id "22",
             :indicator_value "88.3", :breakdown "England", :level_description "England",
             :level "Wales", :numerator "891213.9", :year "2011/12",
             :denominator "1009576.2", :period_of_coverage "July 2011 to March 2012"}]
           (board-report/enrich-dataset
            {:indicator-id "22"
             :fields-to-extract [:indicator_value :year :period_of_coverage]
             :conditions [{:field :level :values #{"Scotland"}}]
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
    (is (= [{:indicator_id "22" :indicator_value "85.7" :year "2013/14" :period_of_coverage "July 2013 to March 2014"}
            {:indicator_id "22" :indicator_value "86.7" :year "2012/13" :period_of_coverage "July 2012 to March 2013"}
            {:indicator_id "22" :indicator_value "88.3" :year "2011/12" :period_of_coverage "July 2011 to March 2012"} ]
           (board-report/enrich-dataset
            {:indicator-id "22"
             :fields-to-extract [:indicator_value :year :period_of_coverage]
             :conditions [{:field :level :values #{"England"}}]
             :resource-id "2b10e7b4-c799-44f9-81d6-3a42f4260893"}
            [{:year "2013/14" :indicator_value "85.7" :period_of_coverage "July 2013 to March 2014"}
             {:year "2012/13" :indicator_value "86.7" :period_of_coverage "July 2012 to March 2013"}
             {:year "2011/12" :indicator_value "88.3" :period_of_coverage "July 2011 to March 2012"}])))))

(deftest sum-sequence-test
  (testing "Testing adding up values for a given key"
    (is (= {:year "2012/13" :sum 50263.6 :period_of_coverage "July 2012 to March 2013"}
           (board-report/sum-sequence
            [{:denominator "49962.0" :period_of_coverage "July 2012 to March 2013" :year "2012/13"}
             {:denominator "301.6" :period_of_coverage "July 2012 to March 2013" :year "2012/13"}]
            :denominator)))
    (is (= {:year "2012" :sum 15 :period_of_coverage nil}
           (board-report/sum-sequence
            [{:denominator "10" :period_of_coverage nil :year "2012"}
             {:denominator "5" :period_of_coverage nil :year "2012"}]
            :denominator)))
    (is (= {:year "2012" :sum nil :period_of_coverage nil}
           (board-report/sum-sequence
            [{:denominator "" :year "2012"}
             {:denominator "" :year "2012"}]
            :denominator)))
    (is (= {:year "2012" :sum nil :period_of_coverage nil}
           (board-report/sum-sequence
            [{:denominator nil :year "2012"}
             {:denominator nil :year "2012"}]
            :denominator)))
    (is (= {:year "2012" :sum 1 :period_of_coverage nil}
           (board-report/sum-sequence
            [{:denominator "1" :year "2012"}
             {:denominator nil :year "2012"}]
            :denominator)))))

(deftest divide-seqs-test
  (testing "Testing division of two sequences"
    (is (= {:year "2012/13" :division-result (float 0.08128504) :period_of_coverage "July 2012 to March 2013"}
           (board-report/divide-seqs {:year "2012/13" :sum 7206.0 :period_of_coverage "July 2012 to March 2013"}
                                     {:year "2012/13" :sum 88651 :period_of_coverage "July 2012 to March 2013"})))
    (is (= {:year "2012/13" :division-result 4.0 :period_of_coverage "July 2012 to March 2013"}
           (board-report/divide-seqs {:year "2012/13" :sum 8 :period_of_coverage "July 2012 to March 2013"}
                                     {:year "2012/13" :sum 2 :period_of_coverage "July 2012 to March 2013"})))
    (is (= {:year "2012/13" :division-result nil :period_of_coverage nil}
           (board-report/divide-seqs {:year "2012/13" :sum nil :period_of_coverage nil}
                                     {:year "2012/13" :sum 2 :period_of_coverage nil})))
    (is (= {:year "2012/13" :division-result nil :period_of_coverage nil}
           (board-report/divide-seqs {:year "2012/13" :sum nil :period_of_coverage nil}
                                     {:year "2012/13" :sum nil :period_of_coverage nil})))))

(deftest subtract-seqs-test
  (testing "Testing subtracting"
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value "-0.79871494"}
           (board-report/subtract-seqs {:division-result 0.08128504
                                        :indicator_value 88 :period_of_coverage "July 2012 to March 2013"
                                        :year "2012/13"})))
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value "9.95"}
           (board-report/subtract-seqs {:division-result 10.0
                                        :indicator_value 5 :period_of_coverage "July 2012 to March 2013"
                                        :year "2012/13"})))
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value nil}
           (board-report/subtract-seqs {:division-result 10.0
                                        :indicator_value nil :period_of_coverage "July 2012 to March 2013"
                                        :year "2012/13"})))
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value nil}
           (board-report/subtract-seqs {:division-result nil
                                        :indicator_value nil :period_of_coverage "July 2012 to March 2013"
                                        :year "2012/13"})))))

(deftest final-dataset-test
  (testing "Testing final dataset calc"
    (is (= [{:indicator_id "213"
             :value "-0.09895137"
             :year "2013/14"
             :period_of_coverage "July 2013 to March 2014"}
            {:indicator_id "213"
             :value "-0.10988522"
             :year "2012/13"
             :period_of_coverage "July 2012 to March 2013"}]
           (board-report/final-dataset "213"
                                       [{:year "2013/14" :period_of_coverage "July 2013 to March 2014" :sum 37801.2}
                                        {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :sum 41031.1}]
                                       [{:year "2013/14" :period_of_coverage "July 2013 to March 2014" :sum 49153.2}
                                        {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :sum 53279.2}]
                                       [{:indicator_value 86.8 :period_of_coverage "July 2013 to March 2014" :year "2013/14"}
                                        {:indicator_value 88 :period_of_coverage "July 2012 to March 2013" :year "2012/13"}])))
    (is (= [{:indicator_id "213"
             :value nil
             :year "2013/14"
             :period_of_coverage "July 2013 to March 2014"}
            {:indicator_id "213"
             :value nil
             :year "2012/13"
             :period_of_coverage "July 2012 to March 2013"}]
           (board-report/final-dataset "213"
                                       [{:year "2013/14" :period_of_coverage "July 2013 to March 2014" :sum 37801.2}
                                        {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :sum nil}]
                                       [{:year "2013/14" :period_of_coverage "July 2013 to March 2014" :sum 49153.2}
                                        {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :sum 0}]
                                       [{:indicator_value nil :period_of_coverage "July 2013 to March 2014" :year "2013/14"}
                                        {:indicator_value nil :period_of_coverage "July 2012 to March 2013" :year "2012/13"}])))))


(deftest all-fields-exist?-test
  (testing "Testing function to check whether all fields a"
    (is (board-report/all-fields-exist? [:year :id] {:year "2014" :id "5" :value 67}))
    (is (not (board-report/all-fields-exist? [:year :id] {:year "2014" :value 67})))))

(deftest deprivation-groups-avg-test
  (testing "Testing deprivation groups averages"
    (let [data [{:level "1" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "83.6"}
                {:level "2" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "83.5"}
                {:level "3" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "84"}
                {:level "4" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "85"}
                {:level "5" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "85.6"}
                {:level "6" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "86.4"}
                {:level "7" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "87.2"}
                {:level "8" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "87.1"}
                {:level "9" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "87.3"}
                {:level "10" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "87.5"}
                {:level "Unknown" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "88.1"}]]
      (is (= [{:level 1,
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value (float 83.55)}
              {:level 2
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value (float 84.5)}
              {:level 3
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value (float 86.0)}
              {:level 4
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value (float 87.15)}
              {:level 5
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value (float 87.4)}]
             (vec (board-report/deprivation-groups-avg data)))))))

(deftest get-slop-test
  (testing "Testing slope calculation"
    (is (= {:year "2013/14", :period_of_coverage "July 2013 to March 2014", :slope 5.1749999999999545}
           (board-report/get-slope [{:level 1 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 83.55}
                                    {:level 2 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 84.5}
                                    {:level 3 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 86.0}
                                    {:level 4 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 87.15}
                                    {:level 5 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 87.4}])))
    (is (= {:year "2013/14", :period_of_coverage "July 2013 to March 2014", :slope -2.6200000000000045}
           (board-report/get-slope [{:level 1 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 10.59}
                                    {:level 2 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.79}
                                    {:level 3 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.21}
                                    {:level 4 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 8.83}
                                    {:level 5 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 8.45}])))
    (is (= {:year "2013/14", :period_of_coverage "July 2013 to March 2014", :slope -2.8049999999999997}
           (board-report/get-slope [{:level 1 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 11.47}
                                    {:level 2 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 10.66}
                                    {:level 3 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.92}
                                    {:level 4 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.53}
                                    {:level 5 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.23}])))))

(deftest divide-slope-by-median-test
  (testing "Testing dividing the slope value by the indicator value of deprivation group 3."
    (is (= (float -0.2844734)
           (:division (board-report/divide-slope-by-median {:slope -2.62 :median_indicator_value 9.21 :year "2002/03"
                                                            :period_of_coverage ""}))))
    (is (= (float -0.27806926)
           (:division (board-report/divide-slope-by-median {:slope -2.65 :median_indicator_value 9.53 :year "2002/03"
                                                            :period_of_coverage ""}))))
    (is (= (float -0.2827621)
           (:division (board-report/divide-slope-by-median {:slope -2.805 :median_indicator_value 9.92 :year "2002/03"
                                                            :period_of_coverage ""}))))))

(deftest deprivation-analysis-test
  (let [data [[{:level 1 :year "2002/03" :indicator_value 10.59}
               {:level 2 :year "2002/03" :indicator_value 9.79}
               {:level 3 :year "2002/03" :indicator_value 9.21}
               {:level 4 :year "2002/03" :indicator_value 8.83}
               {:level 5 :year "2002/03" :indicator_value 8.45}]
              [{:level 1 :year "2003/04" :indicator_value 10.96}
               {:level 2 :year "2003/04" :indicator_value 10.05}
               {:level 3 :year "2003/04" :indicator_value 9.53}
               {:level 4 :year "2003/04" :indicator_value 9.13}
               {:level 5 :year "2003/04" :indicator_value 8.77}]
              [{:level 1 :year "2004/05" :indicator_value 11.47}
               {:level 2 :year "2004/05" :indicator_value 10.66}
               {:level 3 :year "2004/05" :indicator_value 9.92}
               {:level 4 :year "2004/05" :indicator_value 9.53}
               {:level 5 :year "2004/05" :indicator_value 9.23}]
              [{:level 1 :year "2005/06" :indicator_value 11.84}
               {:level 2 :year "2005/06" :indicator_value 11.01}
               {:level 3 :year "2005/06" :indicator_value 10.38}
               {:level 4 :year "2005/06" :indicator_value 9.95}
               {:level 5 :year "2005/06" :indicator_value 9.57}]
              [{:level 1 :year "2006/07" :indicator_value 12.07}
               {:level 2 :year "2006/07" :indicator_value 11.22}
               {:level 3 :year "2006/07" :indicator_value 10.49}
               {:level 4 :year "2006/07" :indicator_value 9.93}
               {:level 5 :year "2006/07" :indicator_value 9.69}]
              [{:level 1 :year "2007/08" :indicator_value 12.23}
               {:level 2 :year "2007/08" :indicator_value 11.32}
               {:level 3 :year "2007/08" :indicator_value 10.61}
               {:level 4 :year "2007/08" :indicator_value 10.25}
               {:level 5 :year "2007/08" :indicator_value 9.96}]
              [{:level 1 :year "2008/09" :indicator_value 12.57}
               {:level 2 :year "2008/09" :indicator_value 11.59}
               {:level 3 :year "2008/09" :indicator_value 11.05}
               {:level 4 :year "2008/09" :indicator_value 10.55}
               {:level 5 :year "2008/09" :indicator_value 10.31}]
              [{:level 1 :year "2009/10" :indicator_value 12.8}
               {:level 2 :year "2009/10" :indicator_value 11.89}
               {:level 3 :year "2009/10" :indicator_value 11.32}
               {:level 4 :year "2009/10" :indicator_value 10.85}
               {:level 5 :year "2009/10" :indicator_value 10.52}]
              [{:level 1 :year "2010/11" :indicator_value 13.11}
               {:level 2 :year "2010/11" :indicator_value 12.11}
               {:level 3 :year "2010/11" :indicator_value 11.42}
               {:level 4 :year "2010/11" :indicator_value 10.98}
               {:level 5 :year "2010/11" :indicator_value 10.66}]
              [{:level 1 :year "2011/12" :indicator_value 13.05}
               {:level 2 :year "2011/12" :indicator_value 12.24}
               {:level 3 :year "2011/12" :indicator_value 11.48}
               {:level 4 :year "2011/12" :indicator_value 10.98}
               {:level 5 :year "2011/12" :indicator_value 10.53}]]]
    (testing "Testing deprivation analysis"
      (is (= [{:indicator_id "212" :year "2011/12" :period_of_coverage nil :value (str (float 0.0393156))}
              {:indicator_id "212a" :year "2011/12" :period_of_coverage nil :value (str (float -0.048478067))}]
             (board-report/deprivation-analysis data))))))
