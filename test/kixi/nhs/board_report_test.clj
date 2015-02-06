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
  (testing "Testing function to check whether all fields are present in the data"
    (is (board-report/all-fields-exist? [:year :id] {:year "2014" :id "5" :value 67}))
    (is (not (board-report/all-fields-exist? [:year :id] {:year "2014" :value 67})))))
