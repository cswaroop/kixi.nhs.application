(ns kixi.nhs.patient-experience.ethnicity-test
  (:use clojure.test)
  (:require [kixi.nhs.patient-experience.ethnicity :as ethnicity]))

(deftest divide-maps-test
  (testing "Testing division of two maps"
    (is (= {:year "2012/13" :division-result (float 0.08128504) :period_of_coverage "July 2012 to March 2013"}
           (ethnicity/divide-maps {:year "2012/13" :sum 7206.0 :period_of_coverage "July 2012 to March 2013"}
                                               {:year "2012/13" :sum 88651 :period_of_coverage "July 2012 to March 2013"})))
    (is (= {:year "2012/13" :division-result 4.0 :period_of_coverage "July 2012 to March 2013"}
           (ethnicity/divide-maps {:year "2012/13" :sum 8 :period_of_coverage "July 2012 to March 2013"}
                                               {:year "2012/13" :sum 2 :period_of_coverage "July 2012 to March 2013"})))
    (is (= {:year "2012/13" :division-result nil :period_of_coverage nil}
           (ethnicity/divide-maps {:year "2012/13" :sum nil :period_of_coverage nil}
                                               {:year "2012/13" :sum 2 :period_of_coverage nil})))
    (is (= {:year "2012/13" :division-result nil :period_of_coverage nil}
           (ethnicity/divide-maps {:year "2012/13" :sum nil :period_of_coverage nil}
                                               {:year "2012/13" :sum nil :period_of_coverage nil})))))

(deftest subtract-indicator-value-test
  (testing "Testing subtracting"
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value "-0.79871494"}
           (ethnicity/subtract-indicator-value {:division-result 0.08128504
                                                :indicator_value 88 :period_of_coverage "July 2012 to March 2013"
                                                :year "2012/13"})))
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value "9.95"}
           (ethnicity/subtract-indicator-value {:division-result 10.0
                                                :indicator_value 5 :period_of_coverage "July 2012 to March 2013"
                                                :year "2012/13"})))
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value nil}
           (ethnicity/subtract-indicator-value {:division-result 10.0
                                                :indicator_value nil :period_of_coverage "July 2012 to March 2013"
                                                :year "2012/13"})))
    (is (= {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :value nil}
           (ethnicity/subtract-indicator-value {:division-result nil
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
           (ethnicity/final-dataset "213"
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
           (ethnicity/final-dataset "213"
                                    [{:year "2013/14" :period_of_coverage "July 2013 to March 2014" :sum 37801.2}
                                     {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :sum nil}]
                                    [{:year "2013/14" :period_of_coverage "July 2013 to March 2014" :sum 49153.2}
                                     {:year "2012/13" :period_of_coverage "July 2012 to March 2013" :sum 0}]
                                    [{:indicator_value nil :period_of_coverage "July 2013 to March 2014" :year "2013/14"}
                                     {:indicator_value nil :period_of_coverage "July 2012 to March 2013" :year "2012/13"}])))))
