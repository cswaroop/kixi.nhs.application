(ns kixi.nhs.patient-experience.deprivation-test
  (:use clojure.test)
  (:require [kixi.nhs.patient-experience.deprivation :as deprivation]))

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
               :indicator_value 83.55}
              {:level 2
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value 84.5}
              {:level 3
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value 86.0}
              {:level 4
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value 87.15}
              {:level 5
               :period_of_coverage "July 2013 to March 2014"
               :year "2013/14"
               :indicator_value 87.4}]
             (vec (deprivation/deprivation-groups-avg data)))))))

(deftest slope-test
  (testing "Testing slope calculation"
    (is (= {:year "2013/14", :period_of_coverage "July 2013 to March 2014", :slope 5.1749999999999545}
           (deprivation/slope [{:level 1 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 83.55}
                               {:level 2 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 84.5}
                               {:level 3 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 86.0}
                               {:level 4 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 87.15}
                               {:level 5 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 87.4}])))
    (is (= {:year "2013/14", :period_of_coverage "July 2013 to March 2014", :slope -2.6200000000000045}
           (deprivation/slope [{:level 1 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 10.59}
                               {:level 2 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.79}
                               {:level 3 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.21}
                               {:level 4 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 8.83}
                               {:level 5 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 8.45}])))
    (is (= {:year "2013/14", :period_of_coverage "July 2013 to March 2014", :slope -2.8049999999999997}
           (deprivation/slope [{:level 1 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 11.47}
                               {:level 2 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 10.66}
                               {:level 3 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.92}
                               {:level 4 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.53}
                               {:level 5 :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value 9.23}])))))

(deftest divide-slope-by-median-test
  (testing "Testing dividing the slope value by the indicator value of deprivation group 3."
    (is (= (float -0.2844734)
           (:division (deprivation/divide-slope-by-median {:slope -2.62 :median_indicator_value 9.21 :year "2002/03"
                                                           :period_of_coverage ""}))))
    (is (= (float -0.27806926)
           (:division (deprivation/divide-slope-by-median {:slope -2.65 :median_indicator_value 9.53 :year "2002/03"
                                                           :period_of_coverage ""}))))
    (is (= (float -0.2827621)
           (:division (deprivation/divide-slope-by-median {:slope -2.805 :median_indicator_value 9.92 :year "2002/03"
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
             (deprivation/deprivation-analysis data))))))
