(ns kixi.nhs.patient-experience.gender-comparison-test
  (:use clojure.test)
  (:require [kixi.nhs.patient-experience.gender-comparison :as gender]))

(deftest subtract-males-from-females-test
  (testing "Testing subtracting indicator value for males from indicator value from females"
    (is (= {:year "2013/14" :period_of_coverage "July 2013 to March 2014" :value "0.7999999999999972"}
           (gender/subtract-males-from-females
            [{:level "Male" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "85.3"}
             {:level "Female" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "86.1"}])))
    (is (= {:year "2013/14" :period_of_coverage "July 2013 to March 2014" :value "1"}
           (gender/subtract-males-from-females
            [{:level "Male" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "2"}
             {:level "Female" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "3"}])))
    (is (= {:year "2013/14" :period_of_coverage "July 2013 to March 2014" :value "0"}
           (gender/subtract-males-from-females
            [{:level "Male" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "0"}
             {:level "Female" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "0"}])))
    (is (= {:year "2013/14" :period_of_coverage "July 2013 to March 2014" :value nil}
           (gender/subtract-males-from-females
            [{:level "Male" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value nil}
             {:level "Female" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value nil}])))
    (is (= {:year "2013/14" :period_of_coverage "July 2013 to March 2014" :value "-7"}
           (gender/subtract-males-from-females
            [{:level "Male" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "12"}
             {:level "Female" :period_of_coverage "July 2013 to March 2014" :year "2013/14" :indicator_value "5"}])))))

(deftest gender-analysis-test
  (let [data [{:breakdown "England",
               :level "England",
               :year "2013/14",
               :indicator_value "85.7",
               :period_of_coverage "July 2013 to March 2014"}
              {:breakdown "England",
               :level "England",
               :year "2012/13",
               :indicator_value "86.7",
               :period_of_coverage "July 2012 to March 2013"}
              {:breakdown "Gender",
               :level "Male",
               :year "2013/14",
               :indicator_value "85.3",
               :period_of_coverage "July 2013 to March 2014"}
              {:breakdown "Gender",
               :level "Female",
               :year "2013/14",
               :indicator_value "86.1",
               :period_of_coverage "July 2013 to March 2014"}
              {:breakdown "Gender",
               :level "Unknown",
               :year "2013/14",
               :indicator_value "84.8",
               :period_of_coverage "July 2013 to March 2014"}
              {:breakdown "Gender",
               :level "Male",
               :year "2012/13",
               :indicator_value "86.2",
               :period_of_coverage "July 2012 to March 2013"}
              {:breakdown "Gender",
               :level "Female",
               :year "2012/13",
               :indicator_value "87.3",
               :period_of_coverage "July 2012 to March 2013"}
              {:breakdown "Gender",
               :level "Unknown",
               :year "2012/13",
               :indicator_value "85.6",
               :period_of_coverage "July 2012 to March 2013"}]]
    (testing "Testing gender analysis calculation."
      (is (= [{:indicator_id "211" :year "2013/14", :period_of_coverage "July 2013 to March 2014", :value "0.7999999999999972"
               :breakdown "Gender" :level "Female - Male"}
              {:indicator_id "211" :year "2012/13", :period_of_coverage "July 2012 to March 2013", :value "1.0999999999999943"
               :breakdown "Gender" :level "Female - Male"}]
             (gender/gender-analysis {:indicator-id "211"
                                      :resource-id "7cb803a1-5c88-46e0-9e61-cf4c47ffadcb"
                                      :fields-to-extract [:indicator_value :year
                                                          :period_of_coverage :level]
                                      :conditions [{:field :breakdown
                                                    :values #{"Gender"}}]}
                                     data)))
      (is (= [{:indicator_id "211" :year "2011", :period_of_coverage "2011", :value "-5"
               :breakdown "Gender" :level "Female - Male"}]
             (gender/gender-analysis {:indicator-id "211"
                                      :resource-id "7cb803a1-5c88-46e0-9e61-cf4c47ffadcb"
                                      :fields-to-extract [:indicator_value :year
                                                          :period_of_coverage :level]
                                      :conditions [{:field :breakdown
                                                    :values #{"Gender"}}]}
                                     [{:breakdown "Gender" :level "Male" :year "2011" :period_of_coverage "2011"
                                       :indicator_value "15"}
                                      {:breakdown "Gender" :level "Female" :year "2011" :period_of_coverage "2011"
                                       :indicator_value "10"}]))))))
