(ns kixi.nhs.board-report-test
  (:use clojure.test)
  (:require [kixi.nhs.board-report :refer :all]
            [clj-time.core         :as t]))

(deftest latest-month-test
  (testing "Testing latest-month"
    (is (= (t/date-time 2015 4 14)
           (latest-month :reporting_period
                         [{:breakdown "CCG",
                           :reporting_period "2015-01-14T00:00:00",
                           :level "00C",
                           :mrsa_count "1"}
                          {:breakdown "CCG",
                           :reporting_period "2010-06-14T00:00:00",
                           :level "00D",
                           :mrsa_count "0"}
                          {:breakdown "CCG",
                           :reporting_period "2011-02-14T00:00:00",
                           :level "00F",
                           :mrsa_count "0"}
                          {:breakdown "CCG",
                           :reporting_period "2015-03-14T00:00:00",
                           :level "00G",
                           :mrsa_count "0"}
                          {:breakdown "CCG",
                           :reporting_period "2015-04-14T00:00:00",
                           :level "00H",
                           :mrsa_count "0"}])))))

(deftest enrich-test
  (testing "Testing enrich"
    (is (= {:sum "73" :year "2015" :period_of_coverage "2015-04-14T00:00:00"}
           (enrich (t/date-time 2015 4 14) {:year nil :period_of_coverage nil :sum 73})))))
