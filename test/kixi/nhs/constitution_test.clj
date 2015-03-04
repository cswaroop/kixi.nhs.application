(ns kixi.nhs.constitution-test
  (:use clojure.test)
  (:require [kixi.nhs.constitution :as c]))

(deftest total-test
  (testing "Testing calculating total for a sequence of data."
    (is (= 20
           (c/total :v [{:v "10"} {:b "4" :v "5"} {:b "a" :v "5"}])))
    (is (= 15
           (c/total :v [{:v "10"} {:b "4" :v ""} {:b "a" :v "5"}])))
    (is (= 15
           (c/total :v [{:v "10"} {:b "4" :v nil} {:b "a" :v "5"}])))
    (is (= 0
           (c/total :v [{:b "4"} {:b "a"}])))))

(deftest percentage-seen-within-x-days-test
  (testing "Testing calulation of percentage seen within x days."
    (let [fields   [:after_62_days :total_treated]
          metadata {:year "2014/2015" :period_of_coverage "01/04/2014 - 30/06/2014"}
          data     [{:area_team "Foo"
                     :total_treated "10"
                     :after_62_days "2"
                     :within_62_days "8"
                     :area_team_code_1 "123"}
                    {:area_team "Foo"
                     :total_treated "10"
                     :after_62_days "2"
                     :within_62_days "8"
                     :area_team_code_1 "123"}]]
      (is (= {:value "0.2"
              :breakdown "Area Team Code"
              :level "123"
              :year "2014/2015"
              :period_of_coverage "01/04/2014 - 30/06/2014"}
             (c/percentage-seen-within-x-days fields
                                              metadata
                                              "Area Team Code"
                                              "123" "Foo"
                                              data))))))

(deftest per-team-area-test
  (testing "Testing calculation per team area."
    (let [fields   [:after_62_days :total_treated]
          metadata {:year "2014/2015" :period_of_coverage "01/04/2014 - 30/06/2014"}
          data     [{:area_team "Foo"
                     :total_treated "10"
                     :after_62_days "2"
                     :within_62_days "8"
                     :area_team_code_1 "123"}
                    {:area_team "Foo"
                     :total_treated "10"
                     :after_62_days "2"
                     :within_62_days "8"
                     :area_team_code_1 "123"}
                    {:area_team "Bar"
                     :total_treated "10"
                     :after_62_days "2"
                     :within_62_days "8"
                     :area_team_code_1 "222"}
                    {:area_team "Bar"
                     :total_treated "10"
                     :after_62_days "2"
                     :within_62_days "8"
                     :area_team_code_1 "222"}]]
      (is (= [{:value "0.2",
               :breakdown "Area Team Code",
               :level "123",
               :year "2014/2015",
               :period_of_coverage "01/04/2014 - 30/06/2014"}
              {:value "0.2",
               :breakdown "Area Team Code",
               :level "222",
               :year "2014/2015",
               :period_of_coverage "01/04/2014 - 30/06/2014"}]
             (c/per-team-area fields metadata data))))))
