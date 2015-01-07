(ns kixi.nhs.data.transform-test
  (:use clojure.test)
  (:require [kixi.nhs.data.transform :as transform]))

(deftest parse-number
  (is (= 21
         (transform/parse-number "21")))
  (is (= 0.01
         (transform/parse-number "0.01")))
  (is (= 2000
         (transform/parse-number "2,000")))
  (is (= 2000.1
         (transform/parse-number "2,000.1"))))

(deftest split-by-field-value-test
  (is (= {2009 [{:v 0, :year 2009}], 2011 [{:v 3, :year 2011} {:v 7, :year 2011}], 2010 [{:v 5, :year 2010} {:v 4, :year 2010}]}
         (transform/split-by-field-value :year
                                         [{:year 2010 :v 5} {:year 2010 :v 4} {:year 2011 :v 3}
                                          {:year 2011 :v 7} {:year 2009 :v 0}]))))

(deftest outer-join-test
  (is (= [{:common-field "A" "01" 1 "02" 3} {:common-field "B" "01" 2 "02" 4} {:common-field "C" "02" 1}]
         (transform/outer-join :k
                               (fn [k data] (let [d (get data k)]
                                              (when (seq d)
                                                (hash-map :common-field (:k d)
                                                          (:id d) (:v d)))))
                               [[{:v 1 :id "01" :k "A"} {:v 2 :id "01" :k "B"}]
                                [{:v 3 :id "02" :k "A"} {:v 4 :id "02" :k "B"} {:v 1 :id "02" :k "C"}]]))))

(deftest remove-ids-test
  (is (= [{"Level" "National", "DSR" "70", "Breakdown" "National", "Gender" "Person", "CI Upper" "70.7", "Level Description" "All registered patients in England", "Observed" "36,265", "Year" "2009", "CI Lower" "69.2", "Registered patients" "51,450,031"}
          {"Level" "National", "DSR" "45.2", "Breakdown" "National", "Gender" "Female", "CI Upper" "46", "Level Description" "All registered patients in England", "Observed" "11,502", "Year" "2009", "CI Lower" "44.3", "Registered patients" "25,450,302"}]
         (transform/remove-ids [{"Level" "National", "DSR" "70", "Breakdown" "National", "Gender" "Person", "CI Upper" "70.7", "Level Description" "All registered patients in England", "Observed" "36,265", "_id" 13, "Year" "2009", "CI Lower" "69.2", "Registered patients" "51,450,031"} {"Level" "National", "DSR" "45.2", "Breakdown" "National", "Gender" "Female", "CI Upper" "46", "Level Description" "All registered patients in England", "Observed" "11,502", "_id" 14, "Year" "2009", "CI Lower" "44.3", "Registered patients" "25,450,302"}]))))
