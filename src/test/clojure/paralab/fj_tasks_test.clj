(ns paralab.fj-tasks-test
  (:require [clojure.test :refer :all]
            [paralab.fj-tasks :refer :all]))

(deftest fj-job-test
  (let [
        fjpool (create-fjpool) 
        fjtask {
                  :size-threshold 5 
                  :size-f count
                  :split-f split-vector
                  :process-f  #(reduce + %)
                  :merge-f #(+ %1 %2)
                  :data (into [] (range 1 100001))
                 }
        ]
    (is 
     (= (fj-run fjpool fjtask) 5000050000))
    (is 
     (= (fj-run-serial fjtask) 5000050000))))