(ns paralab.fj-tasks-test
  (:require [clojure.test :refer :all]
            [paralab.fj-tasks :refer :all]))

(deftest fj-run-test
  (let [
        fjpool (make-fjpool) 
        fjtask {
                  :size-threshold 5 
                  :size-f count
                  :split-f split-vector-halves
                  :process-f  #(reduce + %)
                  :merge-f #(+ %1 %2)
                  :data (into [] (range 1 100001))
                 }
        ]
    (is 
     (= (fj-run fjpool fjtask) 5000050000))
    (is 
     (= (fj-run-serial fjtask) 5000050000))))

(deftest fj-run!-test
  (let [
          counter (atom 0)
          fjpool (make-fjpool) 
          fjtask {
                  :size-threshold 5 
                  :size-f count
                  :split-f split-vector-halves
                  :process-f
                     #(swap! counter 
                           +  (reduce + %))
                  :data (into [] (range 1 100001))
                  }
          exit-val-par (fj-run! fjpool fjtask)
          res-par @counter
          _ (reset! counter 0)
          exit-val-ser (fj-run-serial! fjtask)
          res-ser @counter
       ]
    (is (= exit-val-par nil))
    (is (= res-par 5000050000))
    (is (= exit-val-ser nil))
    (is (= res-ser 5000050000))))

