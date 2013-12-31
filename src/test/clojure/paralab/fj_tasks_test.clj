(ns paralab.fj-tasks-test
  (:require [clojure.test :refer :all]
            [paralab.fj-core :refer [ make-fj-pool ]]
            [paralab.fj-tasks :refer :all]))

(deftest run-fj-task-test
  (let [
        fj-pool (make-fj-pool)
        fj-task (make-fj-task
                  :size-threshold 5
                  :size-f count
                  :split-f split-vector-halves
                  :process-f  #(reduce + %)
                  :merge-f #(+ %1 %2)
                  :data (into [] (range 1 100001)))
        ]
    (is
     (= (run-fj-task fj-pool fj-task) 5000050000))
    (is
     (= (run-fj-task-serial fj-task) 5000050000))))

(deftest run-fj-task-with-make-vec-test
  (let [
        fj-pool (make-fj-pool)
        fj-task (make-fj-task-vec
                  :process-f  #(reduce + %)
                  :merge-f #(+ %1 %2)
                  :data (into [] (range 1 100001)))
        ]
    (is
     (= (run-fj-task fj-pool fj-task) 5000050000))
    (is
     (= (run-fj-task-serial fj-task) 5000050000))))

(deftest run-fj-task-with-make-map-reduce-vec-test
  (let [
        fj-pool (make-fj-pool)
        fj-task (make-fj-task-map-reduce-vec
                 :map-f  identity
                  :reduce-f #(+ %1 %2)
                  :data (into [] (range 1 100001)))
        ]
    (is
     (= (run-fj-task fj-pool fj-task) 5000050000))
    (is
     (= (run-fj-task-serial fj-task) 5000050000))))

(deftest run-fj-task!-test
  (let [
          counter (atom 0)
          fj-pool (make-fj-pool)
          fj-task (make-fj-task
                   :size-threshold 5
                   :size-f count
                   :split-f split-vector-halves
                   :process-f
                     #(swap! counter
                           +  (reduce + %))
                    :data (into [] (range 1 100001)))
          exit-val-par (run-fj-task! fj-pool fj-task)
          res-par @counter
          _ (reset! counter 0)
          exit-val-ser (run-fj-task-serial! fj-task)
          res-ser @counter
       ]
    (is (= exit-val-par nil))
    (is (= res-par 5000050000))
    (is (= exit-val-ser nil))
    (is (= res-ser 5000050000))))

(deftest run-fj-task!-with-make-vec-test
  (let [
          counter (atom 0)
          fj-pool (make-fj-pool)
          fj-task (make-fj-task-vec
                   :size-threshold 5
                   :process-f
                     #(swap! counter
                           +  (reduce + %))
                    :data (into [] (range 1 100001)))
          exit-val-par (run-fj-task! fj-pool fj-task)
          res-par @counter
          _ (reset! counter 0)
          exit-val-ser (run-fj-task-serial! fj-task)
          res-ser @counter
       ]
    (is (= exit-val-par nil))
    (is (= res-par 5000050000))
    (is (= exit-val-ser nil))
    (is (= res-ser 5000050000))))
