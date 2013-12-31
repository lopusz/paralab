(ns paralab.fj-tasks-test
  (:require [clojure.test :refer :all]
            [paralab.fj-core :refer [ make-fj-pool ]]
            [paralab.fj-tasks :refer :all]))

(deftest fj-run-test
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
     (= (fj-run fj-pool fj-task) 5000050000))
    (is
     (= (fj-run-serial fj-task) 5000050000))))

(deftest fj-run-with-make-vec-test
  (let [
        fj-pool (make-fj-pool)
        fj-task (make-fj-task-vec
                  :process-f  #(reduce + %)
                  :merge-f #(+ %1 %2)
                  :data (into [] (range 1 100001)))
        ]
    (is
     (= (fj-run fj-pool fj-task) 5000050000))
    (is
     (= (fj-run-serial fj-task) 5000050000))))

(deftest fj-run-with-make-map-reduce-vec-test
  (let [
        fj-pool (make-fj-pool)
        fj-task (make-fj-task-map-reduce-vec
                 :map-f  identity
                  :reduce-f #(+ %1 %2)
                  :data (into [] (range 1 100001)))
        ]
    (is
     (= (fj-run fj-pool fj-task) 5000050000))
    (is
     (= (fj-run-serial fj-task) 5000050000))))

(deftest fj-run!-test
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
          exit-val-par (fj-run! fj-pool fj-task)
          res-par @counter
          _ (reset! counter 0)
          exit-val-ser (fj-run-serial! fj-task)
          res-ser @counter
       ]
    (is (= exit-val-par nil))
    (is (= res-par 5000050000))
    (is (= exit-val-ser nil))
    (is (= res-ser 5000050000))))

(deftest fj-run!-with-make-vec-test
  (let [
          counter (atom 0)
          fj-pool (make-fj-pool)
          fj-task (make-fj-task-vec
                   :size-threshold 5
                   :process-f
                     #(swap! counter
                           +  (reduce + %))
                    :data (into [] (range 1 100001)))
          exit-val-par (fj-run! fj-pool fj-task)
          res-par @counter
          _ (reset! counter 0)
          exit-val-ser (fj-run-serial! fj-task)
          res-ser @counter
       ]
    (is (= exit-val-par nil))
    (is (= res-par 5000050000))
    (is (= exit-val-ser nil))
    (is (= res-ser 5000050000))))
