(ns paralab.fj-reducers-test
  (:require 
    [clojure.test :refer :all]
    [paralab.fj-core :refer [make-fj-pool]]
    [paralab.fj-reducers :as fjr]))

(deftest fj-reduce-test
  (let [
        fj-pool (make-fj-pool)
        data (into [] (range 1 100001))
        res* (reduce + (filter odd? (map #(* 2 %) data)))
        res (fjr/fold-p fj-pool 
               + 
              (fjr/filter odd? (fjr/map #(* 2 %) data)))
        ]
    (is (= res res*))))

(deftest fj-fold-into-vec-test
  (let [
        fj-pool (make-fj-pool)
        res* (into [] (range 4 1000 4))
        data (into [] (range 1 500))
        res  (fjr/fold-into-vec-p
                fj-pool
                  (fjr/map #(* 2 %)
                    (fjr/filter even? data)))
        ]
    (is (= res* res))))