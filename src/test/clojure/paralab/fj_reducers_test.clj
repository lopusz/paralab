(ns paralab.fj-reducers-test
    (:require 
            [clojure.test :refer :all]
            [paralab.fj-tasks :refer [make-fjpool]]
            [paralab.fj-reducers :as fjr]))

(deftest fj-reduce-test
  (let [
        fjpool (make-fjpool)
        data (into [] (range 1 100001))
        res* (reduce + (filter odd? (map #(* 2 %) data)))
        res (fjr/fold-p fjpool 
               + 
              (fjr/filter odd? (fjr/map #(* 2 %) data)))
        ]
    (is (= res res*))))

(deftest fj-fold-into-vec-test
  (let [
        fjpool (make-fjpool)
        res* (into [] (range 4 1000 4))
        data (into [] (range 1 500))
        res  (fjr/fold-into-vec-p
                fjpool
                  (fjr/map #(* 2 %)
                    (fjr/filter even? data)))
        ]
    (is (= res* res))))