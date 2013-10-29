(ns paralab.benchmarks
   (:require 
    [paralab.fj-tasks :refer :all]
    [paralab.fj-reducers :as fjr]
    [clojure.math.numeric-tower :refer [expt]]
    [criterium.core :refer [quick-benchmark benchmark]])
  (:gen-class))

(defn task1 [fjpool data]
  (fjr/fold-p fjpool +   (fjr/map #(* 3 %) data)))

(defn task2* [data]
  (into [] (filter odd? (map #(* 3 %) data))))

(defn task2 [fjpool data]
  (fjr/fold-into-vec-p fjpool (fjr/filter odd? (fjr/map #(* 3 %) data))))

(defn get-time [b]
  (first (:mean b)))

(defn main-fast [& args]
  (let [
        data (into [] (range 1 1000000))
        fjpool1 (create-fjpool 1)
        fjpool2 (create-fjpool 2)
        b-t2-p0 (time (task2* data))
        b-t2-p1 (time (task2 fjpool1 data))
        b-t2-p2 (time (task2 fjpool2 data))
       ]
    nil))

(defn main-slow [& args]
  (let [
        data (into [] (range 1 10000))
        fjpool1 (create-fjpool 1)
        fjpool2 (create-fjpool 2)
        b-t2-p0 (quick-benchmark (task2* data) {})
        b-t2-p1 (quick-benchmark (task2 fjpool1 data) {})
        b-t2-p2 (quick-benchmark  (task2 fjpool2 data) {})
       ]
    (println (get-time b-t2-p0))
    (println (get-time b-t2-p1))
    (println (get-time b-t2-p2))))

(defn -main [& args]
  (main-slow args))
