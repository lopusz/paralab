(ns paralab.pmap-n
  (:require [clojure.math.numeric-tower :refer [ceil]]))

(defn pmap-n
  "Runs pmap spawning n futures.
   
   Original pmap is recovered by pmap with n = available processors + 2.
  "

  ([ n f coll ]
   (let [
         rets (map #(future (f %)) coll)
         step (fn step [[x & xs :as vs] fs]
                (lazy-seq
                 (if-let [s (seq fs)]
                   (cons (deref x) (step xs (rest s)))
                   (map deref vs))))]
     (step rets (drop n rets))))
  ([ n f coll & colls]
   (let [step (fn step [cs]
                (lazy-seq
                 (let [ss (map seq cs)]
                   (when (every? identity ss)
                     (cons (map first ss) (step (map rest ss)))))))]
     (pmap-n n  #(apply f %) (step (cons coll colls))))))


(defn ppmap-n [ n f coll ]
  (let [
        coll-size (count coll)
        partition-size (int (ceil (/ coll-size n)))
        coll-partitions (partition-all partition-size coll)
        f-partitions #(doall (map f %))
        res-partitions (pmap-n n f-partitions coll-partitions)
       ]
    (apply concat res-partitions)))

;; These are routines contributed by Andy Fingerhut to The Computer Language 
;; Benchmarks Game, see
;; http://benchmarksgame.alioth.debian.org/u32/program.php?test=knucleotide&lang=clojure&id=2

;; modified-pmap is like pmap from Clojure 1.1, but with only as much
;; parallelism as specified by the parameter num-threads.  Uses
;; my-lazy-map instead of map from core.clj, since that version of map
;; can use unwanted additional parallelism for chunked collections,
;; like ranges.

(defn- af-lazy-map [f coll]
  (lazy-seq
    (when-let [s (seq coll)]
      (cons (f (first s)) (af-lazy-map f (rest s))))))

(defn af-pmap-n
  ([num-threads f coll]
     (if (== num-threads 1)
       (map f coll)
       (let [n (if (>= num-threads 2) (dec num-threads) 1)
             rets (af-lazy-map #(future (f %)) coll)
             step (fn step [[x & xs :as vs] fs]
                    (lazy-seq
                      (if-let [s (seq fs)]
                        (cons (deref x) (step xs (rest s)))
                        (map deref vs))))]
         (step rets (drop n rets)))))
  ([num-threads f coll & colls]
     (let [step (fn step [cs]
                  (lazy-seq
                    (let [ss (af-lazy-map seq cs)]
                      (when (every? identity ss)
                        (cons (af-lazy-map first ss)
			      (step (af-lazy-map rest ss)))))))]
       (af-pmap-n num-threads #(apply f %) (step (cons coll colls))))))
