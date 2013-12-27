(ns paralab.fj-tasks
  " Simple Clojure interface to ForkJoin task pool.

    On the basis of gist from swannodette:
    https://gist.github.com/888733"
  (:refer-clojure :exclude [assert])
  (:require 
    [pjstadig.assertions :refer [assert]]
    [paralab.fj-core :refer :all]))

;;(set! *warn-on-reflection* true)

;; Java 1.6 vs. Java 1.7 compatibility trick from Reducers library

(defn split-vector-halves [ v ]
  (let [
       half (quot (count v) 2)
       ]
    [ (subvec v 0 half) (subvec v half) ]))

(defn- priv-fj-run
  [ fj-task ]

  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fj-task
      ]
    (if (> (size-f data) size-threshold)
      (let [
          [data1 data2] (split-f data)
          fj-task1 (merge fj-task {:data data1})
          fj-task2 (merge fj-task {:data data2})
          f-res1 (fork (task (priv-fj-run fj-task1)))
          res2 (run (task (priv-fj-run fj-task2)))
          ]
        (merge-f (join f-res1) res2))
        (process-f data))))

(defn make-vec-fj-task [ fj-task ]
  (assert (contains? fj-task :data))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :merge-f))

  (let [
         vector-fj-task {
                    :size-threshold 1
                    :size-f count
                    :split-f split-vector-halves }
        ]
    (merge vector-fj-task fj-task)))

(defn fj-run [ fj-pool fj-task ]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :merge-f))
  (assert (contains? fj-task :data))
  (invoke fj-pool (task (priv-fj-run fj-task))))

(defn- priv-fj-run!
  [ fj-task ]
  (let [
        {:keys [ size-threshold size-f
                 split-f process-f data ] } fj-task
      ]
    (if (> (size-f data) size-threshold)
      (let [
          [data1 data2] (split-f data)
          fj-task1 (merge fj-task {:data data1})
          fj-task2 (merge fj-task {:data data2})
          f-res1 (fork (task (priv-fj-run! fj-task1)))
          res2 (run (task (priv-fj-run! fj-task2)))
          ]
        (join f-res1)
        nil)
      (do (process-f data) nil))))

(defn fj-run!
  "Used to run fj-tasks for side effects only.
   The only results are written to the output by process-f.
   Any results returned by process-f are discarded, preferably it should
   return  nil.
   There is no need for merge-f function required by ordinary fj-run.
   Doing blocking I/O is against the rules of fork-join and can result
   in suboptimal performance."
  [ fj-pool fj-task ]

  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :data))

  (invoke fj-pool (task (priv-fj-run! fj-task))))

(defn- priv-fj-run-serial
  [ fj-task ]
  (let [
         {:keys [ size-threshold size-f
                  split-f process-f merge-f data ] } fj-task
       ]
       (if (> (size-f data) size-threshold)
         (let [
               [data1 data2] (split-f data)
               fj-task1 (merge fj-task {:data data1})
               fj-task2 (merge fj-task {:data data2})
               res1 (priv-fj-run-serial fj-task1)
               res2 (priv-fj-run-serial fj-task2)
              ]
           (merge-f res1 res2))
         (process-f data))))

(defn fj-run-serial
  [ fj-task ]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :merge-f))
  (assert (contains? fj-task :data))
  (priv-fj-run-serial fj-task))

(defn- priv-fj-run-serial!
  [ fj-task ]
  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fj-task
      ]
      (if (> (size-f data) size-threshold)
        (let [
              [data1 data2] (split-f data)
              fj-task1 (merge fj-task {:data data1})
              fj-task2 (merge fj-task {:data data2})
              res1 (priv-fj-run-serial! fj-task1)
              res2 (priv-fj-run-serial! fj-task2)
             ]
          nil)
        (do
          (process-f data)
          nil))))

(defn fj-run-serial!
  "Runs `fj-task` for sideffects only. Returns nil."
  [ fj-task ]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :data))
  (priv-fj-run-serial! fj-task))
