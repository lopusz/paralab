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

(defn make-fj-task
  [ & { :keys [size-threshold size-f split-f process-f data]
       :as fj-task}]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :data))
  fj-task)

(defn make-fj-task-vec
  [ & {:keys [size-threshold process-f merge-f data]
       :or { size-threshold 2 } } ]

  (assert (not= process-f nil))
  (assert (not= data nil))
  (assert (= (class data) clojure.lang.PersistentVector))

   (if (not= merge-f nil)
     (make-fj-task :size-threshold size-threshold
                   :size-f count
                   :split-f split-vector-halves
                   :process-f process-f
                   :merge-f merge-f
                   :data data)
     (make-fj-task :size-threshold size-threshold
                   :size-f count
                   :split-f split-vector-halves
                   :process-f process-f
                   :data data)))

(defn make-fj-task-map-reduce-vec
  [ & { :keys [ map-f reduce-f data size-threshold ]
      :or { size-threshold 2 } } ]

  (assert (not= map-f nil))
  (assert (not= reduce-f nil))
  (assert (not= data nil))
  (assert (= (class data) clojure.lang.PersistentVector))

  (make-fj-task :size-threshold size-threshold
                :size-f count
                :split-f split-vector-halves
                :process-f #(reduce reduce-f
                               (map map-f %))
                :merge-f reduce-f
                :data data))

(defn- priv-run-fj-task
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
          f-res1 (forkTask (task (priv-run-fj-task fj-task1)))
          res2 (runTask (task (priv-run-fj-task fj-task2)))
          ]
        (merge-f (joinTask f-res1) res2))
        (process-f data))))

(defn run-fj-task
  "Run `fj-task` in a given `fj-pool`."
  [ fj-pool fj-task ]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :merge-f))
  (assert (contains? fj-task :data))
  (invoke fj-pool (task (priv-run-fj-task fj-task))))

(defn- priv-run-fj-task!
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
          f-res1 (forkTask (task (priv-run-fj-task! fj-task1)))
          res2 (runTask (task (priv-run-fj-task! fj-task2)))
          ]
        (joinTask f-res1)
        nil)
      (do (process-f data) nil))))

(defn run-fj-task!
  "Run `fj-task` in a given `fj-pool` for side-effects only.

   `fj-task` must not contain `merge-f` field.
   Any results returned by `process-f` are discarded, preferably it should
   return  `nil`. Too much blocking I/O is can result in suboptimal
   performance, since Fork/Join is designed to CPU intensive tasks."

  [ fj-pool fj-task ]

  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :data))
  (assert (not (contains? fj-task :merge-f)))

  (invoke fj-pool (task (priv-run-fj-task! fj-task))))

(defn- priv-run-fj-task-serial
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
               res1 (priv-run-fj-task-serial fj-task1)
               res2 (priv-run-fj-task-serial fj-task2)
              ]
           (merge-f res1 res2))
         (process-f data))))

(defn run-fj-task-serial
  [ fj-task ]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :merge-f))
  (assert (contains? fj-task :data))
  (priv-run-fj-task-serial fj-task))

(defn- priv-run-fj-task-serial!
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
              res1 (priv-run-fj-task-serial! fj-task1)
              res2 (priv-run-fj-task-serial! fj-task2)
             ]
          nil)
        (do
          (process-f data)
          nil))))

(defn run-fj-task-serial!
  "Runs `fj-task` for side effects only. Returns nil."
  [ fj-task ]
  (assert (contains? fj-task :size-threshold))
  (assert (contains? fj-task :size-f))
  (assert (contains? fj-task :split-f))
  (assert (contains? fj-task :process-f))
  (assert (contains? fj-task :data))
  (priv-run-fj-task-serial! fj-task))
