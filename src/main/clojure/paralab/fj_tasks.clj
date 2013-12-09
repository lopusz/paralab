(ns paralab.fj-tasks

  " Simple Clojure interface to ForkJoin task pool.

    On the basis of gist from swannodette:
    https://gist.github.com/888733"
  (:refer-clojure :exclude [assert])
  (:require [pjstadig.assertions :refer [assert]]))

;;(set! *warn-on-reflection* true)

;; Java 1.6 vs. Java 1.7 compatibility trick from Reducers library

(defmacro ^:private compile-if
  "Evaluate `exp` and if it returns logical true and doesn't error, expand to
   `then`. Else expand to `else`.

    (compile-if (Class/forName \"java.util.concurrent.ForkJoinTask\")
        (do-cool-stuff-with-fork-join)
        (fall-back-to-executor-services))"
  [exp then else]
  (if (try (eval exp)
           (catch Throwable _ false))
    `(do ~then)
    `(do ~else)))

(compile-if
  (Class/forName "java.util.concurrent.ForkJoinTask")
  (import '(java.util.concurrent RecursiveTask ForkJoinPool))
  (import '(jsr166y RecursiveTask ForkJoinPool)))

;; End of trick

;; Helpers to provide an idiomatic interface to FJ

(defprotocol IFJTask
  (fork [this])
  (join [this])
  (run [this])
  (compute [this]))

(deftype FJTask [^RecursiveTask task]
  IFJTask
  (fork [_] (FJTask. (.fork task)))
  (join [_] (.join task))
  (run [_] (.invoke task))
  (compute [_] (.compute task)))

(defn- ^FJTask task* [f]
  (FJTask. (proxy [RecursiveTask] []
             (compute [] (f)))))

(defmacro ^:private task [& rest]
  `(task* (fn [] ~@rest)))

(defprotocol IFJPool
  (shutdown [this])
  (submit [this task])
  (invoke [this task])
  (execute [this task])
  (getRawFJPool [this])) ;; ugliness for fj-reducers

(deftype FJPool [^ForkJoinPool fjp]
  IFJPool
  (shutdown [this] (.shutdown this))
  (submit [this task]
          (let [^FJTask task task]
            (.submit fjp
              ^RecursiveTask (.task task))))
  (invoke [this task]
          (let [^FJTask task task]
            (.invoke fjp
              ^RecursiveTask (.task task))))
  (execute [this task]
           (let [^FJTask task task]
             (.execute fjp
               ^RecursiveTask (.task task))))
  (getRawFJPool [ this ]
    (.fjp this))) ;; ugliness for fj-reducers

(defn split-vector-halves [ v ]
  (let [
       half (quot (count v) 2)
       ]
    [ (subvec v 0 half) (subvec v half) ]))

(defn ^FJPool create-fjpool
  "Creates fork-join theradpool with a specified number of threads.
   If no argument is given it creates number of threads equal to #cpus."
  ([]
     (FJPool. (ForkJoinPool.)))
  ([n-cpus]
     (FJPool. (ForkJoinPool. n-cpus))))

(defn- priv-fj-run
  [ fjtask ]

  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fjtask
      ]
    (if (> (size-f data) size-threshold)
      (let [
          [data1 data2] (split-f data)
          fj-task1 (merge fjtask {:data data1})
          fj-task2 (merge fjtask {:data data2})
          f-res1 (fork (task (priv-fj-run fj-task1)))
          res2 (run (task (priv-fj-run fj-task2)))
          ]
        (merge-f (join f-res1) res2))
        (process-f data))))

(defn make-vec-fjtask [ fjtask ]
  (assert (contains? fjtask :data))
  (assert (contains? fjtask :process-f))
  (assert (contains? fjtask :merge-f))

  (let [
         vector-fjtask {
                    :size-threshold 1
                    :size-f count
                    :split-f split-vector-halves }
        ]
    (merge vector-fjtask fjtask)))

(defn fj-run [ fjpool fjtask ]
  (assert (contains? fjtask :size-threshold))
  (assert (contains? fjtask :size-f))
  (assert (contains? fjtask :split-f))
  (assert (contains? fjtask :process-f))
  (assert (contains? fjtask :merge-f))
  (assert (contains? fjtask :data))
  (invoke fjpool (task (priv-fj-run fjtask))))

(defn- priv-fj-run!
  [ fjtask ]
  (let [
        {:keys [ size-threshold size-f
                 split-f process-f data ] } fjtask
      ]
    (if (> (size-f data) size-threshold)
      (let [
          [data1 data2] (split-f data)
          fj-task1 (merge fjtask {:data data1})
          fj-task2 (merge fjtask {:data data2})
          f-res1 (fork (task (priv-fj-run! fj-task1)))
          res2 (run (task (priv-fj-run! fj-task2)))
          ]
        (join f-res1)
        nil)
      (do (process-f data) nil))))

(defn fj-run!
  "Used to run fjtasks for side effects only.
   The only results are written to the output by process-f.
   Any results returned by process-f are discarded, preferably it should
   return  nil.
   There is no need for merge-f function required by ordinary fj-run.
   Doing non-blocking I/O is against the rules of fork-join and can result
   in suboptimal performance."
  [ fjpool fjtask ]

  (assert (contains? fjtask :size-threshold))
  (assert (contains? fjtask :size-f))
  (assert (contains? fjtask :split-f))
  (assert (contains? fjtask :process-f))
  (assert (contains? fjtask :data))

  (invoke fjpool (task (priv-fj-run! fjtask))))

(defn- priv-fj-run-serial
  [ fjtask ]
  (let [
         {:keys [ size-threshold size-f
                  split-f process-f merge-f data ] } fjtask
       ]
       (if (> (size-f data) size-threshold)
         (let [
               [data1 data2] (split-f data)
               fj-task1 (merge fjtask {:data data1})
               fj-task2 (merge fjtask {:data data2})
               res1 (priv-fj-run-serial fj-task1)
               res2 (priv-fj-run-serial fj-task2)
              ]
           (merge-f res1 res2))
         (process-f data))))

(defn fj-run-serial
  [ fjtask ]
  (assert (contains? fjtask :size-threshold))
  (assert (contains? fjtask :size-f))
  (assert (contains? fjtask :split-f))
  (assert (contains? fjtask :process-f))
  (assert (contains? fjtask :merge-f))
  (assert (contains? fjtask :data))
  (priv-fj-run-serial fjtask))

(defn- priv-fj-run-serial!
  [ fjtask ]
  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fjtask
      ]
      (if (> (size-f data) size-threshold)
        (let [
              [data1 data2] (split-f data)
              fj-task1 (merge fjtask {:data data1})
              fj-task2 (merge fjtask {:data data2})
              res1 (priv-fj-run-serial! fj-task1)
              res2 (priv-fj-run-serial! fj-task2)
             ]
          nil)
        (do
          (process-f data)
          nil))))

(defn fj-run-serial!
  [ fjtask ]
  (assert (contains? fjtask :size-threshold))
  (assert (contains? fjtask :size-f))
  (assert (contains? fjtask :split-f))
  (assert (contains? fjtask :process-f))
  (assert (contains? fjtask :data))
  (priv-fj-run-serial! fjtask))
