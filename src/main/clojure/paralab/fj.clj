(ns paralab.fj

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

(defn ^FJTask task* [f]
  (FJTask. (proxy [RecursiveTask] []
             (compute [] (f)))))

(defmacro task [& rest]
  `(task* (fn [] ~@rest)))

(defprotocol IFJPool
  (shutdown [this])
  (submit [this task])
  (invoke [this task])
  (execute [this task]))

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
               ^RecursiveTask (.task task)))))

(defn split-vector [ v ]
  (let [
       half (quot (count v) 2)
       ]
    [ (subvec v 0 half) (subvec v half) ]))

(defn ^FJPool fjpool
  ([]
     (FJPool. (ForkJoinPool.)))
  ([ n-cpus ]
     (FJPool. (ForkJoinPool. n-cpus))))

(defn fj-run
  [ fj-job ]
  
  (assert #(contains? fj-job :size-threshold)) 
  (assert #(contains? fj-job :size-f)) 
  (assert #(contains? fj-job :split-f)) 
  (assert #(contains? fj-job :process-f)) 
  (assert #(contains? :merge-f))
  (assert #(contains? :data))

  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fj-job 
      ]
    (if (> (size-f data) size-threshold)
      (let [
          [data1 data2] (split-f data)
          fj-job1 (merge fj-job {:data data1}) 
          fj-job2 (merge fj-job {:data data2})
          f-res1 (fork (task (fj-run fj-job1)))
          res2 (run (task (fj-run fj-job2)))
          ]
        (merge-f (join f-res1) res2))  
        (process-f data))))

(defn fj-run!
  "Used to run fj-tasks for side effects only.
   The only results are written to the output by process-f.
   Any results returned by process-f are discarded, preferably it should 
   return  nil.
   There is no need for merge-f function required by ordinary fj-run.
   Doing non-blocking I/O is against the rules of fork-join and can result
   in suboptimal performance."
  [ fj-job ]

  (assert #(contains? fj-job :size-threshold)) 
  (assert #(contains? fj-job :size-f)) 
  (assert #(contains? fj-job :split-f)) 
  (assert #(contains? fj-job :process-f)) 
  (assert #(contains? :merge-f))
  (assert #(contains? :data))

  (let [
        {:keys [ size-threshold size-f
                 split-f process-f data ] } fj-job 
      ]
    (if (> (size-f data) size-threshold)
      (let [
          [data1 data2] (split-f data)
          fj-job1 (merge fj-job {:data data1}) 
          fj-job2 (merge fj-job {:data data2})
          f-res1 (fork (task (fj-run! fj-job1)))
          res2 (run  (task (fj-run! fj-job2)))
          ]
        (join f-res1))  
      (process-f data))))

(defn fj-run-serial
  [ fj-job ]

  (assert #(contains? fj-job :size-threshold))
  (assert #(contains? fj-job :size-f)) 
  (assert #(contains? fj-job :split-f)) 
  (assert #(contains? fj-job :process-f)) 
  (assert #(contains? :merge-f))
  (assert #(contains? :data))

  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fj-job 
      ]
      (if (> (size-f data) size-threshold)
        (let [
              [data1 data2] (split-f data)
              fj-job1 (merge fj-job {:data data1}) 
              fj-job2 (merge fj-job {:data data2})
              res1 (fj-run-serial fj-job1)
              res2 (fj-run-serial fj-job2)
             ]          
          (merge-f res1 res2))
        (process-f data))))

(defn fj-run-serial!
  [ fj-job ]

  (assert #(contains? fj-job :size-threshold))
  (assert #(contains? fj-job :size-f)) 
  (assert #(contains? fj-job :split-f)) 
  (assert #(contains? fj-job :process-f)) 
  (assert #(contains? :merge-f))
  (assert #(contains? :data))

  (let [
        {:keys [ size-threshold size-f
                 split-f process-f merge-f data ] } fj-job 
      ]
      (if (> (size-f data) size-threshold)
        (let [
              [data1 data2] (split-f data)
              fj-job1 (merge fj-job {:data data1}) 
              fj-job2 (merge fj-job {:data data2})
              res1 (fj-run-serial! fj-job1)
              res2 (fj-run-serial! fj-job2)
             ]          
          res2)
        (process-f data))))
  