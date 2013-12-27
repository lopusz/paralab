(ns paralab.fj-core

   "Module contains core interface to Java ForkJoin library. 

    On the basis of gist from swannodette: https://gist.github.com/888733")

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

(defn ^FJPool make-fj-pool
  "Creates fork-join thread pool with a specified number of threads.
   If no argument is given it creates number of threads equal to #cpus."
  ([]
     (FJPool. (ForkJoinPool.)))
  ([n-cpus]
     (FJPool. (ForkJoinPool. n-cpus))))
