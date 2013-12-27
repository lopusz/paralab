(ns paralab.core
  "Module the most general functionality for parallelism.")

(defn get-n-cpus 
  "Returns the number of available processors."
  []
  (. (Runtime/getRuntime) availableProcessors))
