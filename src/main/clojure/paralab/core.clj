(ns paralab.core
  "Module contains general purpose functions for parallelism.")

(defn get-n-cpus 
  "Returns the number of available processors."
  []
  (. (Runtime/getRuntime) availableProcessors))
