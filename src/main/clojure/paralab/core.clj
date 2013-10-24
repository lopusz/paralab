(ns paralab.core)

(defn get-n-cpus []
  (. (Runtime/getRuntime) availableProcessors))
