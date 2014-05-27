(defproject paralab "0.1.0"

  ; GENERAL OPTIONS

  :description "paralab library"
  :url "http://github.com/lopusz/paralab"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :omit-source true
  

  ;; options used by Java
  ;;; run with assertions enabled
  :jvm-opts ["-ea"]
  ;;; hints on java code, sometimes maybe useful  
  ;;; :javac-options [ "-Xlint"]
  
  ;; enable reflection warnings
  ;;;:global-vars {*warn-on-reflection* true} 
  
  ; DEPENDENCIES

  :dependencies [   
    [org.clojure/clojure "1.6.0"]
    [pjstadig/assertions "0.1.0"]
    [org.clojure/math.numeric-tower "0.0.4"]
    ]

  ; SOURCE DIRECTORY RECONFIGURATION

  :source-paths ["src" "src/main/clojure"]
  :java-source-paths ["src/main/java"] 
  :test-paths [ "src/test/clojure"]
  
  ; PLUGINS + CONFIGURATION

  :plugins [ 
             [codox "0.8.7"] 
             [lein-ancient "0.5.5"]
            ]

  ;; codox configuration   

  :codox {
          :output-dir "target/apidoc"
          :exclude paralab.fj-reducers
          :sources [ "src/main/clojure"]
          :defaults {:doc/format :markdown}
          :src-dir-uri "http://github.com/lopusz/paralab/blob/master/"
          :src-linenum-anchor-prefix "L"
          }  )
