(defproject paralab "0.1.0-SNAPSHOT"

  ; GENERAL OPTIONS

  :description "paralab library"
  :url ""
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all 
  :omit-source true
  
  ;; options used by Java
  ;;; run with assertions enabled
  :jvm-opts ["-ea"]
  ;;; hints on java code, sometimes maybe useful  
  ;;; :javac-options [ "-Xlint"]

  :main paralab.benchmarks

  ; DEPENDENCIES

  :dependencies [   
    [org.clojure/clojure "1.5.1"]
    [pjstadig/assertions "0.1.0"]
    [org.clojure/math.numeric-tower "0.0.2"]
    [criterium "0.4.2"]
    ]

  ; SOURCE DIRECTORY RECONFIGURATION

  :source-paths ["src" "src/main/clojure"]
  :java-source-paths ["src/main/java"] 
  :test-paths [ "src/test/clojure"]
  
  ; PLUGINS + CONFIGURATION

  :plugins [ [codox "0.6.6"] ]

  ;; codox configuration   

  :codox {
          :output-dir "target/apidoc"
          :sources [ "src/main/clojure"]
          ;; TODO Uncoment below before push to github
          ;; :src-dir-uri "http://github.com/lopusz/langlab/blob/master/"
          ;; :src-linenum-anchor-prefix "L"
          }  
)
