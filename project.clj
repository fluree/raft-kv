(defproject raft-kv "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [fluree/raft "0.11.1"]
                 [aleph "0.4.6"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/tools.logging "0.4.1"]
                 [com.taoensso/nippy "2.14.0"]
                 [ring/ring-core "1.7.0"]
                 [ring-cors "0.1.12"]
                 [compojure "1.6.1"]
                 [cheshire "5.8.1"]
                 ;; network comm
                 [net.async/async "0.1.0"]])
