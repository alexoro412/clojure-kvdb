(ns kvdb-client.core
  (:gen-class)
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [clojure.edn :as edn]
    [aleph.tcp :as tcp]
    [gloss.core :as gloss]
    [gloss.io :as io]))

(def protocol
  (gloss/compile-frame
    (gloss/finite-frame :uint32
      (gloss/string :utf-8))
    pr-str
    edn/read-string))

(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect ;; send all data from first to second
      (s/map #(io/encode protocol %) out) ;; map encode over a stream
    s)
    (s/splice ;; all messages from put! go to out
      out ;; all messages from take! come from source
      (io/decode-stream s protocol))))

;; creates a tcp client, and then wraps the protocol over it
(defn client
  [host port] ;; d/chain basically ->
  (d/chain (tcp/client {:host host :port port})
    #(wrap-duplex-stream protocol %)))

(defn run-cmd
  [client cmd]
  (do @(s/put! client cmd)
      @(s/take! client)))

(defmacro >>
  [& form]
  (let [string (clojure.string/trim (reduce
    #(clojure.string/join " " [%1 (str %2)]) "" form))]
    `(run-cmd c ~string)))

(def c @(client "localhost" 5432))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (print "WELCOME\n>>")
  (loop [cmd (read-line)]
    (if (clojure.string/starts-with? (clojure.string/upper-case cmd) "QUIT")
      (println "Bye!")
      (do (print (run-cmd c cmd) "\n>> ")
        (flush) ;; to ensure printing happens before reading
        (recur (read-line))))))
