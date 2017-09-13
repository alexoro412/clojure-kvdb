(ns kvdb-client.core
  (:gen-class)
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [clojure.edn :as edn]
    [aleph.tcp :as tcp]
    [gloss.core :as gloss]
    [gloss.io :as io]
    [clj-gatling.core :as gatling]))

(defmulti validate
  "Validates syntax"
  (fn [string] (clojure.string/upper-case (first (clojure.string/split string #" ")))))

(defmethod validate "ASYNC"
  [string]
  (->> string (#(clojure.string/split % #" ")) rest (clojure.string/join " ") validate))

(defmacro check-arity
  [command argc]
  (let [string (gensym)]
    `(defmethod validate ~command
      [~string]
      (= (+ ~argc 1) (->> ~string (#(clojure.string/split % #" ")) count)))))

(defmacro check-min-arity
  [command argc]
  (let [string (gensym)]
    `(defmethod validate ~command
      [~string]
      (< ~argc (->> ~string (#(clojure.string/split % #" ")) count)))))

(check-arity "GET" 1)
(check-arity "SET" 2)
(check-arity "HGET" 2)
(check-arity "CLEAR" 0)
(check-min-arity "DEL" 1)
(check-min-arity "HDEL" 2)
(check-min-arity "EXISTS" 1)

(defmethod validate "HSET"
  [string]
  (let [argc (->> string (#(clojure.string/split % #" ")) count)]
    (and (> argc 2) (= (mod argc 2) 0))))

(defmethod validate :default
  [string]
  false)

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

(def c @(client "localhost" 5432))

(defmacro >>
  [& form]
  (let [string (apply pr-str form)]
  (if (validate string)
    `(run-cmd c ~string)
    (throw (Exception. (str string " is not valid kvdb syntax"))))))

(defn set-del-overload [_]
  (let [k (gensym)
        res1 (run-cmd c (str "SET" k 4))
        res2 (run-cmd c (str "DEL" k))]
        true))

(defn throw-away [x]
  @x
  nil)

; Starts failing around 10% of the time after enough runs
(defn set-get-test [ctx]
  (let [client @(client "localhost" 5432)]
    (every? #(= true %) (doall (repeat (:number ctx)
      (let [k (str (java.util.UUID/randomUUID))
          v (str (java.util.UUID/randomUUID))
          res1 (run-cmd client (str "SET " k " " v))
          res2 (run-cmd client (str "GET " k))]
          #_(println (str v) (second res2))
          (= (str v) (second res2))))))))

(defn set-del-test [ctx]
  (let [client @(client "localhost" 5432)]
    (every? #(= true %) (doall (repeat (:number ctx)
      (let [k (str (java.util.UUID/randomUUID))
          v (str (java.util.UUID/randomUUID))
          res1 (run-cmd client (str "SET " k " " v))
          res2 (run-cmd client (str "DEL " k))]
          (= "1" (second res2))))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  #_(print "WELCOME\n>>")
  #_(loop [cmd (read-line)]
    (if (clojure.string/starts-with? (clojure.string/upper-case cmd) "QUIT")
      (println "Bye!")
      (do (print (run-cmd c cmd) "\n>> ")
        (flush) ;; to ensure printing happens before reading
        (recur (read-line)))))
  (gatling/run
    {:name "Load test"
    :post-hook (fn [_] (run-cmd c "CLEAR"))
    :scenarios [{:name "Test 1"
                  :context {:number 100}
                 :steps [{:name "SET/DEL overload"
                          :request set-del-overload}
                          {:name "SET/GET test"
                          :request set-get-test}]}]}
    {:concurrency 10
      :requests 1000}))
