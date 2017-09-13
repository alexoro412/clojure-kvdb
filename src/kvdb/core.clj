(ns kvdb.core
  (:gen-class)
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [clojure.edn :as edn]
    [aleph.tcp :as tcp]
    [gloss.core :as gloss]
    [gloss.io :as io]
    [aleph.netty]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Standardize return types
;; Write load tester

;; LATER TODO
;; Add radix tree?

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HELPER FUNCTIONS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn bool->int
  [bool]
  (if bool 1 0))

(defn- kv-contains?
  [db k]
  (contains? @db k))

(defn empty-db
  []
  (atom {}))

(defn kv-whole-map
  "Returns a hashmap snapshot of the entire database
  {k [type value]}"
  [db]
  (into {}
    (map
    #((comp vec list)
      (first %) ; key
      [(first (second %)) ; type
        (deref (second (second %)))]) ; value
    @db)))

(defn kv-value
  [db k]
  (second (get @db k)))

(defn kv-type
  [db k]
  (first (get @db k)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; KV-* FUNCTIONS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn kv-hset
  "Adds a key-value pairs to a hashmap
  Creates the map if it doesn't already exists.
  Returns (:ok) or (:error :type-mismatch)"
  [db k & kvs]
  (if (contains? @db k)
    (let [[type value-atom] (get @db k)]
      (if (= type :hash)
        (do (swap! value-atom #(apply assoc % kvs))
          (list :ok))
        (list :error :type-mismatch)))
    (do (swap! db assoc k [:hash (atom (apply assoc {} kvs))])
        (list :ok))))

(defn kv-hget
  "Get value for hashmap k, field hk
  Returns (:ok value), (:error :type-mismatch), (:error :nil)"
  [db k hk]
  (if (contains? @db k)
    (let [[type value-atom] (get @db k)]
      (if (= type :hash)
        (if-let [value (get @value-atom hk)]
          (list :ok value)
          (list :error :nil))
        (list :error :type-mismatch)))
    (list :error :nil)))

(defn kv-hdel
  "Delete keys from a hashmap
  returns (:ok num) where num is the number of keys deleted
  or (:error :type-mismatch).
  If the map doesn't exist, returns (:ok 0)"
  [db k & hks]
  (if (contains? @db k)
    (let [[type value-atom] (get @db k)]
      (if (= type :hash)
        (list :ok (reduce
          #(+ %1 (if (contains? @value-atom %2)
                      (do (swap! value-atom dissoc %2)
                          1)
                      0))
          0
          hks))
        (list :error :type-mismatch)))
    (list :ok 0)))

(defn kv-assoc
  "Returns one of the following:
    (:new new-value)
    (:updated new-value)
    (:error :type-mismatch)"
  [db k v]
  (if (contains? @db k)
    (let [[type value-atom] (get @db k)]
      (if (= type :raw)
        (list :updated (swap! value-atom (fn [_] v)))
        (list :error :type-mismatch)))
    (do (swap! db assoc k [:raw (atom v)])
        (list :new v))))

(defn kv-dissoc
  "Returns (:ok num) where num is the number of keys deleted"
  [db & ks]
  (list :ok (reduce
    #(+ %1 (if (kv-contains? db %2)
              (do (swap! db dissoc %2)
                1)
              0))
    0
    ks)))

(defn kv-get
  "Returns (:ok value), (:error :type-mismatch), or (:error :nil)"
  [db k]
  (if (contains? @db k)
    (let [[type value-atom] (get @db k)]
      (if (= type :raw)
        (list :ok @value-atom)
        (list :error :type-mismatch)))
    (list :error :nil)))

(defn kv-exists
  "Returns (:ok num) where num is the number of keys that exist"
  [db & ks]
  (list :ok (reduce #(+ %1 (bool->int (kv-contains? db %2)))
        0
        ks)))

(defn kv-run
  "Runs commands given as list of strings.
  See kv-parse"
  [db [function & operands]]
  (case (clojure.string/upper-case function)
      "ASYNC" (do (future (kv-run db operands)) (list :ok :async))
      "SET" (apply kv-assoc db operands) ;; TODO turn into dispatch macro?
      "GET" (apply kv-get db operands)
      "DEL" (apply kv-dissoc db operands)
      "EXISTS" (apply kv-exists db operands)
      "HDEL" (apply kv-hdel db operands)
      "HGET" (apply kv-hget db operands)
      "HSET" (apply kv-hset db operands)
      (list :error :nocmd)))

(defmacro repl
  [db & forms])

(declare validate)

(defn kv-parse
  "Parses and executes a command
  return values:
    the same as calling appropriate kv-* function,
    (:nocmd db) if the command is not found,
    (:ok :async) for ASYNC commands

  kv-* functions:
  - kv-assoc
  - kv-dissoc
  - kv-get
  - kv-exists
  - kv-hset
  - kv-hget
  - kv-hdel "
  [db string]
  (if (validate string)
    (kv-run db (clojure.string/split string #" "))
    (list :error :syntax)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SYNTAX
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TCP
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn start-server
  [handler port]
  (tcp/start-server
    (fn [s info] ;; info = port number?
      (handler (wrap-duplex-stream protocol s) info))
      {:port port}))

(def main-db (empty-db))

(defn kv-parse-handler
  [s info]
  (s/connect
    (s/map (partial kv-parse main-db) s)
   s))

(defn run-cmd
  [client cmd]
  (do @(s/put! client cmd)
      @(s/take! client)))

(defmacro >>
  [& form]
  (let [string (apply pr-str form)]
  (if (validate string)
    `(run-cmd test-client ~(apply pr-str form))
    (throw (Exception. (str string " is not valid kvdb syntax"))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TESTING
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Confirmation that locking the map doesn't lock individual keys
;; And updating a key while the map is locked will persist to the map being unlocked
(defn atom-test
  [db]
  (kv-parse db "SET x 4")
  (println "x SET to 4")
  (future (swap! db (fn [x] (Thread/sleep 10000) (println "Delay ended") (assoc x "y" [:raw (atom "4")]))))
  (println "Delay started")
  (kv-parse db "SET x 5")
  (println (kv-parse db "GET x")))

; y gets in before x, and forces x to retry
(defn dissoc-test
  [db]
  (kv-parse db "SET x 4")
  (kv-parse db "SET y 5")
  (future (swap! db (fn [x] (Thread/sleep 10000) (println "trying x") (dissoc x "x"))))
  (future (swap! db (fn [x] (Thread/sleep 5000) (println "trying y") (dissoc x "y")))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [server (start-server kv-parse-handler 5432)]
    (println "Server listening on localhost:5432")
    (aleph.netty/wait-for-close server))
  (println "Server shutting down"))

; (def test-client @(client "localhost" 5432))
