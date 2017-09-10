(ns kvdb.core
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Catch errors in kv_parse
;; Metadata for assigning types to db entries
;; Hashmap functions https://redis.io/commands#hash
;; Set functions https://redis.io/commands#set
;; Metadata for naming databases

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
  "Returns a hashmap snapshot of the entire database"
  [db]
  (into {} (map #((comp vec list) (first %) (deref (second %))) @db)))

(defn kv-value
  [db k]
  (second (get @db k)))

(defn kv-type
  [db k]
  (first (get @db k)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; KV-* FUNCTIONS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn kv-assoc
  "Returns (:updated new-val) or (:new new-val)"
  [db k v]
  (if (contains? @db k)
    (let [value-atom (get @db k)]
      (list :updated (swap! value-atom (fn [_] v))))
    (do (swap! db assoc k (atom v))
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
  "Returns (:ok value)"
  [db k]
  (list :ok (get @db k)))

(defn kv-exists
  "Returns (:ok num) where num is the number of keys that exist"
  [db & ks]
  (list :ok (reduce #(+ %1 (bool->int (kv-contains? db %2)))
        0
        ks)))

(defn kv-parse
  "Parses and executes a GET, SET, DEL, or EXISTS command
  returns same a calling appropriate kv-* function,
  or (:nocmd db) if the command is not found.

  kv-* functions:
  - kv-assoc
  - kv-dissoc
  - kv-get
  - kv-exists "
  [db string]
  (let [[function & operands] (clojure.string/split string #" ")]
    (case function
        "SET" (apply kv-assoc db operands)
        "GET" (apply kv-get db operands)
        "DEL" (apply kv-dissoc db operands)
        "EXISTS" (apply kv-exists db operands)
        (list :nocmd db))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TESTING
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Confirmation that locking the map doesn't lock individual keys
;; And updating a key while the map is locked will persist to the map being unlocked
(defn atom-test
  [db]
  (kv-parse db "SET x 4")
  (println "x SET to 4")
  (future (swap! db (fn [x] (Thread/sleep 10000) (println "Delay ended") (assoc x "y" (atom "4")))))
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
  (println "Hello, World!"))
