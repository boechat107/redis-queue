(ns redis-queue-clj.core
  (:require [taoensso.carmine :as car]
            [cheshire.core :refer [generate-string]]
            [clojure.string :as s :only [split]]))

(set! *warn-on-reflection* true)

(def *pop-timeout* 30)

(def *task-timeout* 30)

(def *redis* nil)

(def *sleeping-time* (* 1000 1)) ;; default to 1 sec

(defmacro wcar*
  "Works exactly like wcar, but using a pre-configured connection."
  [& body]
  `(car/wcar *redis* ~@body))

(defn- pop-queue
  "Just pops (blocking until the timeout) from the tail of the waiting queue. Returns
  the string of the requisition's id."
  [q-key]
  (second (wcar* (car/brpop q-key *pop-timeout*))))

(defn- get-timestamp [] (long (/ (System/currentTimeMillis) 1000)))

(defn- task-late?
  "Return current timestamp in seconds."
  [timestamp]
  (-> (- (get-timestamp) timestamp)
      (> *task-timeout*)))

(defn- done-key [q-key] (car/key qkey :done))

(defn- done?
  "Checks if the requisition's id is member of the done set."
  [q-key tid]
  (wcar* (car/sismember (done-key q-key) tid)))

(defn- remove-done!
  "Removes the requisition's id from the done set."
  [q-key tid]
  (wcar* (car/srem (done-key q-key) tid)))

(defn- re-push!
  "Pushes the requisition's id on the head of the waiting queue, but appending a unix
  timestamp to its string."
  ([q-key tid] (re-push! q-key tid (get-timestamp)))
  ([q-key tid timestamp]
   (wcar* (car/lpush q-key (str tid "|" timestamp)))))

(defn safe-pop
  "Returns a task id from the given queue or blocks until one arrives."
  [rc q-key]
  (binding [*redis* rc]
    (when-let [popped-task (pop-queue q-key)]
      (let [[tid timestamp] (s/split popped-task #"[|]")]
        (cond 
          ;; The task is already done. Its id is removed from the done set and the
          ;; popped task is just discarded.
          (and timestamp (done? q-key tid))
          (do (remove-done! q-key tid) (recur rc q-key))
          ;; The task was never processed or its processing is taking too much time.
          ;; The popped task is returned to be processed with a new timestamp.
          (or (nil? timestamp)
              (and timestamp (task-late? timestamp)))
          (do (re-push! q-key tid) tid)
          ;; The popped is been processed and it's not late.
          :else 
          (do (Thread/sleep *sleeping-time*) (re-push! q-key tid timestamp)))))))

(defn mark-done!
  "Marks the task id as done. It should be used when a worker finishes its job."
  [rc q-key tid]
  (car/wcar rc (car/sadd (done-key q-key) tid)))
