(ns redis-queue-clj.core
  (:require [taoensso.carmine :as car]
            [cheshire.core :refer [generate-string]]
            [clojure.string :as s :only [split]]))

(set! *warn-on-reflection* true)

(def ^:dynamic *pop-timeout* 30)

(def ^:dynamic *task-timeout* 30)

(def ^:dynamic *redis* nil)

(defmacro wcar*
  "Works exactly like wcar, but using a pre-configured connection."
  [& body]
  `(car/wcar *redis* ~@body))

(defn- get-timestamp [] (long (/ (System/currentTimeMillis) 1000)))

(defn- done-key [q-key] (car/key q-key :done))

(defn- ^:dynamic *remove-done!*
  "Removes the requisition's id from the done set. To understand why this function
  has a dynamic scope, look at safe-pop and safe-pop*."
  [q-key tid]
  (wcar* (car/srem (done-key q-key) tid)))

(defn safe-pop
  "Returns a task id from the given queue or blocks until one arrives."
  [rc q-key]
  (binding [*redis* rc]
    (letfn [(done? [tid] (pos? (wcar* (car/sismember (done-key q-key) tid))))
            (task-late? [time-str]
              (->> (Integer/parseInt time-str)
                   (- (get-timestamp))
                   (< *task-timeout*)))
            (re-push! 
              ([tid] (re-push! tid (get-timestamp)))
              ([tid timestamp]
               (wcar* (car/lpush q-key (str tid "|" timestamp)))))] 
      (when-let [popped-task (second (wcar* (car/brpop q-key *pop-timeout*)))]
        (let [[tid timestamp] (s/split popped-task #"[|]")]
          (cond 
            ;; The task is already done. Its id is removed from the done set and the
            ;; popped task is just discarded.
            (and timestamp (done? tid))
            (do (*remove-done!* q-key tid) (safe-pop rc q-key))
            ;; The task was never processed or its processing is taking too much time.
            ;; The popped task is returned to be processed with a new timestamp.
            (or (nil? timestamp)
                (and timestamp (task-late? timestamp)))
            (do (re-push! tid) tid)
            ;; The popped is been processed and it's not late.
            :else 
            (do (Thread/sleep (* 1000 *pop-timeout*)) 
                (re-push! tid timestamp)
                nil)))))))

(defn safe-pop*
  "Acts exactly as safe-pop, but doesn't remove the task id from the done set."
  [rc q-key]
  (binding [*remove-done!* (fn [_ _] nil)]
    (safe-pop rc q-key)))

(defn mark-done!
  "Marks the task id as done. It should be used when a worker finishes its job."
  [rc q-key tid]
  (car/wcar rc (car/sadd (done-key q-key) tid)))
