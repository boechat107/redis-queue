(ns redis-queue-clj.core-test
  (:require [clojure.test :refer :all]
            [redis-queue-clj.core :refer :all]
            [taoensso.carmine :as car]))

(def redis {:pool {}
            :spec {:host "localhost" :port 49153}})

(defn fixture
  [q-key]
  (car/wcar redis 
            (car/del q-key ('redis-queue-clj.core/done-key q-key))))

(deftest pop-task
  (binding [*task-timeout* 2 
            *pop-timeout* 1]
    (let [q-key (car/key :redis :test :queue)
          tid "100"]
      (fixture q-key)
      (car/wcar redis 
                (car/lpush q-key tid))
      (testing "If the timestamp is respected"
        (is (= (safe-pop redis q-key) tid) "First attempt")
        (is (nil? (safe-pop redis q-key)))
        (Thread/sleep (* 1000 1.2 *task-timeout*))
        (is (= (safe-pop redis q-key) tid) "Second attempt"))
      (fixture q-key))))
