import redis 
import time
import unittest
from redis_queue import RedisQueue


class RedisQueueTest(unittest.TestCase):
    rc = None 

    def setUp(self):
        self.rc = redis.StrictRedis(host='localhost', port=49153)

    def test_failure(self):
        qkey = 'test:failure:queue'
        tid = '12'
        rq = RedisQueue(self.rc, 1, 2)
        self.rc.lpush(qkey, tid)
        tid0 = rq.safe_pop(qkey)
        self.assertEqual(tid, tid0)
        ## Popping another task too fast, before the task timeout has been 
        ## reached.
        tid1 = rq.safe_pop(qkey)
        self.assertIsNone(tid1)
        ## Supposing the worker had died before finishing the task, we can take
        ## it again after the task timeout.
        time.sleep(2)
        tid2 = rq.safe_pop(qkey)
        self.assertEqual(tid, tid2)
        ## Marking the task as done should make impossible to retrieve the same 
        ## task.
        rq.mark_done(qkey, tid2)
        time.sleep(2)
        tid3 = rq.safe_pop(qkey)
        self.assertIsNone(tid3)
        self.rc.delete(qkey, "%s:done" % qkey)

if __name__ == '__main__':
    unittest.main()
