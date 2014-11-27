import time

class RedisQueue(object):
    redis = None
    poptimeout = None
    tasktimeout = None

    def __init__(self, redis, poptimeout=30, tasktimeout=30):
        self.redis = redis 
        self.poptimeout = poptimeout
        self.tasktimeout = tasktimeout

    def safe_pop(self, qkey):
        popped_task = self.redis.brpop(qkey, self.poptimeout)
        if popped_task is None:
            return
        ## brpop returns two elements, the queue name and the message. We want
        ## just the message.
        ## Messages may be composed of 'ID' or 'ID|timestamp'.
        id_timestamp = popped_task[1].split('|')
        task_id = id_timestamp[0]
        timestamp = None 
        if len(id_timestamp) > 1:
            timestamp = long(id_timestamp[1])
        ## Checking the possible actions, depending of the existence of the 
        ## timestamp and its value.
        if timestamp and self.__isdone(qkey, task_id):
            ## The task was done and its id can be safely removed from the done 
            ## set.
            self.redis.srem(self.__done_key(qkey), task_id)
            return self.safe_pop(qkey)
        elif (timestamp is None) or (self.__islate(timestamp)):
            ## If a task doesn't have a timestamp, it was never processed. If 
            ## there is a timestamp and its value indicates that the task is
            ## late, it's scheduled to be processed again.
            self.__repush(qkey, task_id)
            return task_id
        else:
            ## The task is in progress but not late. 
            self.__repush(qkey, task_id, timestamp)
            time.sleep(self.poptimeout)
            return None

    def __done_key(self, qkey):
        return "%s:done" % qkey

    def __isdone(self, qkey, tid):
        """Checks if a task is done, i.e., if its id is a member of the done
        set.
        """
        return self.redis.sismember(self.__done_key(qkey), tid)

    def __islate(self, timestamp):
        passed_time = long(time.time()) - timestamp
        return self.tasktimeout < passed_time

    def __repush(self, qkey, tid, timestamp=long(time.time())):
        self.redis.lpush(qkey, "%s|%s" % (tid, timestamp))

    def mark_done(self, qkey, tid):
        self.redis.sadd(self.__done_key(qkey), tid)
