import time
import uuid
import logging as log

class RedisQueue(object):
    msg_data_temp = ":data:{ID}"

    def __init__(self, redis, poptimeout=30, tasktimeout=30, exptime=60*60*24):
        self.redis = redis 
        self.poptimeout = poptimeout
        self.tasktimeout = tasktimeout
        self.exptime = exptime

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

    def push_msg(self, qkey, msg):
        """Sets the message to a key, generates an unique ID, pushes the ID into
        the queue and returns the ID to the caller of this function.
        """
        mid = uuid.uuid4()
        self.redis.set(qkey + RedisQueue.msg_data_temp.format(ID=mid), 
                       msg,
                       ex=self.exptime)
        self.redis.lpush(qkey, mid)
        return mid

    def safe_pop_msg(self, qkey):
        """Returns a tuple (msg-id, msg-content), following the same ideas of 
        safe_pop.
        """
        mid = self.safe_pop(qkey)
        if not mid:
            return None
        msg = self.__get_msg(qkey, mid)
        if not msg:
            log.warn("""Discarding message ID %s because it's too old or doesn't \
                     have an associated data""")
            self.mark_done(qkey, mid)
            return None 
        return (mid, msg)

    def __get_msg(self, qkey, mid):
        mkey = qkey + RedisQueue.msg_data_temp.format(ID=mid)
        return self.redis.get(mkey)

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

    def __repush(self, qkey, tid, timestamp=None):
        if not timestamp:
            timestamp = long(time.time())
        self.redis.lpush(qkey, "%s|%s" % (tid, timestamp))

    def mark_done(self, qkey, tid):
        """Adds the message ID to the set of messages done.
        If some related key was created to handle this message, it will be
        deleted. 
        """
        self.redis.sadd(self.__done_key(qkey), tid)
        self.redis.delete(qkey + RedisQueue.msg_data_temp.format(ID=tid))
