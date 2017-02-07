import json
from redis import StrictRedis

class RedisPublisher(object):

    def __init__(self, host, channel, db=0, port=6379, password=None, batchsize=1, is_testing=False):
        self.host = host
        self.port = port
        self.db = db
        self.password = password

        self.channel = channel
        self.batch = []

    def open(self):
        self.redis = StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)

        return self.redis

    def write(self, obj_array):
        error_log = []

        r_pipe = self.redis.pipeline()

        for obj in obj_array:
            try:
                data = obj.get('data')
                
                r_pipe.publish(self.channel, json.dumps(data))

            except Exception as ex:
                error_log.append(ex)

        try:
            r_pipe.execute()
        except Exception as ex:
            error_log.append(ex)

        return error_log

    def close(self):
        """
        No documentation found for redis connection termination
        """