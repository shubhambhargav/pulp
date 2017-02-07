import json
from redis import StrictRedis

class RedisSubscriber(object):

    def __init__(self, host, channel, db=0, port=6379, password=None, is_testing=False):
        self.host = host
        self.port = port
        self.db = db
        self.password = password

        self.channel = channel
        self.subscriber = None
        self.listener = None

    def open(self):
        self.redis = StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)
        
        self.subscriber = self.redis.pubsub()
        self.subscriber.subscribe(self.channel)

        self.listener = self.subscriber.listen()
        self.listener.next()


        return self.redis

    def read_one(self):
        listened = self.listener.next()
        return listened['data']