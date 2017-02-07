import abc
from redis import StrictRedis

class RedisWriter(object):

    __metaclass__ = abc.ABCMeta

    """
    Redis Writer with abstract definition of write command
    """

    def __init__(self, host, port, db, password, batchsize=1, test_host=None, test_db=None, test_password=None, is_testing=False):
        self.host = host if not is_testing else test_host
        self.port = port
        self.db = db if not is_testing else test_db
        self.password = password if not is_testing else test_password
        self.batchsize = batchsize

        self.redis = None

    def open(self):
        self.redis = StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)

        return self.redis

    def close(self):
        del self.redis

    @abc.abstractmethod
    def write(self, obj_array):
        """
        Defined afterwards
        """