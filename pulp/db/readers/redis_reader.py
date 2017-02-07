import abc
from redis import StrictRedis

class RedisReader(object):

    __metaclass__ = abc.ABCMeta

    """
    Redis Reader with abstract definition of read command
    """

    def __init__(self, host, port, db, password, test_host=None, test_db=None, test_password=None, is_testing=False):
        self.host = host if not is_testing else test_host
        self.port = port
        self.db = db if not is_testing else test_db
        self.password = password if not is_testing else test_password

        self.redis = None
        self.eof = False

    def open(self):
        self.redis = StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)

        return self.redis

    def close(self):
        del self.redis

    @abc.abstractmethod
    def read_one(self):
        """
        Defined afterwards
        """