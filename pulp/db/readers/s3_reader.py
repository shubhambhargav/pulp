import abc
from boto import connect_s3
from boto.s3 import connection


class S3Reader(object):

    def __init__(self, bucket, aws_access_key, aws_secret_key, is_testing=False):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.is_testing = is_testing
        self.bucket = bucket

        self.conn = None
        self.bucket_handler = None
        self.eof = False
        
    def open(self):
        self.conn = connect_s3(
                                aws_access_key_id=self.aws_access_key,
                                aws_secret_access_key=self.aws_secret_key
                                )
        self.bucket_handler = self.conn.get_bucket(self.bucket)
        return self.conn

    def close(self):
        del self.conn
        del self.bucket_handler

    @abc.abstractmethod
    def read_one(self):
        return