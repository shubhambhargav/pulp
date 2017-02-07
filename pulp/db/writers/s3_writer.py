from boto import connect_s3
from boto.s3 import connection


class S3Writer(object):

    def __init__(self, bucket, aws_access_key, aws_secret_key, aws_host, acl_value=None, batchsize=1, is_testing=False):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_host = aws_host
        self.is_testing = is_testing
        self.bucket = bucket
        self.batchsize = batchsize
        self.acl_value = acl_value

        self.conn = None
        self.bucket_handler = None
        
    def open(self):
        self.conn = connect_s3(
                                aws_access_key_id=self.aws_access_key,
                                aws_secret_access_key=self.aws_secret_key,
                                host=self.aws_host
                                )
        self.bucket_handler = self.conn.get_bucket(self.bucket)
        return self.conn

    def _write_one(self, key, content, is_update=False):
        key_obj = self.bucket_handler.get_key(key)

        if key_obj and not is_update:
            return

        key_obj = self.bucket_handler.new_key(key)

        key_obj.set_contents_from_string(content)

        if self.acl_value:
            key_obj.set_canned_acl(self.acl_value)

    def write(self, obj_array):
        error_log = []

        for obj in obj_array:
            try:
                target_attrs = obj['target_attrs']
                is_update = target_attrs.get('is_update', False)
                data = obj['data']
                self._write_one(data['key'], data['content'], is_update)
            except Exception as ex:
                error_log.append(ex)

        return error_log

    def close(self):
        del self.conn
        del self.bucket_handler