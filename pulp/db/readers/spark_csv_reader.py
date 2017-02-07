import re, json, os
from pyspark.sql import SparkSession
from pyspark import SparkConf


class SparkCsvReader(object):

    def __init__(self, filepath, schema, is_header=False, is_testing=False):
        self.filehandle = None
        self.filepath = filepath
        self.schema = schema
        self.is_header = is_header
        
        self.is_testing = is_testing
        self.eof = False

    def open(self):
        if self._isfilereadable():
            return True

    def close(self):
        if self.filehandle:
            self.filehandle = None

    def _isfilereadable(self):
        if self.filepath is None:
            return False
        return (os.path.isfile(self.filepath) or os.path.isdir(self.filepath)) and os.access(self.filepath, os.R_OK)

    def read(self):
        spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

        loader_args = {'path': self.filepath, 'schema': self.schema}
        if self.is_header:
            loader_args['header'] = self.is_header
        
        return spark.read.csv(**loader_args)


