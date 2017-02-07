import re, json, os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from exceptions import StopIteration
import sys


class SparkCsvIterateReader(object):

    def __init__(self, filepath, schema, is_header=False, is_testing=False):
        self.filehandle = None
        self.filepath = filepath
        self.schema = schema
        self.is_header = is_header
        
        self.is_testing = is_testing
        self.eof = False
        self.iterator = None

    def open(self):
        if self._isfilereadable():
            spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

            loader_args = {'path': self.filepath, 'schema': self.schema}
            if self.is_header:
                loader_args['header'] = self.is_header
            
            data = spark.read.csv(**loader_args)
            self.iterator = data.rdd.toLocalIterator()

            return self.iterator

    def close(self):
        if self.filehandle:
            self.filehandle = None

    def _isfilereadable(self):
        if self.filepath is None:
            return False
        return (os.path.isfile(self.filepath) or os.path.isdir(self.filepath)) and os.access(self.filepath, os.R_OK)

    def read_one(self):
        try:
            data = self.iterator.next()
        except StopIteration:
            self.eof = True
            return None

        return data.asDict()



