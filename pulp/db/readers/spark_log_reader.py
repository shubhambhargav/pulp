import re, json, os
from pyspark import SparkContext, SparkConf


class SparkLogReader(object):

    REGEX_VAL = '\{.*\}'
    ENCODINGS = ['ascii', 'utf-8', 'latin-1']

    def __init__(self, filepath, app_name, is_testing=False):
        self.filehandle = None
        self.filepath = filepath
        self.fetchj = re.compile(self.REGEX_VAL)
        self.is_testing = is_testing
        self.app_name = app_name

    def open(self):
        if self._isfilereadable():
            return True

    def close(self):
        return

    def _isfilereadable(self):
        if self.filepath is None:
            return False
        return os.path.isfile(self.filepath) and os.access(self.filepath, os.R_OK)

    @staticmethod
    def load_json(parsed):
        try:
            return json.loads(parsed.group(0))
        except ValueError:
            return {}


    def read(self):
        conf = SparkConf().setAppName(self.app_name)
        sc = SparkContext(conf=conf)
        fload = sc.textFile(self.filepath)

        data = fload.map(lambda line: self.fetchj.search(line))\
                        .filter(lambda parsed: True if parsed else False)\
                        .map(SparkLogReader.load_json)\
                        .filter(lambda x: True if x else False)
        return data


