import os
import json


class LogWriter(object):

    def __init__(self, filepath, batchsize=1, is_testing=False):
        self.filehandle = None
        self.filepath = filepath
        self.is_testing = is_testing
        self.batchsize = batchsize

    def open(self):
        if not os.path.isfile(self.filepath):
            self.filehandle = open(self.filepath, 'w+')
        elif os.access(self.filepath, os.W_OK):
            self.filehandle = open(self.filepath, 'a+')
        
        return self.filehandle

    def close(self):
        if self.filehandle and not self.filehandle.closed:
            self.filehandle.close()
            self.filehandle = None

    def _write_one(self, jdata):
        self.filehandle.write(json.dumps(jdata))
        self.filehandle.write("\n")

    def write(self, dlist):
        error_log = []
        for e in dlist:
            try:
                self._write_one(e['data'])
            except Exception as ex:
                error_log.append(ex)

        return error_log


