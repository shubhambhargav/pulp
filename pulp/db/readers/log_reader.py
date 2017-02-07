import re, json, os


class LogReader(object):
    """LOG READER"""

    REGEX_VAL = '\{.*\}'
    ENCODINGS = ['ascii', 'utf-8', 'latin-1']

    def __init__(self, filepath, is_testing=False):
        self.filehandle = None
        self.filepath = filepath
        self.fetchj = re.compile(self.REGEX_VAL)
        self.eof = False
        self.is_testing = is_testing

    def open(self):
        if self._isfilereadable():
            self.filehandle = open(self.filepath, 'r')
            return self.filehandle

    def close(self):
        if self.filehandle and not self.filehandle.closed:
            self.filehandle.close()
            self.filehandle = None

    def _isfilereadable(self):
        if self.filepath is None:
            return False
        return os.path.isfile(self.filepath) and os.access(self.filepath, os.R_OK)

    def _extract_json(self, line):
        jsond = None
        _matchedj = self.fetchj.search(line)
        if _matchedj:
            for type_en in self.ENCODINGS:
                try:
                    jsond = json.loads(_matchedj.group(0), encoding=type_en)
                    jsond = dict((key.lower(), val.encode(type_en) if type(val) == unicode else val) \
                                     for key, val in jsond.items() if val is not None)
                    break
                except Exception as ex:
                    pass
            else:
                raise ex
        return jsond

    def read_one(self):
        line = self.filehandle.readline()
        if not line:
            self.eof = True
            return None

        return self._extract_json(line)


