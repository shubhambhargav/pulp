import json


class MissingDefaultDict(dict):

    def __missing__(self, key):
        return ''


class JsonPayloadObject(object):

    def __init__(self, jdata):
        self.data = jdata
        if type(jdata) == str:
            self.__dict__ = json.loads(jdata)
        else:
            self.__dict__ = jdata

        #print self.test3
    
    def convertToDict(self):
        d = self.__dict__
        for k, v in d.items():
            if isinstance(v, JsonPayloadObject):
                d[k] = v.__dict__

        return d