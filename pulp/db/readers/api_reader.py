import re, json, os
import requests

class ApiReader(object):

    def __init__(self, url, params=None, headers=None, format_url=False, format_dict={}, is_testing=False):
        self.url = url
        self.params = params
        self.headers = headers
        self.is_testing = is_testing
        self.format_url = format_url
        self.format_dict = format_dict

        self.data = None
        self.eof = False

    def open(self):
        if self.format_url:
            url = self.url % self.format_dict
        else:
            url = self.url

        resp = requests.get(url, params=self.params, headers=self.headers)

        assert resp.status_code == 200, 'Invalid response code: %s' % (resp.status_code, )

        self.data = resp.json()
        return resp

    def close(self):
        return

    def read_one(self):
        return self.data
