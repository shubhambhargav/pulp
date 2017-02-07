import os
import sys
from elasticsearch import Elasticsearch, helpers

class ElasticSearchReader(object):

    def __init__(self, hosts, port, query, test_hosts=None, test_port=None, index_val=None, type_val=None, is_testing=False):
        self.hosts = hosts if not is_testing else test_hosts
        self.port = port if not is_testing else test_port
        self.index_val = index_val
        self.type_val = type_val
        self.query = query

        self.scroll_id = None

    def open(self):
        self.es = Elasticsearch(hosts=self.hosts, port=self.port)
        self._do_query()

        return self.es

    def _do_query(self):
        q_args = {
                    'search_type': 'scan',
                    'scroll': '1m'
        }
        
        q_args['index'] = self.index_val if self.index_val else '_all'
        q_args['doc_type'] = self.type_val if self.type_val else '_all'
        q_args['body'] = self.query if self.query else {'query': {'match_all': {}}}

        scan_response = self.es.search(**q_args)
        self.scroll_id = scan_response['_scroll_id']

    def read_one(self):
        ret_obj = self.es.scroll(scroll_id=self.scroll_id, scroll='1m')
        return ret_obj

    def close(self):
        """
        No disconnect documents found
        """
        return