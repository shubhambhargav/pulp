import os
import sys
from elasticsearch import Elasticsearch, helpers

OVERRIDABLE_PARAMETERS = ['_id', 'parent', '_routing', '_op_type']

# Parameters that can be passed in target attrs
# 1. _id : Overridable
# 2. parent : Overridable
# 3. _routing : Overridable
# 4. _op_type : Overridable
# 5. upsert : Default upsert value

class ElasticSearchWriter(object):

    def __init__(self, hosts, port, index_format=None, script=None, op_type=None,
                    index_val=None, type_val=None, doc_as_upsert=False,
                    test_hosts=None, test_port=None, batchsize=1, is_testing=False):
        self.hosts = hosts if not is_testing else test_hosts
        self.port = port if not is_testing else test_port
        self.index_val = index_val
        self.index_format = index_format
        self.script = script
        self.op_type = op_type
        self.doc_as_upsert = doc_as_upsert
        self.type_val = type_val

        self.indices = []

        self.batchsize = batchsize
        self.batch = []

    def open(self):
        self.es = Elasticsearch(hosts=self.hosts, port=self.port)

        self.indices = self.es.indices.get_alias().keys()

        return self.es

    def _index_existence_check(self, index_name):
        if index_name not in self.indices:
            if not self.index_format:
                raise ValueError("Index format not defined for auto-index creation")

            self.es.indices.create(index=index_name, body=self.index_format)

            self.indices.append(index_name)


    def _write_one(self, obj_array):
        error_log = []

        for obj in obj_array:
            try:
                if not obj_array.get('_op_type') or obj_array['_op_type'] == 'index':
                    self.es.index(index=obj['_index'], doc_type=obj['_type'], id=obj['_id'], body=obj['_source'])
                elif obj.get('_op_type') == 'update':
                    if obj.get('doc'):
                        self.es.update(index=obj['_index'], doc_type=obj['_type'], id=obj['_id'], body=obj['doc'])
                    else:
                        self.es.update(index=obj['_index'], doc_type=obj['_type'], id=obj['_id'],
                                        script=obj['script'], body=obj['upsert'])
            except Exception as ex:
                error_log.append(ex)

        return error_log

    def write(self, obj_array):
        error_log = []

        for obj in obj_array:
            try:
                data = obj.get('data')
                target_attrs = obj.get('target_attrs')
                
                to_ingest = {}

                if target_attrs:
                    to_ingest['_index'] = target_attrs['index_val'] if 'index_val' in target_attrs else self.index_val
                    to_ingest['_type'] = target_attrs['type_val'] if 'type_val' in target_attrs else self.type_val

                    if self.op_type:
                        to_ingest['_op_type'] = self.op_type

                    for param in OVERRIDABLE_PARAMETERS:
                        if param in target_attrs:
                            to_ingest[param] = target_attrs[param]

                # If operation type is update, do the following:
                # append script and upsert logic corresponding to the same (if exists)
                # if script is not supplied then send data as doc body to the request
                if to_ingest.get('_op_type') == 'update':
                    script = target_attrs['script'] if 'script' in target_attrs else self.script

                    if script:
                        to_ingest['script'] = script
                        to_ingest['upsert'] = data
                    else:
                        to_ingest['doc'] = data

                else:
                    to_ingest['_source'] = data

                self._index_existence_check(to_ingest['_index'])

                self.batch.append(to_ingest)
            except Exception as ex:
                error_log.append(ex)

        try:
            helpers.bulk(self.es, self.batch)
        except Exception as ex:
            error_log.append(ex)
            error_log += self._write_one(self.batch)
        
        self.batch = []

        return error_log

    def close(self):
        """
        Delete connection variable
        """
        del self.es