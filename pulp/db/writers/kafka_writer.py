import json
from time import sleep
from kafka import KafkaProducer

class KafkaWriter(object):

    def __init__(self, hosts, topic, key=None, batchsize=1, test_hosts=None, is_testing=False):
        self.hosts = hosts if not is_testing else test_hosts
        self.topic = topic
        self.key = key

        self.batchsize = batchsize

    def open(self):
        """
        Create kafka connection with given attributes
        """
        if self.key:
            self.producer = KafkaProducer(
                                         bootstrap_servers=self.hosts,
                                         key_serializer=lambda k: k.encode('utf-8'),
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                        )
        else:
            self.producer = KafkaProducer(
                                         bootstrap_servers=self.hosts,
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                         )

        return self.producer

    def _write_one(self, data):
        """
        Write entries one by one
        """

        if self.key:
            self.producer.send(topic=self.topic, value=data, key=data[self.key])
        else:
            self.producer.send(topic=self.topic, value=data)

    def write(self, obj_array):
        """
        Insert a batch of entries to kafka
        """
        error_log = []

        for obj in obj_array:
            try:
                data = obj.get('data')
                self._write_one(data)
            except Exception as ex:
                error_log.append(ex)

        return error_log

    def close(self):
        """
        Delete producer object
        """
        self.producer.flush()
        del self.producer