import json
import sys
from kafka import KafkaConsumer

class KafkaReader(object):

    def __init__(self, hosts, topic, name, group, test_hosts=None, is_testing=False):
        self.hosts = hosts if not is_testing else test_hosts
        self.topic = topic
        self.name = name
        self.group = group

        self.consumer_timeout_ms = 120 * 1000 if not is_testing else 1 * 1000

        self.eof = False

    def open(self):
        """
        Connect to kafka
        """
        self.consumer = KafkaConsumer(
                                        self.topic,
                                        client_id=self.name,
                                        group_id=self.group,
                                        bootstrap_servers=self.hosts,
                                        auto_offset_reset='earliest',
                                        auto_commit_interval_ms=5 * 1000,
                                        consumer_timeout_ms=self.consumer_timeout_ms
                                    )

        return self.consumer

    def read_one(self):
        """
        Block and read element in kafka queue
        """
        try:
            read_obj = self.consumer.next()
        except StopIteration:
            self.eof = True
            return None

        return json.loads(read_obj.value)

    def close(self):
        """
        Delete consumer object
        """
        del self.consumer
