import os
import unittest
import MySQLdb
from time import sleep
from apps.testing.runners.loglog import SampleIngestor

from pulp.conf import settings

INPUT_FILE = os.path.join(settings.BASE_PATH, 'apps', 'testing', 'fixtures', 'loglogEmbedded.txt')
OUTPUT_FILE = os.path.join(settings.BASE_PATH, 'test.log')


class SampleTester(unittest.TestCase):

    def test_log_ingestion(self):
        reader = {'filepath': INPUT_FILE}
        writer = {'filepath': OUTPUT_FILE}

        ingestor = SampleIngestor(reader=reader, writer=writer, is_testing=True)
        ingestor.start()

        # Sleep for processes to get completed
        #sleep(2)
        self.assertEqual(len(ingestor.error_log), 1) # Expected error for "test1" not available

        count = 0
        with open(OUTPUT_FILE, 'r') as fileop:
            for line in fileop:
                count += 1

        self.assertEqual(count, 3)

    def tearDown(self):
        if os.path.isfile(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)