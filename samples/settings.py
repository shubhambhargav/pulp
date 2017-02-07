import os

from pulp.db.readers.log_reader import LogReader
from pulp.db.writers.log_writer import LogWriter

# Project Root
BASE_PATH = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_PATH, os.path.pardir))


READERS = {
            'log_reader': {
                            'engine': LogReader
                        }
}

WRITERS = {
            'log_writer': {
                            'engine': LogWriter
                        }
}

INSTALLED_APPS = (
            'apps.testing',
        )