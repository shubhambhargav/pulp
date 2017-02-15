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


d = {
    'manage_loc': BASE_PATH
}

# Task Run Command
TASK_OS_COMMAND = "python %(manage_loc)s/manage.py tasks %%(app_name)s %%(task_list)s" % d
# Test Run Command
TEST_OS_COMMAND = "python %(manage_loc)s/manage.py tests %%(app_name)s %%(task_list)s" % d