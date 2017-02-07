import os
from importlib import import_module

settings = import_module(os.environ['PULP_SETTINGS_MODULE'])