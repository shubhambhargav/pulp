#!/usr/bin/env python
import os
import sys
import argparse
from importlib import import_module

if __name__ == '__main__':
    # Append manage.py location to pythonpath
    dirname = os.path.dirname(__file__)
    base_dirname = os.path.abspath(os.path.join(dirname, os.path.pardir))
    sys.path.append(base_dirname)

    # Define settings path in module
    SETTINGS_DEFAULT = 'settings'
    os.environ.setdefault('PULP_SETTINGS_MODULE', SETTINGS_DEFAULT)
    settings = import_module(SETTINGS_DEFAULT)
    
    from pulp.core.management.executioner import execute_task_from_cmd

    # Valid runner types
    runner_types = ['tasks', 'tests', 'register_tasks']
    # Apps list from settings file
    apps_list = [app.replace('apps.', '') for app in settings.INSTALLED_APPS] + [None,]

    parser = argparse.ArgumentParser(description='Management Parser')
    parser.add_argument('type', choices=runner_types, help='Runner Type')
    parser.add_argument('app', choices=apps_list, nargs='?', default=None, help='App name')
    parser.add_argument('name', nargs='*', default=[], help='Runner name to be run. Leave it blank to run all')
    args = parser.parse_args()
    
    # Execute the runners
    execute_task_from_cmd(args.type, args.app, args.name)