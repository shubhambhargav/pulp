import os
import sys
import luigi
import shutil
import MySQLdb
import unittest
from glob import glob
from inspect import isclass
from importlib import import_module

from pulp import conf
from pulp.conf import settings
from pulp.utils.util import Utilities


class TestsExecutioner(object):

    DEFAULT_APP_STRING = 'apps.%s.tests'

    def __init__(self, app_name, test_list):
        """
        Create app and test attributes for the given object
        """
        if app_name:
            self.app_name_list = [self.DEFAULT_APP_STRING % app_name]
        else:
            self.app_name_list = [app_name + '.tests' for app_name in settings.INSTALLED_APPS]

        self.app_dict = {}
        for app_name in self.app_name_list:
            self.app_dict[app_name] = import_module(app_name)

        # Check for existing test_list
        if not test_list:
            self.test_list = []
            self.is_test_list = False
        else:
            self.test_list = test_list
            self.is_test_list = True

    def _get_all_test_modules(self, app_name, app):
        """
        Steps:
        1. Find the base path of given package
        2. Access all the modules belonging to the specified package
        """
        l = []
        module_path = os.path.abspath(app.__file__).replace('__init__.pyc', 'test_*.py')
        tests_path_list = glob(module_path)
        
        for path in tests_path_list:
            test_module_name = path.split('/')[-1].replace('.py', '')
            test_module = import_module('.'.join([app_name, test_module_name]))
            l.append(test_module)

        return l

    def _get_all_tests(self, app_name, app):
        test_modules_list = self._get_all_test_modules(app_name, app)        

        d = {}
        for test_module in test_modules_list:
            for key, val in test_module.__dict__.items():
                # Extraction of valid test cases
                if isinstance(val, type) and isclass(val) and issubclass(val, unittest.case.TestCase):
                    d[key] = val

        return d

    def _error_display(self, error_log):
        """
        Display error message per task in following format:
        
        <Test> failed due to following error:
        <Exception>
        """
        if not error_log:
            return "All Tests Ran Successfully!"

        msg = ''
        pattern_print = """'%s' test failed due to following error:\n%s\n"""
        for test, ex in error_log.items():
            msg += pattern_print % (test, ex)

        return msg

    def execute(self):
        """
        Steps:
        1. Fetch all test modules for given app
        2. Search for the validity of given tests list
        3. Run all the valid tests
        """
        suite_list = []
        not_valid_tests = []

        for app_name, app in self.app_dict.items():
            all_tests = self._get_all_tests(app_name, app)
            
            if not self.is_test_list:
                self.test_list = all_tests.keys()
            else:
                not_valid_tests = [test_name for test_name in self.test_list if test_name not in all_tests]

            if not_valid_tests:
                raise Exception("Invalid Tests Found: '%s' (Execution Halted)" % (str(not_valid_tests), ))

            for test in self.test_list:
                module = all_tests.get(test)

                loader = unittest.TestLoader().loadTestsFromTestCase(module)
                suite_list.append(loader)

        suite_all = unittest.TestSuite(suite_list)
        unittest.TextTestRunner(verbosity=2).run(suite_all)


class TasksExecutioner(object):

    DEFAULT_APP_STRING = 'apps.%s.tasks'

    def __init__(self, app_name, task_list):
        """
        Create app and task attributes for the given object
        """
        self.task_list = task_list
        self.app_name = self.DEFAULT_APP_STRING % app_name

        self.app = import_module(self.app_name)

    def _get_all_tasks(self):
        l = []
        for key, val in self.app.__dict__.items():
            if isinstance(val, type) and isclass(val) and issubclass(val, luigi.Task):
                l.append(key)

        return l

    def _error_display(self, error_log):
        """
        Display error message per task in following format:
        
        <Task> failed due to following error:
        <Exception>
        """
        if not error_log:
            return "All Tasks Completed Successfully!"

        msg = ''
        pattern_print = """'%s' task failed due to following error:\n%s\n"""
        for task, ex in error_log.items():
            msg += pattern_print % (task, ex)

        return msg

    def execute(self):
        if not self.task_list:
            self.task_list = self._get_all_tasks()

        error_log = {}
        execution_list = []

        for task in self.task_list:
            try:
                task_to_execute = getattr(self.app, task)()
                execution_list.append(task_to_execute)
            except Exception as ex:
                error_log[task] = ex

        # build luigi with given valid tasks
        # do not run luigi if any of the task is not valid
        if not error_log:
            luigi.build(execution_list)
            luigi.run()

        print self._error_display(error_log)


class TasksRegister(object):
    
    DEFAULT_APP_STRING = 'apps.%s'
    QUERY = """INSERT INTO luigi_task_details (task_name) VALUES ('%s')"""
    GET_QUERY = """SELECT task_name FROM luigi_task_details"""

    def __init__(self, app_name, tasks_list):
        self.sqlconn = MySQLdb.connect(
                                        settings.SQL_HOST,
                                        settings.SQL_USER,
                                        settings.SQL_PASSWORD,
                                        settings.SQL_REPORTING_DB
                                    )
        self.sqlconn.cursor = self.sqlconn.cursor()
        self.sqlconn.autocommit(True)

        self.registered_tasks = self._get_registered_tasks_list()

        if app_name:
            self.app_name = self.DEFAULT_APP_STRING % app_name
        else:
            self.app_name = app_name
        self.tasks_list = tasks_list

    def _get_registered_tasks_list(self):
        if self.sqlconn.cursor.execute(self.GET_QUERY):
            return [task[0] for task in self.sqlconn.cursor.fetchall()]
        return []

    def _get_all_tasks(self, app):
        l = []
        for key, val in app.__dict__.items():
            if isclass(val) and issubclass(val, luigi.Task):
                l.append(key)

        return l

    def execute(self):
        """
        Register new tasks
        """
        # Append app_name to default string type
        # or in case app_name is not there, get all apps
        if not self.app_name:
            app_list = settings.INSTALLED_APPS
        else:
            app_list = [self.app_name]

        # extract all tasks for give app list
        # if tasks are not provided
        if not self.tasks_list:
            self.tasks_list = []
            for app_name in app_list:
                app = import_module(app_name + '.tasks')
                self.tasks_list += self._get_all_tasks(app)

        existing_registered = []
        newly_registered = []
        for task_name in self.tasks_list:
            if task_name in self.registered_tasks:
                existing_registered.append(task_name)
                continue
            
            self.sqlconn.cursor.execute(self.QUERY % task_name)
            newly_registered.append(task_name)

        self.sqlconn.close()

        print "<Success>\nFollowing tasks added to task list:\n%s" % str(newly_registered)
        if existing_registered:
            print "Already Registered Tasks found:\n%s" % str(existing_registered)


class ProjectCreator(object):

    def __init__(self, project_name):
        self.project_name = project_name

    def execute(self):
        template_base_loc = Utilities.get_module_abs_path(conf)
        project_template_loc = ''.join([template_base_loc, 'project_template'])
        template_files = os.listdir(project_template_loc)

        current_dir = os.getcwd()
        project_loc = '/'.join([current_dir, self.project_name])
        os.makedirs(project_loc)
        for fname in template_files:
            from_loc = '/'.join([project_template_loc, fname])

            to_file = fname.replace('-tpl', '')
            to_loc = '/'.join([project_loc, to_file])

            shutil.copyfile(from_loc, to_loc)


class AppCreator(object):

    def __init__(self, app_name):
        self.app_name = app_name

    def execute(self):
        template_base_loc = Utilities.get_module_abs_path(conf)
        project_template_loc = ''.join([template_base_loc, 'app_template'])
        template_files = os.listdir(project_template_loc)

        current_dir = os.getcwd()
        app_loc = '/'.join([current_dir, self.app_name])

        os.makedirs(app_loc)
        os.makedirs('/'.join([app_loc, 'tests']))
        os.makedirs('/'.join([app_loc, 'runners']))

        for fname in template_files:
            from_loc = '/'.join([project_template_loc, fname])

            to_file = fname.replace('-tpl', '')
            to_loc = '/'.join([current_dir, to_file])

            shutil.copyfile(from_loc, to_loc)

# Start Projects/Apps
def execute_entry_point_from_cmd():
    runner_type = sys.argv[1]
    runner_name = sys.argv[2]

    if runner_type == 'startproject':
        execution_master = ProjectCreator(runner_name)
    elif runner_type == 'startapp':
        execution_master = AppCreator(runner_name)
    else:
        return

    execution_master.execute()

# Load Apps
def execute_task_from_cmd(runner_type, runner_name, runner_list=[]):
    """
    Executes the corresponding tasks for a given app.
    """

    if runner_type == 'tasks':
        execution_master = TasksExecutioner(runner_name, runner_list)
    elif runner_type == 'tests':
        execution_master = TestsExecutioner(runner_name, runner_list)
    elif runner_type == 'register_tasks':
        execution_master = TasksRegister(runner_name, runner_list)
    else:
        return

    execution_master.execute()