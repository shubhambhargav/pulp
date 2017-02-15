#!/usr/bin/env python

import os
import bottle
import subprocess
from bottle import response
from pulp.conf import settings

TASK_OS_COMMAND = settings.TASK_OS_COMMAND

app = bottle.Bottle()

def runner_function(app_name, task_list):
    cmd = TASK_OS_COMMAND % {'app_name':  app_name, 'task_list': task_list.replace(',', ' ')}
    proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    if 'failed' in out:
        response.status = 400
        return out
    return '{"detail": "Ok"}'

app.get('/v1/run/task/<app_name>/<task_list>')(runner_function)