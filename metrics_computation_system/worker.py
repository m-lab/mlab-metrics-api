# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Author: Dylan Curley

import logging
import time

from google.appengine.ext import webapp
from google.appengine.api import runtime

import server


def HANDLERS():
    return [
        ('/_ah/start', StartupHandler),
        ('/_ah/task', TaskRequestHandler),
        ('/_ah/stop', ShutdownHandler),
    ]


class StartupHandler(webapp.RequestHandler):
    def get(self):
        logging.info('Received START request.')
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Started...')
        # Noop.  All work is done when the task is received.


class ShutdownHandler(webapp.RequestHandler):
    def get(self):
        logging.info('Received STOP request.')
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Shutting down.')
        # Noop.  Workers watch & catch runtime.is_shutting_down().


class TaskRequestHandler(webapp.RequestHandler):
    def post(self):
        request = self.request.get('request', default_value=None)
        metric = self.request.get('metric', default_value=None)
        logging.info('Received work task. {request: %s, metric: %s}'
                     % (request, metric))

        if request == 'delete_metric':
            _DeleteMetric(metric)
        elif request == 'refresh_metric':
            _RefreshMetric(metric)
        elif request == 'update_metric':
            _UpdateMetric(metric)
        elif request == 'update_locales':
            _UpdateLocales()
        else:
            logging.error('Unrecognized request: %s' % request)


def _DeleteMetric(metric):
    #todo
    # cloudsql = ...
    while not runtime.is_shutting_down():
        logging.debug('zzz in _DeleteMetric(%s)' % metric)
        time.sleep(20)
    logging.debug('Shutting down.')

def _RefreshMetric(metric):
    #todo
    # bigquery = ...
    # cloudsql = ...
    while not runtime.is_shutting_down():
        logging.debug('zzz in _RefreshMetric(%s)' % metric)
        time.sleep(20)
    logging.debug('Shutting down.')

def _UpdateMetric(metric):
    #todo
    # bigquery = ...
    # cloudsql = ...
    while not runtime.is_shutting_down():
        logging.debug('zzz in _UpdateMetric(%s)' % metric)
        time.sleep(20)
    logging.debug('Shutting down.')

def _UpdateLocales():
    #todo
    # cloudsql = ...
    while not runtime.is_shutting_down():
        logging.debug('zzz in _UpdateLocales()')
        time.sleep(20)
    logging.debug('Shutting down.')


if __name__ == '__main__':
    server.start(HANDLERS())
