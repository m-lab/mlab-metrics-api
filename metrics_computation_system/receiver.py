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

from google.appengine.ext import webapp
from google.appengine.api import taskqueue

import server


def HANDLERS():
    return [
        ('/delete', DeleteMetricHandler),
        ('/refresh', RefreshMetricHandler),
        ('/update', UpdateMetricHandler),
    ]


class DeleteMetricHandler(webapp.RequestHandler):
    def get(self):
        metric = self.request.get('metric', default_value=None)

        if metric is None:
            logging.error('Ignoring empty DELETE request.')
            return

        # Pass the delete request on to the worker pool.
        _SendBackendRequest({'metric': metric, 'request': 'delete_metric'})


class RefreshMetricHandler(webapp.RequestHandler):
    def get(self):
        metrics = self.request.get('metric', default_value='').split(',')

        if metrics == ['']:  # No metrics specified.
            logging.error('Ignoring empty REFRESH request.')
            return
        if metrics == ['*']:  # All metrics.
            metrics = _FetchMetricNames()

        # Pass the refresh request on to the worker pool.
        for metric in metrics:
            _SendBackendRequest({'metric': metric, 'request': 'refresh_metric'})
        _RefreshLocales()


class UpdateMetricHandler(webapp.RequestHandler):
    def get(self):
        metric = self.request.get('metric', default_value=None)

        if metric is None:
            logging.error('Ignoring empty UPDATE request.')
            return

        # Pass the update request on to the worker pool.
        _SendBackendRequest({'metric': metric, 'request': 'update_metric'})
        _RefreshLocales()


def _FetchMetricNames():
    names = ['a','b','c']
    #todo: get metric names from CloudSQL
    return names

def _SendBackendRequest(params):
    if 'metric' in params:
        logging.info('Sending %s request to the worker queue for metric: %s'
                     % (params['request'].upper(), params['metric']))
    else:
        logging.info('Sending %s request to the worker queue.'
                     % params['request'].upper())

    taskqueue.add(target='worker', url='/_ah/task', params=params)

def _RefreshLocales():
    # Request regeneration of locale data.
    _SendBackendRequest({'request': 'refresh_locales'})


if __name__ == '__main__':
    server.start(HANDLERS())
