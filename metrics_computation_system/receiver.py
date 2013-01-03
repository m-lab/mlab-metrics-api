# Copyright 2013 Google Inc. All Rights Reserved.
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

"""This module contains logic for receiving create/modify/delete requests.
"""

import logging

from google.appengine.ext import webapp
from google.appengine.api import taskqueue

import server
from worker import RequestType
from worker import SendTaskRequest


def HANDLERS():
    """Returns a list of URL handlers for this application.

    Returns:
        (list) A list of (string, fn) tuples where the first element is a target
        URL and the second is a function that handles requests at that URL.
    """
    return [
        ('/delete', DeleteMetricHandler),
        ('/refresh', RefreshMetricHandler),
        ('/update', UpdateMetricHandler),
        ('/relocate', UpdateLocalesHandler),
    ]


class DeleteMetricHandler(webapp.RequestHandler):
    """Handle a request to delete metrics data.
    """
    #todo: unused, right? delete this handler?
    def get(self):
        """Handles a "get" request to delete a metric and its data.

        The request is sent to the backend system as a work task, where it is
        actually completed.
        """
        metric = self.request.get('metric', default_value=None)

        if metric is None:
            logging.error('Ignoring empty DELETE request.')
            return

        # Pass the delete request on to the worker pool.
        SendTaskRequest({'request': RequestType.DELETE_METRIC, 'metric': metric})


class RefreshMetricHandler(webapp.RequestHandler):
    """Handle a request to refresh metrics data.
    """
    def get(self):
        """Handles a "get" request to refresh metric data.

        The request is sent to the backend system as a work task, where it is
        actually completed.
        """
        metric = self.request.get('metric', default_value=None)

        if metric is None:  # No metrics specified.
            logging.error('Ignoring empty REFRESH request.')
            return

        # Pass the refresh request on to the worker pool.
        if metric == '*':  # Refresh all metrics.
            SendTaskRequest({'request': RequestType.REFRESH_METRIC})
        else:
            SendTaskRequest({'request': RequestType.REFRESH_METRIC, 'metric': metric})


class UpdateMetricHandler(webapp.RequestHandler):
    """Handle a request to update (recompute) metrics data.
    """
    def get(self):
        """Handles a "get" request to update metric data.

        The request is sent to the backend system as a work task, where it is
        actually completed.
        """
        metric = self.request.get('metric', default_value=None)

        if metric is None:
            logging.error('Ignoring empty UPDATE request.')
            return

        # Pass the update request on to the worker pool.
        SendTaskRequest({'request': RequestType.UPDATE_METRIC, 'metric': metric})


class UpdateLocalesHandler(webapp.RequestHandler):
    """Handle a request to update (recompute) locales.
    """
    def get(self):
        """Handles a "get" request to update locales data.

        The request is sent to the backend system as a work task, where it is
        actually completed.
        """
        # Pass the update request on to the worker pool.
        SendTaskRequest({'request': RequestType.UPDATE_LOCALES})


if __name__ == '__main__':
    server.start(HANDLERS())
