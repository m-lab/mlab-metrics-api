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

"""This module contains logic for triggering weekly refreshes of metric data.
"""

import httplib
import logging

from google.appengine.ext import webapp

import server


def HANDLERS():
    """Returns a list of URL handlers for this application.

    Returns:
        (list) A list of (string, fn) tuples where the first element is a target
        URL and the second is a function that handles requests at that URL.
    """
    return [
        ('/cron/weekly_refresh', WeeklyRefreshHandler),
    ]


class WeeklyRefreshHandler(webapp.RequestHandler):
    """Handle a request to send a metrics refresh request to the receiver.
    """
    def get(self):
        """Handles "get" requests to send a metrics refresh request.
        """
        host = self.request.headers['Host']

        logging.info('Requesting weekly refresh of all metrics.')
        self._SendRequest(host, '/refresh?metric=*')
        logging.info('Requesting weekly update of all locales.')
        self._SendRequest(host, '/relocate')

    def _SendRequest(self, host, path):
        logging.debug('Sending request to: %s%s' % (host, path))
        conn = httplib.HTTPConnection(host, timeout=20)
        conn.request('GET', path)

        res = conn.getresponse()
        logging.debug('Request response: %s %s' % (res.status, res.reason))


if __name__ == '__main__':
    server.start(HANDLERS())
