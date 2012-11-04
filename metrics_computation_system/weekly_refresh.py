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

import httplib
import logging

from google.appengine.ext import webapp

import server


def HANDLERS():
    return [
        ('/cron/weekly_refresh', WeeklyRefreshHandler),
    ]


class WeeklyRefreshHandler(webapp.RequestHandler):
    def get(self):
        logging.info('Starting weekly refresh of all metrics.')
        host = self.request.headers['Host']
        path = '/refresh?metric=*'

        logging.debug('Sending request to: %s%s' % (host, path))
        conn = httplib.HTTPConnection(host)
        conn.request('GET', path)

        res = conn.getresponse()
        logging.debug('Request response: %s %s' % (res.status, res.reason))


if __name__ == '__main__':
    server.start(HANDLERS())
