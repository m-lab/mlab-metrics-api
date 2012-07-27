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

"""This module ...

todo: Lots more text.
"""

import httplib2
import logging

from apiclient.discovery import build
from google.appengine.api import memcache
from oauth2client.appengine import AppAssertionCredentials


class Error(Exception):
    pass

class ConnectionError(Error):
    pass

class QueryError(Error):
    pass


class BigQueryClient(object):
    def __init__(self, project_id, dataset):
        self.project_id = project_id
        self.dataset = dataset
        self._server = None

        self._Connect()

    def Query(self, query):
        logging.info('Query/info')
        logging.debug('Query/debug')
        if self._server is None:
            raise ConnectionError('Lost connection to the BigQuery server?')

        response = self._server.query(
            projectId=self.project_id, body={'query': query}).execute()
        logging.debug('Query: %s' % query)
        logging.debug('Response: %s' % response)

        if not response[u'jobComplete']:
            raise QueryError('Query failed: %s' % query)

        result = {'fields': [], 'data': []}
        for field in response[u'schema'][u'fields']:
            result['fields'].append(field[u'name'])
        for row in response[u'rows']:
            result['data'].append([field[u'v'] for field in row[u'f']])

        return result

    def _Connect(self):
        # Certify BigQuery access credentials.
        self._credentials = AppAssertionCredentials(
            scope='https://www.googleapis.com/auth/bigquery')
        self._http = self._credentials.authorize(httplib2.Http(memcache))
        self._service = build('bigquery', 'v2', http=self._http)
        self._server = self._service.jobs()
