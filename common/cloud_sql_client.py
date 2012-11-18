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

from datetime import datetime
from datetime import timedelta
import httplib2
import json
import logging
import os
import pprint
import time

from google.appengine.api import rdbms


class CloudSQLClient(object):
    def __init__(self, instance, database):
        """Constructor.

        Args:
            instance (string): CloudSQL instance, eg "mlab-metrics:api".
            database (string): CloudSQL database, eg "data".
        """
        self._instance = instance
        self._database = database

    def Query(self, query):
        # Issue the query.
        conn = rdbms.connect(instance=self._instance, database=self._database)
        cursor = conn.cursor()
        #logging.debug('Issuing query: %s' % query)
        cursor.execute(query)

        # Parse the response data into a more convenient dict, with members
        # 'fields' for row names and 'data' for row data.
        if cursor.description is None:  # Probably not a SELECT.
            result = None
        else:
            result = { 'fields': [d[0] for d in cursor.description],
                       'data': cursor.fetchall() }

        conn.close()
        return result

    def Update(self, table_name, metric_name, data):
        new_settings = ['%s="%s"' % (k, v) for (k, v) in data.iteritems()]

        self.Query('UPDATE %s'
                   '   SET %s'
                   ' WHERE name="%s"' %
                   (table_name, ', '.join(new_settings), metric_name))

    def Create(self, table_name, metric_name, data):
        new_settings = ['%s="%s"' % (k, v) for (k, v) in data.iteritems()]

        self.Query('INSERT INTO %s'
                   '   SET name="%s", %s' %
                   (table_name, metric_name, ', '.join(new_settings)))

    def Delete(self, table_name, metric_name):
        self.Query('DELETE FROM %s'
                   ' WHERE name="%s"' %
                   (table_name, metric_name))
