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

"""This module contains a client for interacting with the CloudSQL API.
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
    """CloudSQL client.
    """
    def __init__(self, instance, database):
        """Constructor.

        Args:
            instance (string): CloudSQL instance, eg "mlab-metrics:api".
            database (string): CloudSQL database, eg "data".
        """
        self._instance = instance
        self._database = database

    def Query(self, query):
        """Issues a query to CloudSQL.

        Args:
            query (string): The query to be issued.

        Returns:
            (dict) Dictionary of results, split among 'fields' which describe
            the columns of the result and 'data' which contains rows of result
            data.
        """
        # Issue the query.
        conn = rdbms.connect(instance=self._instance, database=self._database)
        cursor = conn.cursor()
        cursor.execute(query)

        # Parse the response data into a more convenient dict, with members
        # 'fields' for row names and 'data' for row data.
        if cursor.description is None:  # Probably not a SELECT.
            result = None
        else:
            result = { 'fields': tuple(d[0] for d in cursor.description),
                       'data': cursor.fetchall() }

        conn.commit()
        conn.close()
        return result

    def Update(self, table_name, metric_name, data):
        """Updates data for a given table and metric name.

        Basically an SQL 'UPDATE'.

        Args:
            table_name (string): The table to edit.
            metric_name (string): The name of the metric to update.
            data (dict): Dictionary of key-value pair data to update.
        """
        new_settings = ['%s="%s"' % (k, v) for (k, v) in data.iteritems()]

        self.Query('UPDATE %s'
                   '   SET %s'
                   ' WHERE name="%s"' %
                   (table_name, ', '.join(new_settings), metric_name))

    def Create(self, table_name, metric_name, data):
        """Creates new metric data for the given name.

        Basically an SQL 'INSERT'.

        Args:
            table_name (string): The table to edit.
            metric_name (string): The name of the metric to create.
            data (dict): Dictionary of key-value pair data to add.
        """
        new_settings = ['%s="%s"' % (k, v) for (k, v) in data.iteritems()]

        self.Query('INSERT INTO %s'
                   '   SET name="%s", %s' %
                   (table_name, metric_name, ', '.join(new_settings)))

    def Delete(self, table_name, metric_name):
        """Deletes the specified metric from the given table.

        Basically an SQL 'DELETE'.

        Args:
            table_name (string): The table to edit.
            metric_name (string): The name of the metric to delete.
        """
        self.Query('DELETE FROM %s'
                   ' WHERE name="%s"' %
                   (table_name, metric_name))
