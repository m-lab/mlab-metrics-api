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

"""This module contains clients for interacting with the BigQuery API.

The client should be instantiated with either the ClientSecretsBQClient class or
the AppAssertionCredentialsBQClient class, depending on the desired credentials
model.

BigQuery client exceptions are defined in this module.
"""

from datetime import datetime
from datetime import timedelta
import httplib2
import json
import logging
import os
import pprint
import time

from apiclient import errors
from apiclient.discovery import build
from google.appengine.api import memcache
from oauth2client.appengine import AppAssertionCredentials

MAX_RESULTS_PER_PACKET = 2000


class Error(Exception):
    """Common exception that all other exceptions in this module inherit from.
    """
    pass

class ConnectionError(Error):
    """An error occurred while connecting to BigQuery.
    """
    pass
class TimeoutError(Error):
    """A query took too long to return results.
    """
    pass
class QueryError(Error):
    """An error occurred while querying BigQuery.
    """
    pass


class _BigQueryClient(object):
    """This class does not implement authentication, and should be subclassed.

    Use ClientSecretsBQClient() or AppAssertionCredentialsBQClient() instead, as
    those classes add authentication to the client.
    """
    def __init__(self, project_id, dataset):
        """Constructor.

        Args:
            project_id (string): BigQuery project to connect to.
            dataset (string): BigQuery dataset to connect to.
        """
        self.project_id = project_id
        self.dataset = dataset
        self._max_results_per_packet = MAX_RESULTS_PER_PACKET

        # Data members keyed by BigQuery job ID.
        self._current_row = {}
        self._has_more_data = {}
        self._start_time = {}
        self._total_timeout = {}

        self._Connect()

    def IssueQuery(self, query):
        """Issues a BigQuery query.

        Args:
            query (string): The query to issue.

        Returns:
            (int) A job ID to use for retrieving the query results.
        """
        # Issue the query.
        logging.debug('Query: %s' % query)
        request = {'configuration': {'query': {'query': query}}}
        insertion = self._service.jobs().insert(
            projectId=self.project_id, body=request).execute()

        job_id = insertion['jobReference']['jobId']
        self._current_row[job_id] = 0
        self._has_more_data[job_id] = True
        return job_id

    def HasMoreQueryResults(self, job_id):
        """Returns whether or not more result data exists for the given job.

        Args:
            job_id (int): Job ID tied to a specific query, as previously
                returned by the IssueQuery() method.

        Returns:
            (bool) True if more data exists, otherwise false.
        """
        if job_id not in self._has_more_data:
            return False
        return self._has_more_data[job_id]

    def GetQueryResults(self, job_id, timeout_msec=1000 * 60 * 10,
                        max_rows_to_retrieve=10000):
        """Retrieves query results from BigQuery for the specified job.

        Args:
            job_id (int): Job ID tied to a specific query, as previously
                returned by the IssueQuery() method.
            timeout_msec (int): Allowed runtime for results retrieval. Defaults
                to 10 minutes.
            max_rows_to_retrieve (int): Number of rows to retrieve. Defaults to
                10000 rows.
        """
        if not self.HasMoreQueryResults(job_id):
            return None

        self._total_timeout[job_id] = timedelta(milliseconds=timeout_msec)
        self._start_time[job_id] = datetime.now()

        # Get the response.
        rows_left = max_rows_to_retrieve
        response = self._GetQueryResponse(job_id, max_rows_to_retrieve)

        if 'rows' in response:
            self._current_row[job_id] += len(response['rows'])
            if max_rows_to_retrieve is not None:
                rows_left -= len(response['rows'])

        while ((max_rows_to_retrieve is None or rows_left > 0)
               and self._current_row[job_id] < int(response['totalRows'])):
            more_data = self._GetQueryResponse(job_id, rows_left)

            if 'schema' not in response or 'fields' not in response['schema']:
                if 'schema' in more_data and 'fields' in more_data['schema']:
                    response['schema'] = more_data['schema']
            if 'rows' in more_data:
                self._current_row[job_id] += len(more_data['rows'])
                if max_rows_to_retrieve is not None:
                    rows_left -= len(more_data['rows'])
                response['rows'].extend(more_data['rows'])

        # Note if all rows have been retrieved.
        if self._current_row[job_id] >= int(response['totalRows']):
            self._has_more_data[job_id] = False

        # Parse the response data into a more convenient dict, with members
        # 'fields' for row names and 'data' for row data.
        if 'schema' not in response or int(response['totalRows']) == 0:
            logging.error('Query produced no results!')
            self._has_more_data[job_id] = False
            return None

        result = {'fields': [], 'data': []}
        for field in response['schema']['fields']:
            result['fields'].append(field['name'])
        for row in response['rows']:
            result['data'].append([field['v'] for field in row['f']])

        return result

    def ListTables(self):
        """Retrieves a list of the current tables.

        Returns:
            (tuple) List of table names.
        """
        tables = self._service.tables()
        reply = tables.list(projectId=self.project_id,
                            datasetId=self.dataset).execute()

        return tuple(t['tableReference']['tableId'] for t in reply['tables'])

    def UpdateTable(self, table_name, fields, field_data):
        """Update a given table with the passed new 'field_data'.

        BigQuery does not currently support updates, so the recommended way to
        update table data is to create a new table with the desired data, then
        request a copy of the new table to overwrite the old table.
        https://developers.google.com/bigquery/docs/developers_guide#deletingrows

        Args:
            table_name (string): Name of the table to update.
            fields (list of string tuples): A list of columns, where each list
                element is a (string, string) tuple describing the column name
                and the column type, e.g. 'int' or 'float'.
            field_data (list of tuples): A list of row data to create in the
                updated table. This should contain *all* table data as existing
                table data won't be preserved.
        """
        # Create a new (updated) table at "<table_name>_<timestamp>".
        tmp_table = '%s_%s' % (table_name, datetime.now().strftime('%s'))
        fmt_fields = ',\n'.join('{"name": "%s", "type": "%s"}'
                                % (f, t) for (f, t) in fields)
        table_data = ('--xxx\n'
                      'Content-Type: application/json; charset=UTF-8\n'
                      '\n'
                      '{\n'
                      '  "configuration": {\n'
                      '    "load": {\n'
                      '      "schema": {\n'
                      '        "fields": [\n'
                      '          %s\n'
                      '        ]\n'
                      '      },\n'
                      '      "destinationTable": {\n'
                      '        "projectId": "%s",\n'
                      '        "datasetId": "%s",\n'
                      '        "tableId": "%s"\n'
                      '      }\n'
                      '    }\n'
                      '  }\n'
                      '}\n'
                      '--xxx\n'
                      'Content-Type: application/octet-stream\n'
                      '\n' % (fmt_fields, self.project_id, self.dataset, tmp_table))
        for data in field_data:
            table_row = ','.join('"%s"' % data[f] for (f, _) in fields)
            table_data += '%s\n' % table_row
        table_data += '--xxx--\n'

        logging.debug('Creating temporary table: %s\n%s' % (tmp_table, table_data))
        upload_url = ('https://www.googleapis.com/upload/bigquery/v2/projects/'
                      '%s/jobs' % self.project_id)
        headers = {'Content-Type': 'multipart/related; boundary=xxx'}
        reply, status = self._http.request(upload_url, method='POST', body=table_data,
                                           headers=headers)

        status = json.loads(status)
        while status['status']['state'] in ('PENDING', 'RUNNING'):
            time.sleep(2)
            reply, status = self._http.request(status['selfLink'], method='GET')
            status = json.loads(status)

        logging.debug('Creation request head: %s' % reply)
        logging.debug('Creation request content: %s' % status)

        if 'errors' in status['status']:
            logging.error('Error creating temporary table: %s' %
                          status['status']['errorResult']['message'])
            return

        # Overwrite the old/existing table with the temporary table.
        update_targets = {
            "projectId": self.project_id,
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset,
                        "tableId": tmp_table,
                    },
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset,
                        "tableId": table_name,
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE"
                }
            }
        }

        logging.debug('Overwriting old/existing table with the temporary table.')
        job = self._service.jobs()
        reply = job.insert(projectId=self.project_id, body=update_targets).execute()
        status = job.get(projectId=self.project_id,
                         jobId=reply['jobReference']['jobId']).execute()

        while status['status']['state'] in ('PENDING', 'RUNNING'):
            time.sleep(2)
            status = job.get(projectId=self.project_id,
                             jobId=reply['jobReference']['jobId']).execute()

        if 'errors' in status['status']:
            logging.error('Error updating table: %s' %
                          status['status']['errorResult']['message'])
            return
        logging.debug('Finished updating table with status: %s' %
                      pprint.saferepr(status))

    def _GetQueryResponse(self, job_id, rows_to_retrieve, retries=4):
        if rows_to_retrieve is None:
            max_results = MAX_RESULTS_PER_PACKET
        else:
            max_results = min(MAX_RESULTS_PER_PACKET, rows_to_retrieve)

        jobs = self._service.jobs()
        data = {'status': {'state': 'RUNNING'}}

        while 'status' in data and data['status']['state'] == 'RUNNING':
            try:
                data = jobs.getQueryResults(
                    timeoutMs=self._VerifyTimeMSecLeft(job_id),
                    projectId=self.project_id,
                    jobId=job_id,
                    maxResults=max_results,
                    startIndex=self._current_row[job_id]).execute()
            except errors.Error as e:
                if retries > 0:
                    logging.error('Query failed; attempting %d more times.'
                                  ' Error: %s' % (retries, e))
                    time.sleep(2)     # Sometimes there's an intermittent error,
                    max_results /= 2  # or the response is too large to return.
                    retries -= 1
                    continue
                else:
                    raise
            logging.debug(('Response[job_id=%s, start_index=%d]: %s'
                           % (job_id, self._current_row[job_id], data))[:300])
        return data

    def _VerifyTimeMSecLeft(self, job_id):
        if self._start_time[job_id] is None or self._total_timeout[job_id] is None:
            raise TimeoutError('Start time and/or total timeout not set.')

        time_taken = datetime.now() - self._start_time[job_id]
        if time_taken >= self._total_timeout[job_id]:
            raise TimeoutError('Client timeout reached at %s msec.' %
                               (time_taken.total_seconds() * 1000))

        return int((self._total_timeout[job_id] - time_taken).total_seconds() * 1000)


class AppAssertionCredentialsBQClient(_BigQueryClient):
    """BigQuery client implemented with App Assertion Credentials.

    Use this BigQuery client if the application credentials should be used for
    BigQuery transactions.
    """
    def _Connect(self):
        # Certify BigQuery access credentials.
        self._credentials = AppAssertionCredentials(
            scope='https://www.googleapis.com/auth/bigquery')
        self._http = self._credentials.authorize(httplib2.Http(memcache))
        self._service = build('bigquery', 'v2', http=self._http)


class ClientSecretsBQClient(_BigQueryClient):
    """BigQuery client implemented with Client Secret Credentials.

    Use this BigQuery client if the client's credentials (e.g. the user) should
    be used for BigQuery transactions. This guarantees that only registered and
    trusted clients can connect to BigQuery.
    """
    def _Connect(self):
        self._http = None

    def SetClientHTTP(self, http):
        """Sets the client-certified http credentials.

        Args:
            http (http object): Client-certified http credentials.
        """
        self._http = http

    def Query(self, query, timeout_msec=1000 * 60):
        """Issues a query to BigQuery.

        Args:
            query (string): The query to be issued.
            timeout_msec (int): Amount of time the query is allowed to run.
                Defaults to 1 minute.
        """
        #todo: Does this method actually do anything? Does super.Query() exist?
        self._service = build('bigquery', 'v2', http=self._http)
        return super(ClientSecretsBQClient, self).Query(query, timeout_msec)
