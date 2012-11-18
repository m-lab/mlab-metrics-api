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

from apiclient import errors
from apiclient.discovery import build
from google.appengine.api import memcache
from oauth2client.appengine import AppAssertionCredentials

MAX_RESULTS_PER_PACKET = 2000


class Error(Exception):
    pass

class ConnectionError(Error):
    pass

class TimeoutError(Error):
    pass

class QueryError(Error):
    pass


class _BigQueryClient(object):
    def __init__(self, project_id, dataset):
        self.project_id = project_id
        self.dataset = dataset
        self._job_id = None
        self._max_results_per_packet = MAX_RESULTS_PER_PACKET
        self._start_time = None
        self._total_timeout = None

        self._Connect()

    def IssueQuery(self, query):
        # Issue the query.
        logging.debug('Query: %s' % query)
        request = {'configuration': {'query': {'query': query}}}
        insertion = self._service.jobs().insert(
            projectId=self.project_id, body=request).execute()
        self._job_id = insertion['jobReference']['jobId']
        return 0  #todo: Return a unique query id.

    def HasMoreQueryResults(self):
        return self._job_id is not None

    def GetQueryResults(self, current_row, timeout_msec=1000 * 60 * 10,
                        max_rows_to_retrieve=10000):
        if self._job_id is None:
            return None

        self._total_timeout = timedelta(milliseconds=timeout_msec)
        self._start_time = datetime.now()

        # Get the response.
        rows_left = max_rows_to_retrieve
        response = self._GetQueryResponse(current_row, max_rows_to_retrieve)

        if 'rows' in response:
            current_row += len(response['rows'])
            if max_rows_to_retrieve is not None:
                rows_left -= len(response['rows'])

        while ((max_rows_to_retrieve is None or rows_left > 0)
               and current_row < int(response['totalRows'])):
            more_data = self._GetQueryResponse(current_row, rows_left)

            if 'schema' not in response or 'fields' not in response['schema']:
                if 'schema' in more_data and 'fields' in more_data['schema']:
                    response['schema'] = more_data['schema']
            if 'rows' in more_data:
                current_row += len(more_data['rows'])
                if max_rows_to_retrieve is not None:
                    rows_left -= len(more_data['rows'])
                response['rows'].extend(more_data['rows'])

        # Clear _job_id if all rows have been retrieved.
        if current_row >= int(response['totalRows']):
            self._job_id = None

        # Parse the response data into a more convenient dict, with members
        # 'fields' for row names and 'data' for row data.
        if 'schema' not in response or int(response['totalRows']) == 0:
            logging.error('Query produced no results!')
            return (None, None)

        result = {'fields': [], 'data': []}
        for field in response['schema']['fields']:
            result['fields'].append(field['name'])
        for row in response['rows']:
            result['data'].append([field['v'] for field in row['f']])

        return (current_row, result)

    def ListTables(self):
        """Retrieves a list of the current tables.
        """
        tables = self._service.tables()
        reply = tables.list(projectId=self.project_id,
                            datasetId=self.dataset).execute()

        return [t['tableReference']['tableId'] for t in reply['tables']]

    def UpdateTable(self, table_name, fields, field_data):
        """Update a given table with the passed new 'field_data'.

        BigQuery does not currently support updates, so the recommended way to
        update table data is to create a new table with the desired data, then
        request a copy of the new table to overwrite the old table.
        https://developers.google.com/bigquery/docs/developers_guide#deletingrows
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

    def _GetQueryResponse(self, start_index, rows_to_retrieve, retries=4):
        if rows_to_retrieve is None:
            max_results = MAX_RESULTS_PER_PACKET
        else:
            max_results = min(MAX_RESULTS_PER_PACKET, rows_to_retrieve)

        jobs = self._service.jobs()
        data = {'status': {'state': 'RUNNING'}}

        while 'status' in data and data['status']['state'] == 'RUNNING':
            try:
                data = jobs.getQueryResults(timeoutMs=self.VerifyTimeMSecLeft(),
                                            projectId=self.project_id,
                                            jobId=self._job_id,
                                            maxResults=max_results,
                                            startIndex=start_index).execute()
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
                           % (self._job_id, start_index, data))[:300])
        return data

    def VerifyTimeMSecLeft(self):
        if self._start_time is None or self._total_timeout is None:
            raise TimeoutError('Start time and/or total timeout not set.')

        time_taken = datetime.now() - self._start_time
        if time_taken >= self._total_timeout:
            raise TimeoutError('Client timeout reached at %s msec.' %
                               (time_taken.total_seconds() * 1000))

        return int((self._total_timeout - time_taken).total_seconds() * 1000)


class AppAssertionCredentialsBQClient(_BigQueryClient):
    def _Connect(self):
        # Certify BigQuery access credentials.
        self._credentials = AppAssertionCredentials(
            scope='https://www.googleapis.com/auth/bigquery')
        self._http = self._credentials.authorize(httplib2.Http(memcache))
        self._service = build('bigquery', 'v2', http=self._http)


class ClientSecretsBQClient(_BigQueryClient):
    def _Connect(self):
        self._http = None

    def SetClientHTTP(self, http):
        self._http = http

    def Query(self, query, timeout_msec=1000 * 60):
        self._service = build('bigquery', 'v2', http=self._http)
        return super(ClientSecretsBQClient, self).Query(query, timeout_msec)
