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

from apiclient.discovery import build
from google.appengine.api import memcache
from oauth2client.appengine import AppAssertionCredentials

MAX_RESULTS_PER_PACKET = 1000


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
        self._start_time = None
        self._total_timeout = None
        self._max_results_per_packet = MAX_RESULTS_PER_PACKET

        # BigQuery sometimes returns the previous query response.
        self._previous_job_id = ''

        self._Connect()

    def Query(self, query, timeout_msec=1000 * 60):
        self._total_timeout = timedelta(milliseconds=timeout_msec)
        self._start_time = datetime.now()

        # Issue the query.
        logging.debug('Query: %s' % query)
        jobs = self._service.jobs()
        job_id = self._previous_job_id

        while job_id == self._previous_job_id:
            response = jobs.query(
                body={'query': query,
                      'timeoutMs': self.VerifyTimeMSecLeft(),
                      'maxResults': self._max_results_per_packet},
                projectId=self.project_id).execute()
            job_id = response['jobReference']['jobId']
            logging.debug(('Response[%s]: %s' % (job_id, response))[:1500])

        # Wait for the query to complete.
        while not response['jobComplete']:
            response = self._GetQueryResponse(jobs, job_id, 0)
            logging.debug(('Response[%s]: %s' % (job_id, response))[:1500])

        # Parse the response data into a more convenient dict, with members
        # 'fields' for row names and 'data' for row data.
        if 'schema' not in response or response['totalRows'] == 0:
            raise QueryError('Query produced no results: %s' % query)

        result = {'fields': [], 'data': []}
        for field in response['schema']['fields']:
            result['fields'].append(field['name'])

        current_row = 0
        while 'rows' in response and current_row < response['totalRows']:
            for row in response['rows']:
                result['data'].append([field['v'] for field in row['f']])

            current_row += len(response['rows'])
            if current_row < response['totalRows']:
                response = self._GetQueryResponse(jobs, job_id, current_row)
                logging.debug(('Response[%s]: %s' % (job_id, response))[:1500])

        self._previous_job_id = job_id
        return result

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

    def _GetQueryResponse(self, jobs, job_id, start_index):
        return jobs.getQueryResults(timeoutMs=self.VerifyTimeMSecLeft(),
                                    projectId=self.project_id,
                                    jobId=job_id,
                                    maxResults=self._max_results_per_packet,
                                    startIndex=start_index).execute()

    def VerifyTimeMSecLeft(self):
        if self._start_time is None or self._total_timeout is None:
            raise TimeoutError('Start time and/or total timeout not set.')

        time_taken = datetime.now() - self._start_time
        if time_taken >= self._total_timeout:
            raise TimeoutError('Client timeout reached at %s msec.' %
                               time_taken.total_seconds() * 1000)

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
