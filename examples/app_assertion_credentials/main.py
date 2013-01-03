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

"""Runs an AppEngine webapp and issues a request to BigQuery.

See the README file for more details.
"""

import httplib2
import logging

from apiclient.discovery import build
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials

PROJECT_ID = 'measurement-lab'
TEST_QUERY = """
SELECT
web100_log_entry.connection_spec.remote_ip,
web100_log_entry.connection_spec.local_ip,

connection_spec.client_geolocation.country_code3 as country,
connection_spec.client_geolocation.region as region,
connection_spec.client_geolocation.city as city,

MAX( web100_log_entry.snap.HCThruOctetsAcked /
     ( web100_log_entry.snap.SndLimTimeRwin +
       web100_log_entry.snap.SndLimTimeCwnd +
       web100_log_entry.snap.SndLimTimeSnd
     )
   ) as value


FROM [m_lab.2009_10]


WHERE
IS_EXPLICITLY_DEFINED(project)
AND project = 0

AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)

AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_code3)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.city)

AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
AND connection_spec.data_direction = 1

AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
AND web100_log_entry.is_last_entry = True

AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked)
AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
AND web100_log_entry.snap.HCThruOctetsAcked < 1000000000

AND ( web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd
    ) >= 9000000
AND ( web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd
    ) < 3600000000

AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals)
AND web100_log_entry.snap.CongSignals > 0


GROUP BY
web100_log_entry.connection_spec.remote_ip,
web100_log_entry.connection_spec.local_ip,
country,
region,
city

LIMIT 800
"""
TEST_QUERY = ' '.join(TEST_QUERY.split('\n'))


def main():
    """Initializes the AppEngine server."""
    logging.getLogger().setLevel(logging.DEBUG)
    application = webapp.WSGIApplication([('/', TestQueryHandler)])
    run_wsgi_app(application)


class TestQueryHandler(webapp.RequestHandler):
    """Handles a request for the query page."""
    def get(self):
        self.response.out.write(TestQuery())


def TestQuery():
    """Runs a test query against the measurement-lab BigQuery database.
    
    Returns:
        (string) The query results formatted as an HTML page.
    """
    # Certify BigQuery access credentials.
    credentials = AppAssertionCredentials(
        scope='https://www.googleapis.com/auth/bigquery')
    http = credentials.authorize(httplib2.Http(memcache))
    service = build('bigquery', 'v2', http=http)
    job_runner = service.jobs()

    # Run a query against the BigQuery database.
    logging.debug('Query: %s' % TEST_QUERY)
    jobdata = {'configuration': {'query': {'query': TEST_QUERY}}}
    insert = job_runner.insert(projectId=PROJECT_ID,
                               body=jobdata).execute()
    logging.debug('Response: %s' % insert)

    currentRow = 0
    queryReply = job_runner.getQueryResults(
        projectId=PROJECT_ID,
        jobId=insert['jobReference']['jobId'],
        startIndex=currentRow).execute()
    results = queryReply

    while 'rows' in queryReply and currentRow < queryReply['totalRows'] :
        currentRow += len(queryReply['rows'])
        queryReply = job_runner.getQueryResults(
            projectId=PROJECT_ID,
            jobId=queryReply['jobReference']['jobId'],
            startIndex=currentRow).execute()
        if 'schema' not in results or 'fields' not in results['schema']:
            if 'schema' in queryReply and 'fields' in queryReply['schema']:
                results['schema'] = queryReply['schema']
        if 'rows' in queryReply:
            results['rows'].extend(queryReply['rows'])

    # Format the results as an HTML page.
    body = '<h2>The Query</h2><pre>%s</pre>\n<hr>\n' % TEST_QUERY

    tablerows = '<tr>'
    for field in results['schema']['fields']:
        tablerows += '<th>%s</th>' % field['name']

    for row in results['rows']:
        tablerows += '</tr><tr>'
        for value in row['f']:
            tablerows += '<td>%s</td>' % value['v']
    tablerows += '</tr>'

    body += '<table border=1>\n%s\n</table>\n' % tablerows

    return '<!DOCTYPE html><html><body>%s</body></html>' % body


if __name__=="__main__":
    main()
