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

"""Runs an AppEngine webapp and issues a request to BigQuery.

This is just a tiny program to demonstrate how to issue an authenticated request
to BigQuery from AppEngine.  Note that you must first register your AppEngine
account with the BigQuery project like so:

  1. Determine your AppEngine Service Account Name via the Application Settings
     page (under Administration).  This is an email address.  Look for:
       <your_project_name>@appspot.gserviceaccount.com
  2. Add this account email address to the Team page for your BigQuery project.
     You must enable at least "Can view" access for the following code to work.
  3. Change the 'PROJECT_ID' and 'TEST_QUERY' below, accordingly.
  4. Set the 'application' target in app.yaml for your AppEngine project.
  5. Upload this code to AppEngine.  It won't work with the 'dev_appserver.py'
     script because AppEngine alone can act as your privileged Service Account.
     Specifically, run:  enable-app-engine-project . && appcfg.py update .

Depends on:
  AppEngine Python API
      -- https://developers.google.com/appengine/downloads
  Google API Client Library & OAuth2 Client
      -- http://code.google.com/p/google-api-python-client
"""

import httplib2
import logging

from apiclient.discovery import build
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials

PROJECT_ID = 'measurement-lab'
TEST_QUERY = ('SELECT name, units, short_desc, long_desc, query'
              '  FROM metrics_api_server.metadata')


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
    results = job_runner.query(
        projectId=PROJECT_ID, body={'query': TEST_QUERY}).execute()
    logging.debug('Result: %s' % results)

    # Format the results as an HTML page.
    body = '<h2>The Query</h2><pre>%s</pre>\n<hr>\n' % TEST_QUERY

    if not results[u'jobComplete']:
        body += '<em>Query failed!</em>'
    else:
        tablerows = '<tr>'
        for field in results[u'schema'][u'fields']:
            tablerows += '<th>%s</th>' % field[u'name']

        for row in results[u'rows']:
            tablerows += '</tr><tr>'
            for value in row[u'f']:
                tablerows += '<td>%s</td>' % value[u'v']
        tablerows += '</tr>'

        body += '<table border=1>\n%s\n</table>\n' % tablerows

    return '<!DOCTYPE html><html><body>%s</body></html>' % body


if __name__=="__main__":
    main()
