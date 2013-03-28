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

"""This module contains logic for carrying out create/modify/delete tasks.
"""

from collections import defaultdict
import datetime
import logging
import numpy
import pprint
import re
import time
import urllib

from google.appengine.api import runtime
from google.appengine.api import taskqueue
from google.appengine.ext import webapp

from common import backend as backend_interface
from common import big_query_backend
from common import big_query_client
from common import cloud_sql_backend
from common import cloud_sql_client
import server

_DATE_RE = r'^([1-9][0-9]{3})_([0-9]{2})$'
_DATE_FMT = r'%04d_%02d'
_MIN_ENTRIES_THRESHOLD = 100
_MAX_RESULTS_PER_CYCLE = 300000
_COUNTRY_SPLITS = (
    'AND connection_spec.client_geolocation.country_code < "N"',
    'AND connection_spec.client_geolocation.country_code >= "N"',
    )
_STANDARD_QUERY_PARAMS = {
    'select': """
web100_log_entry.connection_spec.remote_ip as client_ip,
web100_log_entry.connection_spec.local_ip as server_ip,

connection_spec.client_geolocation.country_code as country,
connection_spec.client_geolocation.region as region,
connection_spec.client_geolocation.city as city
""",
    'from': """[%(table_name)s]""",
    'where': """
IS_EXPLICITLY_DEFINED(project)
AND project = 0

AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)

AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_code)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.city)
%(country_split)s

AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
AND connection_spec.data_direction = 1

AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
AND web100_log_entry.is_last_entry = True

AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals)
AND web100_log_entry.snap.CongSignals > 0

AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time)
""",
    'group_by': """
client_ip,
server_ip,
country,
region,
city
""" }
_LOCALES_QUERY = """
SELECT
connection_spec.client_geolocation.country_code as country_id,
connection_spec.client_geolocation.region as region_id,
connection_spec.client_geolocation.city as city_name,
connection_spec.client_geolocation.latitude as latitude,
connection_spec.client_geolocation.longitude as longitude

FROM
[%(table_name)s]

WHERE
IS_EXPLICITLY_DEFINED(project)
AND project = 0
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_code)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.city)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.latitude)
AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.longitude)

GROUP BY
country_id,
region_id,
city_name,
latitude,
longitude
"""


def HANDLERS():
    """Returns a list of URL handlers for this application.

    Returns:
        (list) A list of (string, fn) tuples where the first element is a target
        URL and the second is a function that handles requests at that URL.
    """
    return [
        ('/_ah/start', StartupHandler),
        ('/_ah/task', TaskRequestHandler),
        ('/_ah/stop', ShutdownHandler),
    ]


class RequestType:
    """Requests to the workers backend can be referenced by these constants.
    """
    DELETE_METRIC = 'delete_metric'
    REFRESH_METRIC = 'refresh_metric'
    UPDATE_METRIC = 'update_metric'
    UPDATE_LOCALES = 'update_locales'


def SendTaskRequest(params):
    if 'metric' in params:
        logging.info('Sending %s request to the worker queue for metric: %s'
                     % (params['request'].upper(), params['metric']))
    else:
        logging.info('Sending %s request to the worker queue.'
                     % params['request'].upper())
    taskqueue.add(target='worker', url='/_ah/task', params=params)


class StartupHandler(webapp.RequestHandler):
    """Handle a request to start the backend worker.
    """
    def get(self):
        """Handles a "get" request to start the backend worker.

        Noop.
        """
        logging.info('Received START request.')
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Started...')
        # Noop.  All work is done when the task is received.


class ShutdownHandler(webapp.RequestHandler):
    """Handle a request to stop the backend worker.
    """
    def get(self):
        """Handles a "get" request to stop the backend worker.

        Noop.
        """
        logging.info('Received STOP request.')
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Shutting down.')
        # Noop.  Workers watch & catch runtime.is_shutting_down().


class TaskRequestHandler(webapp.RequestHandler):
    """Handle a task request.
    """
    def post(self):
        """Handles a "get" request to do work specified in the task.

        This is where all of the brains of the worker exists. Tasks are parsed
        and work is dispatched to the appropriate method for completion. Valid
        work tasks include deleting, refreshing, and updating metric data, as
        well as updating locale data.
        """
        request = self.request.get('request', default_value=None)
        metric = self.request.get('metric', default_value=None)
        date = self.request.get('date', default_value=None)
        logging.info('Received work task. {request: %s, metric: %s, date: %s}'
                     % (request, metric, date))

        if date is not None:
            match = re.match(_DATE_RE, date)
            year = int(match.groups()[0])
            month = int(match.groups()[1])
            date = datetime.date(year, month, 1)

        backends = BackendConnections()
        metricworker = MetricWorker(backends)
        localeworker = LocaleWorker(backends)

        # Dispatch the task request.
        if request == RequestType.DELETE_METRIC:
            metricworker.DeleteMetric(metric)
        elif request == RequestType.REFRESH_METRIC:
            metricworker.RefreshMetric(metric, date)
        elif request == RequestType.UPDATE_METRIC:
            metricworker.UpdateMetric(metric, date)
        elif request == RequestType.UPDATE_LOCALES:
            localeworker.UpdateLocales()
        else:
            logging.error('Unrecognized request: %s' % request)


class BackendConnections(object):
    """Manage connections to backend datastores.
    """
    def __init__(self):
        """Constructor.
        """
        # Connect to BigQuery & CloudSQL.
        bq_client = big_query_client.AppAssertionCredentialsBQClient(
            big_query_backend.PROJECT_ID, big_query_backend.DATASET)
        self.bigquery = big_query_backend.BigQueryBackend(bq_client)

        cs_client = cloud_sql_client.CloudSQLClient(
            cloud_sql_backend.INSTANCE, cloud_sql_backend.DATABASE)
        self.cloudsql = cloud_sql_backend.CloudSQLBackend(cs_client)


class LocaleWorker(object):
    """Carry out all computations on locale data.
    """
    def __init__(self, backends):
        """Constructor.

        Args:
            backends (BackendConnections object): Backend datastores.
        """
        self._backends = backends

    def UpdateLocales(self):
        """Updates all locales based on last month's data.
        """
        # Calculate last month.
        today = datetime.date.today()
        first = datetime.date(day=1, month=today.month, year=today.year)
        last_month = first - datetime.timedelta(days=1)
        date_tup = (last_month.year, last_month.month)

        # Generate the query string.
        table = '%s.%s' % (big_query_backend.DATASET,
                           big_query_backend.DATE_TABLES_FMT % date_tup)
        query = _LOCALES_QUERY % {'table_name': table}

        # Parse results, update cities in the datastore.
        total_rows = 0
        results = self._backends.bigquery.RawQuery(query)
        cities = []
        for row in results.Rows():

            row_d = dict(zip(results.ColumnNames(), row))
            row_d['city_id'] = urllib.quote(row_d['city_name'].encode('utf-8'))
            locale = '_'.join([row_d['country_id'], row_d['region_id'], row_d['city_id']])
            if row_d['region_id'] == '00':
                parent = row_d['country_id']
            else:
                parent = '_'.join([row_d['country_id'], row_d['region_id']])
            latitude = float(row_d['latitude'])
            longitude = float(row_d['longitude'])

            if self._backends.cloudsql.SetCityData(
                locale, row_d['city_name'], parent, latitude, longitude):
                total_rows += 1

        logging.info('Added/updated %d cities.' % total_rows)


class MetricWorker(object):
    """Carry out all computations on metric data.
    """
    def __init__(self, backends):
        """Constructor.

        Args:
            backends (BackendConnections object): Backend datastores.
        """
        self._backends = backends

    def DeleteMetric(self, metric):
        """Deletes the given metric and all of its data.

        Args:
            metric (string): The metric to be deleted.
        """
        logging.info('Deleting metric: %s' % metric)

        if self._ShuttingDown():
            logging.info('Interrupted!  Shutting down.')
            return

        self._DeleteMetricInfo(metric)
        self._DeleteMetricData(metric)
        logging.info('Work completed.')

    def RefreshMetric(self, metric, date):
        """Refreshes the given metric at the given date.

        Args:
            metric (string): The metric to be refreshed.
            date (string): The date to be refreshed.
        """
        if metric is None or date is None:
            self._ExpandMetricRequest(RequestType.REFRESH_METRIC, metric, date)
            return

        logging.info('Refreshing metric: %s date: %s' % (metric, date))
        if self._ShuttingDown():
            logging.info('Interrupted!  Shutting down.')

        self._backends.cloudsql.CreateMetricDataTable(metric)
        self._ComputeMetricData(metric, date)
        logging.info('Work completed.')

    def UpdateMetric(self, metric, date):
        """Updates (recomputes) the given metric at the given date.

        Args:
            metric (string): The metric to be updated.
            date (string): The date to be updated.
        """
        if metric is None or date is None:
            self._ExpandMetricRequest(RequestType.UPDATE_METRIC, metric, date)
            return

        logging.info('Updating (recomputing) metric: %s' % metric)
        if self._ShuttingDown():
            logging.info('Interrupted!  Shutting down.')
            return

        self._backends.cloudsql.CreateMetricDataTable(metric)
        self._DeleteMetricData(metric, date)
        self._ComputeMetricData(metric, date)

    def _ShuttingDown(self):
        return runtime.is_shutting_down()

    def _ExpandMetricRequest(self, request_type, metric, date):
        # Expand metric names.
        if metric is None:
            try:
                metric_infos = self._backends.cloudsql.GetMetricInfo()
            except backend_interface.LoadError as e:
                logging.error('Dropping request {type: %s, metric: %s, date: %s}: %s'
                              % (request_type, metric, date, e))
                return

            more_specific_request = {'request': request_type}
            if date is not None:
                more_specific_request['date'] = date
            for metric in metric_infos.keys():
                more_specific_request['metric'] = metric
                SendTaskRequest(more_specific_request)

        # Expand metric dates. This can only be done with the metric name specified.
        elif date is None:
            if request_type == RequestType.REFRESH_METRIC:
                bq_dates = self._backends.bigquery.ExistingDates()
                cs_dates = self._backends.cloudsql.ExistingDates(metric_name=metric)
                dates_to_compute = set(bq_dates) - set(cs_dates)

            elif request_type == RequestType.UPDATE_METRIC:
                dates_to_compute = self._backends.bigquery.ExistingDates()

            else:
                logging.error('Unable to expand metric request type: %s' % request_type)
                return

            more_specific_request = {'metric': metric, 'request': request_type}
            for date in sorted(dates_to_compute):
                more_specific_request['date'] = _DATE_FMT % (date.year, date.month)
                SendTaskRequest(more_specific_request)

    def _DeleteMetricInfo(self, metric):
        logging.info('Deleting metric info for "%s".' % metric)
        self._backends.cloudsql.DeleteMetricInfo(metric)

    def _DeleteMetricData(self, metric, date=None):
        if date is None:
            logging.info('Deleting metric data for "%s".' % metric)
            self._backends.cloudsql.DeleteMetricData(metric)
        else:
            logging.info('Deleting metric data for "%s" at %4d-%02d.'
                         % (metric, date.year, date.month))
            self._backends.cloudsql.DeleteMetricData(metric, (date.year, date.month))

    def _ComputeMetricData(self, metric, date):
        logging.info('START computing metric data for "%s" at %4d-%02d.'
                     % (metric, date.year, date.month))

        missing_params = []
        query = self._backends.cloudsql.GetMetricInfo(metric)['query']
        for param in _STANDARD_QUERY_PARAMS:
            if param not in query:
                missing_params.append(param)

        if len(missing_params):
            logging.error('ABORTED computing metric data for "%s" at %4d-%02d.'
                          ' Query string missing parameters: %s'
                          % (metric, date.year, date.month, missing_params))
            return

        # Insert "standard" parameters into the query.
        date_tup = (date.year, date.month)
        table = '%s.%s' % (big_query_backend.DATASET,
                           big_query_backend.DATE_TABLES_FMT % date_tup)

        query = query % _STANDARD_QUERY_PARAMS
        queries = []

        for split in _COUNTRY_SPLITS:
            final_subs = {'table_name': table, 'country_split': split}
            queries.append(query % final_subs)

        # Parse query results from BigQuery into lists based on locale.
        metric_values = {'city': defaultdict(list),
                         'region': defaultdict(list),
                         'country': defaultdict(list),
                         'world': {'world': []}}

        total_rows = 0
        results = []
        for q in queries:
            results.append(self._backends.bigquery.RawQuery(q))
            results[-1].max_rows_to_bucket = _MAX_RESULTS_PER_CYCLE

        for result_set in results:
            for row in result_set.Rows():
                total_rows += 1

                row_d = dict(zip(result_set.ColumnNames(), row))
                row_d['city'] = urllib.quote(row_d['city'].encode('utf-8'))
                row_d['value'] = float(row_d['value'])

                city = '_'.join([row_d['country'], row_d['region'], row_d['city']])
                region = '_'.join([row_d['country'], row_d['region']])
                country = row_d['country']

                metric_values['city'][city].append(row_d['value'])
                metric_values['region'][region].append(row_d['value'])
                metric_values['country'][country].append(row_d['value'])
                metric_values['world']['world'].append(row_d['value'])

        if total_rows == 0:
            logging.info('ABORTED computing metric data for "%s" at %4d-%02d.'
                         ' Query produced no results.'
                         % (metric, date.year, date.month))
            return
        logging.info('Got %d rows from BigQuery.' % total_rows)

        # Remove locales that don't meet the minimum threshold.
        skipped_locales = {}

        for locale_type in metric_values:
            for locale in metric_values[locale_type]:
                if len(metric_values[locale_type][locale]) < _MIN_ENTRIES_THRESHOLD:
                    if locale_type not in skipped_locales:
                        skipped_locales[locale_type] = [locale]
                    else:
                        skipped_locales[locale_type].append(locale)

        for locale_type in skipped_locales:
            for locale in skipped_locales[locale_type]:
                del metric_values[locale_type][locale]

        logging.debug('Skipping %d locales with fewer than %d entries.'
                      % (sum(len(skipped_locales[lt]) for lt in skipped_locales),
                         _MIN_ENTRIES_THRESHOLD))

        # Write all data out to CloudSQL (keyed by locale & date).
        print_once = True
        for locale_type in metric_values:
            num_locales = len(metric_values[locale_type])
            num_entries = sum(len(metric_values[locale_type][loc])
                              for loc in metric_values[locale_type])
            logging.info('Adding %d new %s locale rows (from %d tests) to CloudSQL.'
                         % (num_locales, locale_type, num_entries))

            for locale in metric_values[locale_type]:
                #todo: support operations other than median
                median = numpy.median(metric_values[locale_type][locale])
                if print_once:
                    logging.debug('Example locale "%s" has median %f from data: %s'
                                  % (locale, median, metric_values[locale_type][locale]))
                    print_once = False
                self._backends.cloudsql.SetMetricData(metric, date_tup, locale, median)

        logging.info('FINISHED computing metric data for "%s" at %4d-%02d.'
                     % (metric, date.year, date.month))

if __name__ == '__main__':
    server.start(HANDLERS())
