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

import base64
from collections import defaultdict
import logging
import numpy
import pprint
import time

from google.appengine.ext import webapp
from google.appengine.api import runtime

import big_query_backend
import big_query_client
import cloud_sql_backend
import cloud_sql_client
import server

_MIN_ENTRIES_THRESHOLD = 100
_MAX_RESULTS_PER_CYCLE = 300000

_STANDARD_QUERY_PARAMS = {
    'select': """
web100_log_entry.connection_spec.remote_ip as client_ip,
web100_log_entry.connection_spec.local_ip as server_ip,

connection_spec.client_geolocation.country_code3 as country,
connection_spec.client_geolocation.region as region,
connection_spec.client_geolocation.city as city
""",
    'from': """[%(table_name)s]""",
    'where': """
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


def HANDLERS():
    return [
        ('/_ah/start', StartupHandler),
        ('/_ah/task', TaskRequestHandler),
        ('/_ah/stop', ShutdownHandler),
    ]


class StartupHandler(webapp.RequestHandler):
    def get(self):
        logging.info('Received START request.')
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Started...')
        # Noop.  All work is done when the task is received.


class ShutdownHandler(webapp.RequestHandler):
    def get(self):
        logging.info('Received STOP request.')
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Shutting down.')
        # Noop.  Workers watch & catch runtime.is_shutting_down().


class TaskRequestHandler(webapp.RequestHandler):
    def post(self):
        request = self.request.get('request', default_value=None)
        metric = self.request.get('metric', default_value=None)
        logging.info('Received work task. {request: %s, metric: %s}'
                     % (request, metric))

        # Connect to BigQuery & CloudSQL.
        bq_client = big_query_client.AppAssertionCredentialsBQClient(
            big_query_backend.PROJECT_ID, big_query_backend.DATASET)
        bigquery = big_query_backend.BigQueryBackend(bq_client)

        cs_client = cloud_sql_client.CloudSQLClient(
            cloud_sql_backend.INSTANCE, cloud_sql_backend.DATABASE)
        cloudsql = cloud_sql_backend.CloudSQLBackend(cs_client)

        # Dispatch the task request.
        if request == 'delete_metric':
            _DeleteMetric(metric, cloudsql)
        elif request == 'refresh_metric':
            _RefreshMetric(metric, bigquery, cloudsql)
        elif request == 'update_metric':
            _UpdateMetric(metric, bigquery, cloudsql)
        elif request == 'update_locales':
            _UpdateLocales(cloudsql)
        else:
            logging.error('Unrecognized request: %s' % request)


def _DeleteMetric(metric, cloudsql):
    logging.info('Deleting metric: %s' % metric)

    if not runtime.is_shutting_down():
        _DeleteMetricInfo(cloudsql, metric)
        _DeleteMetricData(cloudsql, metric, date)

    if runtime.is_shutting_down():
        logging.info('Interrupted!  Shutting down.')
    else:
        logging.info('Work completed.')

def _RefreshMetric(metric, bigquery, cloudsql):
    query = cloudsql.GetMetricInfo(metric)['query']
    bq_dates = bigquery.ExistingDates()
    cs_dates = cloudsql.ExistingDates()
    missing_cs_dates = set(bq_dates) - set(cs_dates)

    logging.info('Refreshing metric %s for dates: %s'
                 % (metric, sorted(missing_cs_dates)))

    for date in sorted(missing_cs_dates):
        if not runtime.is_shutting_down():
            _ComputeMetricData(bigquery, cloudsql, query, metric, date)

    if runtime.is_shutting_down():
        logging.info('Interrupted!  Shutting down.')
    else:
        logging.info('Work completed.')

def _UpdateMetric(metric, bigquery, cloudsql):
    query = cloudsql.GetMetricInfo(metric)['query']

    logging.info('Updating (regenerating) metric: %s' % metric)

    for date in sorted(bigquery.ExistingDates()):
        if not runtime.is_shutting_down():
            _DeleteMetricData(cloudsql, metric, date)
            _ComputeMetricData(bigquery, cloudsql, query, metric, date)

    if runtime.is_shutting_down():
        logging.info('Interrupted!  Shutting down.')
    else:
        logging.info('Work completed.')


def _DeleteMetricInfo(cloudsql, metric):
    logging.info('Deleting metric info for "%s".' % metric)
    cloudsql.DeleteMetricInfo(metric)

def _DeleteMetricData(cloudsql, metric, date=None):
    if date is None:
        logging.info('Deleting metric data for "%s".' % metric)
        cloudsql.DeleteMetricData(metric)
    else:
        logging.info('Deleting metric data for "%s" at %4d-%02d.'
                     % (metric, date.year, date.month))
        cloudsql.DeleteMetricData(metric, (date.year, date.month))

def _ComputeMetricData(bigquery, cloudsql, query, metric, date):
    logging.info('START computing metric data for "%s" at %4d-%02d.'
                 % (metric, date.year, date.month))

    missing_params = []
    for param in _STANDARD_QUERY_PARAMS:
        if param not in query:
            missing_params.append(param)

    if len(missing_params):
        logging.error('ABORTED computing metric data for "%s" at %4d-%02d.'
                      ' Query string missing parameters: %s' % missing_params)
        return

    # Insert 'date' into the query.
    date_tup = (date.year, date.month)
    table = '%s.%s' % (big_query_backend.DATASET,
                       big_query_backend.DATE_TABLES_FMT % date_tup)

    try:
        query = query % _STANDARD_QUERY_PARAMS
        query = query % {'table_name': table}
    except KeyError:
        raise KeyError('Metric "%s" query doesn\'t contain "table_name" key for'
                       ' date specification:' % (metric, query))

    # Parse query results from BigQuery into lists based on locale.
    metric_values = {'city': defaultdict(list),
                     'region': defaultdict(list),
                     'country': defaultdict(list),
                     'world': {'world': []}}

    (query_id, rows) = bigquery.RawQuery(
        query, max_rows_to_retrieve=_MAX_RESULTS_PER_CYCLE)

    if rows is None:
        logging.info('ABORTED computing metric data for "%s" at %4d-%02d.'
                     ' Query produced no results.'
                     % (metric, date.year, date.month))
        return

    while rows is not None and 'data' in rows:
        logging.info('Got %d rows from BigQuery.' % len(rows['data']))

        for row in rows['data']:
            row_d = dict(zip(rows['fields'], row))
            row_d['city'] = base64.b32encode(row_d['city'].encode('utf-8'))
            row_d['value'] = float(row_d['value'])

            city = '_'.join([row_d['country'], row_d['region'], row_d['city']])
            region = '_'.join([row_d['country'], row_d['region']])
            country = row_d['country']

            metric_values['city'][city].append(row_d['value'])
            metric_values['region'][region].append(row_d['value'])
            metric_values['country'][country].append(row_d['value'])
            metric_values['world']['world'].append(row_d['value'])

        (query_id, rows) = bigquery.ContinueRawQuery(
            query_id, max_rows_to_retrieve=_MAX_RESULTS_PER_CYCLE)

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
            cloudsql.SetMetricData(metric, date_tup, locale, median)

    logging.info('FINISHED computing metric data for "%s" at %4d-%02d.'
                 % (metric, date.year, date.month))

def _UpdateLocales(cloudsql):
    #todo

    # Query CloudSQL for all cities, regions, countries.
    # For each city, region, country:
        # ...
    pass

if __name__ == '__main__':
    server.start(HANDLERS())
