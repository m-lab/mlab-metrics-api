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

"""This module contains the datastore backend implementation for BigQuery.

Included in this module is the BigQueryBackend class, a number of constants
that define specific details of the BigQuery instance and its interactions,
and a QueryResults class for use when returning raw results to a user.
"""

import datetime
import logging
import re

import backend
import big_query_client
from metrics import DetermineLocaleType

PROJECT_ID = 'measurement-lab'
DATASET = 'm_lab'
LOCALES_TABLE = '_locales'
METADATA_TABLE = '_metadata'

DATE_TABLES_RE = r'^([1-9][0-9]{3})_([0-9]{2})$'
DATE_TABLES_FMT = r'%04d_%02d'

class QueryResults():
    """A query results generator, parseable by methods Rows() and ColumnNames().
    """
    def __init__(self, bigquery, query, max_rows_to_bucket=10000):
        """Constructor.

        Args:
            bigquery (object): BigQuery client instance.
            query (string): Query to send to the BigQuery.
            max_rows_to_bucket (int): Number of rows to buffer.
        """
        self.max_rows_to_bucket = max_rows_to_bucket

        self._bigquery = bigquery
        self._columns = None
        self._row_data = None
        self._job_id = self._bigquery.IssueQuery(query)

    def ColumnNames(self):
        """Retrieves the names of the columns for these results.

        Raises:
            backend.QueryError: There are no results (thus no columns).

        Returns:
            An ordered list of column names.
        """
        if self._columns is None:
            self._FillDataBucket()
        return self._columns

    def Rows(self):
        """Retrieves a single row of results.

        Raises:
            StopIteration: No more rows exist to be returned.
            backend.QueryError: There are no results (thus no rows).

        Returns:
            A list, one row of results.
        """
        while self._HaveMoreRowData():
            self._FillDataBucket()

            if self._row_data is None:
                raise StopIteration
            for row in self._row_data:
                yield row
            self._row_data = None

    def _DataBucketHasData(self):
        return self._row_data is not None and len(self._row_data)

    def _HaveMoreRowData(self):
        return (self._DataBucketHasData()
                or self._bigquery.HasMoreQueryResults(self._job_id))

    def _FillDataBucket(self):
        if (self._DataBucketHasData()
            or not self._bigquery.HasMoreQueryResults(self._job_id)):
            return

        try:
            result = self._bigquery.GetQueryResults(
                self._job_id, max_rows_to_retrieve=self.max_rows_to_bucket)
        except big_query_client.Error as e:
            raise backend.QueryError(e)

        if result is None:
            self._columns = None
            self._row_data = None

        else:
            if 'fields' in result:
                self._columns = result['fields']
            if 'data' in result:
                self._row_data = result['data']

class BigQueryBackend(backend.Backend):
    """BigQuery backend interface honoring the backend.Backend abstraction.
    """
    def __init__(self, bigquery):
        """Constructor.

        Args:
            bigquery (object): BigQuery client instance.
        """
        self._bigquery = bigquery
        self._next_query_id = 0
        self._queries = {}
        super(BigQueryBackend, self).__init__()

    def SetClientHTTP(self, http):
        """Sets the http client to the passed 'http'.

        This is necessary when using a ClientSecretsBQClient, which validates
        via oauth2 client secrets.

        Args:
            http: The authorized client http.
        """
        self._bigquery.SetClientHTTP(http)

    def RawQuery(self, query):
        """Runs the specified query against the BigQuery.

        Args:
            query (string): Query to send to the BigQuery.

        Raises:
            backend.QueryError: There was an error issuing the query.

        Returns:
            (QueryResults) An object that generates query results for the given
            raw query.
        """
        return QueryResults(self._bigquery, query)

    def ExistingDates(self):
        """Retrieves a list of existing months.
        """
        return self._DateTableMap().keys()

    def DeleteMetricInfo(self, metric_name):
        """Deletes info for this metric.

        Args:
            metric_name (string): The name of the metric to be deleted.

        Raises:
            backend.DeleteError: The requested metric info could not be deleted.
        """
        #todo
        raise backend.DeleteError('Not yet implemented.')

    def GetMetricInfo(self, metric_name=None):
        #todo: figure out how to query once for all metrics (it's not that much
        #      data) instead of querying once for each metric
        """Retrieves metadata for the specified metric, from BigQuery.

        Args:
            metric_name (string): Name of the metric to query.  If None or not
                specified, retrieves info all metric.

        Raises:
            backend.LoadError: There was an error getting the metric info from
                BigQuery.

        Returns:
            (dict) Collection of data for the requested metric, keyed by the
            data type.  If no metric was requested, returns a dict of these
            collections (a dict inside a dict), keyed by metric name.
        """
        query = ('SELECT name, units, short_desc, long_desc, query'
                 '  FROM %s.%s' % (self._bigquery.dataset, METADATA_TABLE))
        if metric_name is not None:
            query += (' WHERE name = "%s"' % metric_name)

        try:
            result = self._bigquery.Query(query)
        except big_query_client.Error as e:
            raise backend.LoadError('Could not load metric info for "%s" from'
                                    ' BigQuery: %s' % (metric_name, e))

        if metric_name is None:
            # Create dict of info-dicts, indexed by metric name.
            infos = dict()
            for row in result['data']:
                row_dict = dict(zip(result['fields'], row))
                infos[row_dict['name']] = row_dict
            return infos
        else:
            # There should be only one row in the result.
            return dict(zip(result['fields'], result['data'][0]))

    def SetMetricInfo(self, unused_request_type, unused_metric_name, metrics_info):
        """Pushes the provided metric info to the backend data store.

        Note that *ALL* metric info is replaced by the collection of info passed
        to this method.  If info for certain metrics is not specified, they will
        be deleted.

        Args:
            metrics_info (dict): Collection of updated metric info to send to
                the backend data store, keyed by metric name.

        Raises:
            backend.LoadError: The requested updates could not be applied.
        """
        #todo: raise LoadError on failure
        fields = tuple((f, 'string')
                       for f in metrics_info[metrics_info.keys()[0]])

        field_data = []
        for metric in metrics_info:
            field_data.append(dict((f, metrics_info[metric][f])
                              for (f, _) in fields))

        self._bigquery.UpdateTable(METADATA_TABLE, fields, field_data)

    def CreateMetricDataTable(self, metric_name):
        """Creates a backend table to store metric data.

        Noop for BigQuery.
        """
        pass

    def GetMetricData(self, metric_name, date, locale):
        """Retrieves data for this metric for the given 'date' and 'locale'.

        Args:
            date (tuple): Date for which data should be loaded, given as a tuple
                consisting of ints (year, month).
            locale (string): Locale for which data should be loaded.

        Raises:
            backend.LoadError: The requested metric data could not be read. This
                may happen if, for example, a bogus locale was requested, or a
                bogus date.

        Returns:
            (dict) Result data from the query, with keys "locale" and "value".
        """
        query = ('SELECT locale, value'
                 '  FROM %s.%s'
                 ' WHERE date = "%s"' %
                 (self._bigquery.dataset, metric_name, '%d-%02d' % date))

        try:
            result = self._bigquery.Query(query)
        except big_query_client.Error as e:
            raise backend.LoadError('Could not load metric data for "%s" from'
                                    ' BigQuery: %s' % (metric_name, e))
        return result

    def DeleteMetricData(self, metric_name, date):
        """Deletes data for this metric for the given 'date'.

        Args:
            date (tuple): Date for which data should be deleted, given as a
                tuple consisting of ints (year, month).

        Raises:
            backend.DeleteError: The requested metric data could not be deleted.
        """
        #todo
        raise backend.DeleteError('Not yet implemented.')

    def GetLocaleData(self, locale_type):
        """Retrieves all locale data for the given 'locale_type'.
        
        Args:
            locale_type (string): One of "country", "region", or "city" which
                specifies the type of locale to retrieve data on.

        Raises:
            backend.LoadError: The locale data could not be retrieved.

        Returns:
            (dict) Result data from the query, with keys "locale", "name",
            "parent", "lat" (latitude), and "lon" (longitude).
        """
        #todo: figure out why this query fails without the 'WHERE'.  timeout?
        query = ('SELECT locale, name, parent, lat, lon'
                 '  FROM %s.%s'
                 ' WHERE type = "%s"' %
                 (self._bigquery.dataset, LOCALES_TABLE, locale_type))

        try:
            result = self._bigquery.Query(query)
        except big_query_client.Error as e:
            raise backend.LoadError('Could not load locale info from BigQuery:'
                                    ' %s' % e)
        return result

    def _DateTableMap(self):
        """Retrieves a map of existing months and corresponding tables.
        """
        dates = {}

        for table in self._bigquery.ListTables():
            match = re.match(DATE_TABLES_RE, table)
            if match:
                year = int(match.groups()[0])
                month = int(match.groups()[1])
                dates[datetime.date(year, month, 1)] = table

        return dates
