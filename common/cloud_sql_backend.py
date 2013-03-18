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

"""This module contains the datastore backend implementation for CloudSQL.

Included in this module is the CloudSQLBackend class, and number of constants
that define specific details of the CloudSQL instance and its interactions.
"""

import logging
import pprint

import backend
from metrics import DetermineLocaleType

INSTANCE = 'mlab-metrics:database'
DATABASE = 'mlab_metrics'
LOCALES_TABLE = '_locales'
METADATA_TABLE = 'definitions'
SAMPLE_METRIC_TABLE = 'num_of_clients'  # Expect 'num_of_clients' metric exists.


class CloudSQLBackend(backend.Backend):
    """CloudSQL backend interface honoring the backend.Backend abstraction.
    """
    def __init__(self, cloudsql):
        """Constructor.

        Args:
            cloudsql (object): CloudSQL client instance.
        """
        self._cloudsql = cloudsql
        super(CloudSQLBackend, self).__init__()

    def ExistingDates(self, metric_name=SAMPLE_METRIC_TABLE):
        """Retrieves a list of months for which data exists.

        Args:
            metric_name (string): The metric/table to query dates for. Defualts
                to the global 'SAMPLE_METRIC_TABLE'.

        Returns:
            (list) A list of strings, each one containing a month in the format
            'YYYY_MM' for which data exists for the specified metric name.
        """
        query = ('SELECT DISTINCT date'
                 '  FROM %s' % metric_name)

        dates = self._cloudsql.Query(query)
        return tuple(d[0] for d in dates['data'])

    def DeleteMetricInfo(self, metric_name):
        """Deletes info for this metric.

        Args:
            metric_name (string): The name of the metric to be deleted.
        """
        query = ('DELETE'
                 '  FROM %s'
                 ' WHERE name="%s"' %
                 (METADATA_TABLE, metric_name))
        self._cloudsql.Query(query)

    def GetMetricInfo(self, metric_name=None):
        """Retrieves the definition of the specified metric, from CloudSQL.

        Args:
            metric_name (string): Name of the metric to query.  If None or not
                specified, retrieves info all metric.

        Returns:
            (dict) Collection of data for the requested metric, keyed by the
            data type.  If no metric was requested, returns a dict of these
            collections (a dict inside a dict), keyed by metric name.
        """
        query = ('SELECT *'
                 '  FROM %s' % METADATA_TABLE)
        if metric_name is not None:
            query += (' WHERE name="%s"' % metric_name)

        result = self._cloudsql.Query(query)

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

    def SetMetricInfo(self, request_type, metric_name, metrics_info):
        """Pushes the provided metric info to the backend data store.

        Args:
            request_type (RequestType): 
            metric_name (string): If provided, only update the specified metric.
            metrics_info (dict): Collection of updated metric info to send to
                the backend data store, keyed by metric name.

        Raises:
            backend.EditError: The requested updates could not be applied.
        """
        if request_type != backend.RequestType.DELETE:
            new_data = dict((k, v)
                            for (k, v) in metrics_info[metric_name].iteritems()
                            if k != 'name')

        if request_type == backend.RequestType.EDIT:
            self._cloudsql.Update(METADATA_TABLE, metric_name, new_data)
        elif request_type == backend.RequestType.NEW:
            self._cloudsql.Create(METADATA_TABLE, metric_name, new_data)
        elif request_type == backend.RequestType.DELETE:
            self._cloudsql.Delete(METADATA_TABLE, metric_name)
        else:
            raise backend.EditError('Unrecognized request type: %s' % request_type)

    def DeleteMetricData(self, metric_name, date=None):
        """Deletes data for this metric for the given 'date'.

        Args:
            metric_name (string): The name of the metric to be deleted.
            date (tuple): Date for which metric data should be deleted, given as
                a tuple consisting of ints (year, month).  If None, all data for
                'metric_name' will be deleted.
        """
        if date is None:
            query = ('DROP TABLE %s' % metric_name)
        else:
            query = ('DELETE'
                     '  FROM %s'
                     ' WHERE date="%s"' %
                     (metric_name, '%4d-%02d-01' % date))

        self._cloudsql.Query(query)

    def CreateMetricDataTable(self, metric_name):
        """Creates a backend table to store metric data.

        Args:
            metric_name (string): Metric name associated with this table data.
        """
        query = ('CREATE'
                 ' TABLE IF NOT EXISTS %s ('
                 '    locale VARCHAR(64) NOT NULL,'
                 '    date DATE NOT NULL,'
                 '    value FLOAT NOT NULL'
                 ' )'
                 % metric_name)
        self._cloudsql.Query(query)

    def GetMetricData(self, metric_name, date, locale):
        """Retrieves data for this metric for the given 'date' and 'locale'.

        Args:
            date (tuple): Date for which data should be loaded, given as a tuple
                consisting of ints (year, month).
            locale (string): Locale for which data should be loaded.

        Returns:
            (dict) Result data from the query, with keys "locale" and "value".
        """
        query = ('SELECT locale, value'
                 '  FROM %s'
                 ' WHERE date="%s"' %
                 (metric_name, '%4d-%02d-01' % date))
        return self._cloudsql.Query(query)

    def SetMetricData(self, metric_name, date, locale, value):
        """Sets data for this metric for the given 'date' and 'locale'.

        Args:
            date (tuple): Date for which data should be loaded, given as a tuple
                consisting of ints (year, month).
            locale (string): Locale for which data should be loaded.
            value (float): The metric value to be loaded.
        """
        date_fmt = '%4d-%02d-01' % date

        query = ('DELETE'
                 '  FROM %s'
                 ' WHERE locale="%s" AND date="%s"' %
                 (metric_name, locale, date_fmt))
        self._cloudsql.Query(query)

        query = ('INSERT'
                 '  INTO %s'
                 '   SET locale="%s",date="%s",value=%f' %
                 (metric_name, locale, date_fmt, value))
        self._cloudsql.Query(query)

    def GetLocaleData(self, locale_type):
        """Retrieves all locale data for the given 'locale_type'.
        
        Args:
            locale_type (string): One of "country", "region", or "city" which
                specifies the type of locale to retrieve data on.

        Returns:
            (dict) Result data from the query, with keys "locale", "name",
            "parent", "lat" (latitude), and "lon" (longitude).
        """
        query = ('SELECT locale, name, parent, lat, lon'
                 '  FROM %s'
                 ' WHERE type="%s"' %
                 (LOCALES_TABLE, locale_type))
        return self._cloudsql.Query(query)

    def SetLocaleData(self, locale_type, locale, name, parent, lat, lon):
        """Sets/updates a locale in the database.
        
        Args:
            locale_type (string): Type of locale ("city" "region" "country").
            locale (string): Locale ID.
            name (string): Locale name.
            parent (string): Parent locale ID.
            lat (float): Latitude, south is negative.
            lon (float): Longitude, west is negative.
        """
        query = ('DELETE'
                 '  FROM %s'
                 ' WHERE locale="%s"' %
                 (LOCALES_TABLE, locale))
        self._cloudsql.Query(query)

        query = ('INSERT'
                 '  INTO %s'
                 '   SET type="%s",locale="%s",name="%s",parent="%s",lat=%f,lon=%f' %
                 (LOCALES_TABLE, locale_type, locale, name, parent, lat, lon))
        self._cloudsql.Query(query)
