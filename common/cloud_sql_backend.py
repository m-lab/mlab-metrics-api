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
LOCALE_TYPES_TABLE = 'locale_types'
LOCALES_TABLE = 'locales'
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
        self._city_ids_by_name = None
        self._country_ids_by_name = None
        self._region_ids_by_name = None
        self._type_ids_by_name = None

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
            (dict) Result data from the query, with keys "id", "locale", "name",
            "parent_id", "lat" (latitude), and "lon" (longitude).
        """
        query = ('SELECT locales.id, locale, locales.name, parent_id, lat, lon'
                 '  FROM %s, %s'
                 ' WHERE locales.type_id = locale_types.id'
                 '   AND locale_types.name = "%s"' %
                 (LOCALES_TABLE, LOCALE_TYPES_TABLE, locale_type))
        return self._cloudsql.Query(query)

    def SetCityData(self, locale, name, parent, lat, lon):
        """Sets/updates the passed city locale data.

        Args:
            locale (string): Full locale name, globally unique.
            name (string): City name.
            panent (string): Parent locale's full name, globally unique.
            lat (float): Latitude of this city.
            lon (float): Longitude of this city.
        """
        # Determine all locale types, and their associated keys.
        if self._type_ids_by_name is None:
            query = ('SELECT id, name'
                     '  FROM %s' %
                     LOCALE_TYPES_TABLE)
            types = self._cloudsql.Query(query)
            self._type_ids_by_name = dict((t[1], int(t[0])) for t in types['data'])

        if 'city' not in self._type_ids_by_name or 'region' not in self._type_ids_by_name:
            raise KeyError('Locale types (%s) do not include "city" or "region". '
                           'Cannot insert city "%s".' % (self._type_ids_by_name, name))
        type_id = self._type_ids_by_name['city']

        # Determine all parents (regions/countries), and their associated keys.
        if self._region_ids_by_name is None:
            query = ('SELECT id, locale'
                     '  FROM %s'
                     ' WHERE type_id=%d' %
                     (LOCALES_TABLE, self._type_ids_by_name['region']))
            regions = self._cloudsql.Query(query)
            self._region_ids_by_name = dict((r[1], int(r[0])) for r in regions['data'])

        if self._country_ids_by_name is None:
            query = ('SELECT id, locale'
                     '  FROM %s'
                     ' WHERE type_id=%d' %
                     (LOCALES_TABLE, self._type_ids_by_name['country']))
            countries = self._cloudsql.Query(query)
            self._country_ids_by_name = dict((c[1], int(c[0])) for c in countries['data'])

        if parent in self._region_ids_by_name:
            parent_id = self._region_ids_by_name[parent]
        elif parent in self._country_ids_by_name:
            parent_id = self._country_ids_by_name[parent]
        else:
            logging.error('Cannot find parent locale "%s". Cannot insert city "%s".' %
                          (parent, name))
            return False

        # Determine all cities, and their associated keys.
        if self._city_ids_by_name is None:
            query = ('SELECT id, locale'
                     '  FROM %s'
                     ' WHERE type_id=%d' %
                     (LOCALES_TABLE, self._type_ids_by_name['city']))
            cities = self._cloudsql.Query(query)
            self._city_ids_by_name = dict((c[1], int(c[0])) for c in cities['data'])

        # If this city exists then update it, otherwise insert it.
        if locale in self._city_ids_by_name:
            city_id = self._city_ids_by_name[locale]
            query = ('UPDATE %s'
                     '   SET name="%s", parent_id=%d, lat=%f, lon=%f'
                     ' WHERE id=%d' %
                     (LOCALES_TABLE, name, parent_id, lat, lon, city_id))
            self._cloudsql.Query(query)

        else:
            query = ('INSERT'
                     '  INTO %s'
                     '   SET locale="%s", name="%s", parent_id=%d, lat=%f, lon=%f, type_id=%d' %
                     (LOCALES_TABLE, locale, name, parent_id, lat, lon, type_id))
            self._cloudsql.Query(query)
            query = ('SELECT id'
                     '  FROM %s'
                     ' WHERE locale = "%s"' %
                     (LOCALES_TABLE, locale))
            city_id = self._cloudsql.Query(query)
            if city_id is None or len(city_id['data']) == 0:
                raise KeyError('Failed to insert city "%s" locale "%s".' %
                               (name, locale))
            self._city_ids_by_name[locale] = city_id['data'][0][0]

        return True
