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

"""This module contains classes and functions for dealing with Metric data.
"""

from datetime import datetime
from datetime import timedelta
import logging

import backend as backend_interface

#todo: use this or delete it
MAX_LOADED_METRICS_KEYS = 300

# Timeout when cached metrics should be considered old.
METRICS_REFRESH_RATE = timedelta(days=2)

_last_metrics_info_refresh = datetime.fromtimestamp(0)


class Error(Exception):
    """Common exception that all other exceptions in this module inherit from.
    """
    pass

class LoadError(Error):
    """An error occurred loading metric data.
    """
    pass
class LookupError(Error):
    """An error occurred querying or looking up metric data.
    """
    pass
class RefreshError(Error):
    """An error occurred refreshing metric data.
    """
    pass


class Metric(object):
    """A single metric, including metadata and all associated data.

    Note that this class also deals with the filesystem and makes certain
    assumptions about how data (and metadata) is laid out on disk.  Also, data
    is loaded on demand instead of up front.  This is intended to mitigate RAM
    usage at the expense of load time, but load time is only impacted on first
    request and should be fast for all subsequent requests (until the cache
    expires).
    """
    def __init__(self, name, units, short_desc, long_desc, query):
        """Constructor.

        Args:
            name (string): This metric's name.
            units (string): Units the metric is measured in.
            short_desc (string): Short description.
            long_desc (string): Long description.
            query (string): BigQuery query string to compute this metric.
        """
        self.name = name
        self.units = units
        self.short_desc = short_desc
        self.long_desc = long_desc
        self.query = query
        self._data = dict()
        self._metadata = dict()

    def Lookup(self, backend, year, month, locale):
        """Looks up metric data for a given year, month, and locale.

        Args:
            backend (Backend): Datastore backend.
            year (int): Year to retrieve.
            month (int): Month to retrieve.
            locale (int): Locale for which metric data should be given.

        Raises:
            LookupError: If the requested data doesn't exist.

        Returns:
            (dict) The requested data as a dict.  Specifically,
            { 'metric': (string) <metric name>,
              'units': (string) <metric units>,
              'value': (float) <metric value> }
        """
        #todo: allow regex lookups
        date = (year, month)

        if date not in self._metadata:
            self._metadata[date] = dict()
        self._metadata[date]['last_request_time'] = datetime.now()

        if date not in self._data or locale not in self._data[date]:
            self._LoadData(backend, date, locale)

        if date not in self._data or locale not in self._data[date]:
            raise LookupError('No data for metric=%s, year=%d, month=%d,'
                              ' locale=%s.' % (self.name, year, month, locale))

        return {'metric': self.name,
                'units' : self.units,
                'value' : self._data[date][locale]}

    def _LoadData(self, backend, date, locale):
        """Loads/updates data for this metric from the backend datastore.

        The metric data is not returned, it's loaded into memory so that it can
        be queried later.
        """
        locale_type = DetermineLocaleType(locale)
        m_key = (date, locale_type)
        if m_key not in self._metadata:
            self._metadata[m_key] = {'last_load_time': datetime.fromtimestamp(0)}

        metrics_age = datetime.now() - self._metadata[m_key]['last_load_time']
        if metrics_age < METRICS_REFRESH_RATE:
            return
        self._metadata[m_key]['last_load_time'] = datetime.now()

        try:
            info = backend.GetMetricData(self.name, date, locale)
        except backend_interface.LoadError as e:
            raise RefreshError(e)

        if date not in self._data:
            self._data[date] = dict()

        for row in info['data']:
            locale, value = row
            self._data[date][locale] = float(value)


class MetricsManager(object):
    """Manage metrics data, specifically hiding the details of data caching.
    """
    def __init__(self, backend):
        """Constructor.

        Args:
            backend (Backend object): Datastore backend.
        """
        self._backend = backend
        self._metrics = {}
        self._last_refresh = datetime.fromtimestamp(0)

    def Exists(self, metric):
        """Whether or not a given metric exists.

        Args:
            metric (string): Metric name.

        Raises:
            RefreshError: An error occurred while refreshing the metric cache.

        Returns:
            (bool) True if the metric exists and can be queried, otherwise false.
        """
        self._Refresh()
        return metric in self._metrics

    def MetricNames(self):
        """Retrieves all metric names.

        Raises:
            RefreshError: An error occurred while refreshing the metric cache.

        Returns:
            (list) List of metric names, as strings.
        """
        self._Refresh()
        return self._metrics.keys()

    def Metric(self, metric):
        """Retrieves the given metric.

        Args:
            metric (string): Metric name.

        Raises:
            LookupError: The metric doesn't exist.
            RefreshError: An error occurred while refreshing the metric cache.

        Returns:
            (Metric) The metric object.
        """
        if not self.Exists(metric):
            raise LookupError('Unknown metric: %s' % metric)

        return self._metrics[metric]

    def SetMetric(self, metric, units, short_desc, long_desc, query):
        """Creates or updates the metric with the passed values.

        Args:
            metric (string): Metric name.
            units (string): Units the metric is measured in.
            short_desc (string): Short description.
            long_desc (string): Long description.
            query (string): BigQuery query string to compute this metric.

        Raises:
            RefreshError: An error occurred while refreshing the metric cache.
        """
        if not self.Exists(metric):
            request_type = backend_interface.RequestType.NEW
        else:
            request_type = backend_interface.RequestType.EDIT

        self._metrics[metric] = Metric(metric, units, short_desc, long_desc, query)
        infos = dict((m, {'name'      : self._metrics[m].name,
                          'short_desc': self._metrics[m].short_desc,
                          'long_desc' : self._metrics[m].long_desc,
                          'units'     : self._metrics[m].units,
                          'query'     : self._metrics[m].query})
                     for m in self._metrics)
        self._backend.SetMetricInfo(request_type, metric, infos)

    def DeleteMetric(self, metric):
        """Deletes the given metric and all of its data.

        Args:
            metric (string): Metric name.

        Raises:
            LookupError: The metric doesn't exist.
            RefreshError: An error occurred while refreshing the metric cache.
        """
        if not self.Exists(metric):
            raise LookupError('Unknown metric: %s' % metric)

        del self._metrics[metric]
        self._backend.SetMetricInfo(backend_interface.RequestType.DELETE,
                                    metric, None)

    def LookupResult(self, metric, year, month, locale):
        """Looks up metric data for a given year, month, and locale.

        Args:
            metric (string): Metric name.
            year (int): Year to retrieve.
            month (int): Month to retrieve.
            locale (int): Locale for which metric data should be given.

        Raises:
            LookupError: If the requested metric or data doesn't exist.
            RefreshError: An error occurred while refreshing the metric cache.

        Returns:
            (dict) The requested data as a dict.  Specifically,
            { 'metric': (string) <metric name>,
              'units': (string) <metric units>,
              'value': (float) <metric value> }
        """
        if not self.Exists(metric):
            raise LookupError('Unknown metric: %s' % metric)

        return self._metrics[metric].Lookup(self._backend, year, month, locale)

    def ForceRefresh(self):
        """Forces a refresh of the internal metrics data.
        """
        self._last_refresh = datetime.fromtimestamp(0)
        self._Refresh()

    def _Refresh(self):
        """Refreshes MetricsManager data at most every 'METRICS_REFRESH_RATE'.
        """
        if datetime.now() - self._last_refresh < METRICS_REFRESH_RATE:
            return

        try:
            metric_infos = self._backend.GetMetricInfo()
        except backend_interface.LoadError as e:
            raise RefreshError(e)

        available_metrics = set(metric_infos.keys())
        known_metrics = set(self._metrics.keys())
        old_metrics_for_deletion = known_metrics - available_metrics
        new_metrics_to_be_loaded = available_metrics - known_metrics
        logging.info('Old metrics for deletion: %s'
                     % ' '.join(old_metrics_for_deletion))
        logging.info('New metrics to be loaded: %s'
                     % ' '.join(new_metrics_to_be_loaded))
 
        # Update data members.
        if available_metrics != known_metrics:
            for old_metric in old_metrics_for_deletion:
                del self._metrics[old_metric]
            for new_metric in new_metrics_to_be_loaded:
                self._metrics[new_metric] = Metric(
                    new_metric,
                    metric_infos[new_metric]['units'],
                    metric_infos[new_metric]['short_desc'],
                    metric_infos[new_metric]['long_desc'],
                    metric_infos[new_metric]['query'])
        
        self._last_refresh = datetime.now()


def DetermineLocaleType(locale_str):
    """Determines the locale 'type' for a given locale name.

    Returns:
        (string) Always one of the following strings,
        'world' If the locale name refers to the world.
        'country' If the locale name looks like a country ID.
        'region' If the locale name looks like a region ID.
        'city' If the locale name looks like a city ID.
    """
    if locale_str == 'world':
        return 'world'

    depth_map = {1: 'country', 2: 'region', 3: 'city'}
    depth = len(locale_str.split('_'))

    return depth_map[depth]
