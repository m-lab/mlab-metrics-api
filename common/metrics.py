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
import logging

import backend as backend_interface
import big_query_client

#todo: use this or delete it
MAX_LOADED_METRICS_KEYS = 300

# Timeout when cached metrics should be considered old.
METRICS_REFRESH_RATE = timedelta(days=2)

_last_metrics_info_refresh = datetime.fromtimestamp(0)


class Error(Exception):
    pass

class LoadError(Error):
    pass

class LookupError(Error):
    pass

class RefreshError(Error):
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
    def __init__(self, backend, name, load_info=True):
        """Constructor.

        Args:
            backend (object): Backend interface, e.g. BigQueryBackend.
            name (string): This metric's name.
        """
        self._backend = backend
        self.name = name
        self.short_desc = None
        self.long_desc = None
        self.units = None
        self.query = None
        self.data = dict()
        self._metadata = dict()

        if load_info:
            self._LoadInfo()
    
    def Describe(self):
        """Describes this metric.
 
        Returns:
            (dict) Information about this metric in a dict.  Specifically,
            { 'name': (string) <metric name>,
              'short_desc': (string) <short description>,
              'long_desc': (string) <long description>,
              'units': (string) <metric units>,
              'query': (string) <bigquery query that produced this metric> }
        """
        return {'name'               : self.name,
                'short_desc'         : self.short_desc,
                'long_desc'          : self.long_desc,
                'units'              : self.units,
                'query'              : self.query}

    def Update(self, units=None, short_desc=None, long_desc=None, query=None):
        """Updates the metric with passed data.

        Args:
            units (optional, string): Metric units, e.g. 'Mbps'.
            short_desc (optional, string): Short text description.
            long_desc (optional, string): Long text description.
            query (optional, string): BigQuery query that produces this metric.
        """
        if units is not None:
            self.units = units
        if short_desc is not None:
            self.short_desc = short_desc
        if long_desc is not None:
            self.long_desc = long_desc
        if query is not None:
            self.query = query

    def Lookup(self, year, month, locale):
        """Looks up metric data for a given year, month, and locale.

        Args:
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

        if date not in self.data or locale not in self.data[date]:
            self._LoadData(date, locale)

        if date not in self.data or locale not in self.data[date]:
            raise LookupError('No data for metric=%s, year=%d, month=%d,'
                              ' locale=%s.' % (self.name, year, month, locale))

        return {'metric': self.name,
                'units' : self.units,
                'value' : self.data[date][locale]}

    def _LoadInfo(self):
        """Loads/updates metadata for this metric from the backend datastore.

        The metric info is not returned, it's loaded into memory so that it can
        be queried later.
        """
        try:
            info = self._backend.GetMetricInfo()
        except backend_interface.LoadError as e:
            raise LoadError(e)

        for field in info:
            if field == 'name':
                continue
            self.__dict__[field] = result[field]

    def _LoadData(self, date, locale):
        """Loads/updates data for this metric from the backend datastore.

        The metric data is not returned, it's loaded into memory so that it can
        be queried later.
        """
        #todo: move the "load/update" logic out of the metric class.
        locale_type = DetermineLocaleType(locale)
        m_key = (date, locale_type)
        if m_key not in self._metadata:
            self._metadata[m_key] = {'last_load_time': datetime.fromtimestamp(0)}

        metrics_age = datetime.now() - self._metadata[m_key]['last_load_time']
        if metrics_age < METRICS_REFRESH_RATE:
            return
        self._metadata[m_key]['last_load_time'] = datetime.now()

        try:
            info = self._backend.GetMetricData(self.name, date, locale)
        except backend_interface.LoadError as e:
            raise LoadError(e)

        if date not in self.data:
            self.data[date] = dict()

        for row in info['data']:
            locale, value = row
            self.data[date][locale] = float(value)


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


def edit_metric(backend, metrics_dict, metric_name, units=None,
                short_desc=None, long_desc=None, query=None, delete=False):
    """Update values for the given metric.
    """
    _update_metrics_info(backend, metrics_dict, force=True)

    if delete:
        del metrics_dict[metric_name]
    else:
        if metric_name not in metrics_dict:
            metrics_dict[metric_name] = Metric(backend, metric_name,
                                               load_info=False)
        metrics_dict[metric_name].Update(units=units, short_desc=short_desc,
                                         long_desc=long_desc, query=query)

    infos = dict((m, metrics_dict[m].Describe()) for m in metrics_dict)
    backend.SetMetricInfo(infos)


def refresh(backend, metrics_dict):
    #todo: move the "refresh" logic to its own file.
    _update_metrics_info(backend, metrics_dict)
    _update_metrics_data(backend, metrics_dict)


def _update_metrics_info(backend, metrics_dict, force=False):
    #todo: move the "refresh" logic to its own file.
    global _last_metrics_info_refresh

    metrics_age = datetime.now() - _last_metrics_info_refresh
    if metrics_age < METRICS_REFRESH_RATE and not force:
        return

    try:
        metric_infos = backend.GetMetricInfo()
    except backend_interface.LoadError as e:
        raise LoadError(e)
        
    available_metrics = set(metric_infos.keys())
    known_metrics = set(metrics_dict.keys())
    old_metrics_for_deletion = known_metrics - available_metrics
    new_metrics_to_be_loaded = available_metrics - known_metrics
    logging.info('Old metrics for deletion: %s',
                 ' '.join(old_metrics_for_deletion))
    logging.info('New metrics to be loaded: %s',
                 ' '.join(new_metrics_to_be_loaded))

    if available_metrics != known_metrics:
        for old_metric in old_metrics_for_deletion:
            del metrics_dict[old_metric]
        for new_metric in new_metrics_to_be_loaded:
            info = metric_infos[new_metric]
            metric = Metric(backend, new_metric, load_info=False)
            metric.Update(units=info['units'], short_desc=info['short_desc'],
                          long_desc=info['long_desc'], query=info['query'])
            metrics_dict[new_metric] = metric
    
    _last_metrics_info_refresh = datetime.now()


def _update_metrics_data(backend, metrics_dict):
    #todo: move the "refresh" logic to its own file.
    # Do nothing here.  Metrics data will be updated on query.
    pass
