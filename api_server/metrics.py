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
import logging
import os

METRICS_DIR = '../data'
INFO_FILE = 'metric_info.txt'

#todo: use this or delete it
MAX_LOADED_METRICS_KEYS = 300

# Refresh metrics at most every X seconds.
METRICS_REFRESH_RATE = 3600

_last_metrics_info_refresh = datetime.fromtimestamp(0)


class Error(Exception):
    pass

class LoadError(Error):
    pass

class RefreshError(Error):
    pass


class Metric(object):
    class MetricData(object):
        pass

    def __init__(self, name):
        self.name = name
        self.short_desc = None
        self.long_desc = None
        self.units = None  #todo: create metric units, follow through to the UI
        self.query = None
        self.data = dict()
        self.metadata = dict()

        self.LoadInfoFile()

    def LoadInfoFile(self):
        fname = os.path.join(METRICS_DIR, self.name, INFO_FILE)

        #todo: this won't work for (ideally) multiline data, like sample api
        #      queries. use protobufs instead
        try:
            with open(fname, 'r') as fd:
                lines = fd.readlines()
        except IOError:
            raise LoadError('Could not load metric info for "%s" from file: %s'
                            % (self.name, fname))

        file_data = dict(l.strip().split(':', 1) for l in lines)

        expected_keys = self.__dict__.keys()
        #todo: raise error if found unexpected keys, or if keys are missing,
        #      or (ideally) if the file has duplicate keys
        matched_keys = set(expected_keys).intersection(set(file_data.keys()))
        for key in matched_keys:
            if key == 'name':
                continue
            self.__dict__[key] = file_data[key]

    def LoadDataFile(self, date, locale):
        locale_type = self._DetermineLocaleType(locale)
        m_key = (date, locale_type)

        if m_key not in self.metadata:
            self.metadata[m_key] = {'last_load_time': datetime.fromtimestamp(0)}

        metrics_age = datetime.now() - self.metadata[m_key]['last_load_time']
        if metrics_age.seconds < METRICS_REFRESH_RATE:
            return
        self.metadata[m_key]['last_load_time'] = datetime.now()

        fname = os.path.join(METRICS_DIR, self.name, '%d-%02d' % date,
                             '%s.csv' % locale_type)
        try:
            print 'opening: %s' % fname
            with open(fname, 'r') as fd:
                lines = fd.readlines()
        except IOError:
            raise LoadError('Could not load metric data for "%s" from file: %s'
                            % (self.name, fname))

        file_data = [l.strip().split(',', 1) for l in lines]

        if date not in self.data:
            self.data[date] = dict()

        if locale_type == 'world':
            # world data has no 'locale', so the csv has only 1 value
            self.data[date][locale] = float(file_data[0][0])
            print 'got world data for date=%s, locale=%s : %s' % (
                date, locale, self.data[date][locale])
            return

        for locale, value in file_data:
            print "%s (%s) < %s, %s" % (self.name, date, locale, value)
            self.data[date][locale] = float(value)

    def Lookup(self, year, month, locale):
        #todo: allow regex lookups
        date = (year, month)

        if date not in self.metadata:
            self.metadata[date] = dict()
        self.metadata[date]['last_request_time'] = datetime.now()

        if date not in self.data or locale not in self.data[date]:
            self.LoadDataFile(date, locale)

        if date not in self.data or locale not in self.data[date]:
            return {'error': 'No data for metric=%s, year=%d, month=%d,'
                             ' locale=%s.' % (self.name, year, month, locale)}

        return {'metric': self.name,
                'units' : self.units,
                'value' : self.data[date][locale]}
    
    def Describe(self):
        return {'name'               : self.name,
                'short_desc'         : self.short_desc,
                'long_desc'          : self.long_desc,
                'units'              : self.units,
                'query'              : self.query}

    def _DetermineLocaleType(self, locale_str):
        if locale_str == 'world':
            return 'world'

        depth_map = {1: 'country', 2: 'region', 3: 'city'}
        depth = len(locale_str.split('_'))

        print 'locale: %s, locale_type: %s' % (locale_str, depth_map[depth])
        return depth_map[depth]


def refresh(metrics_dict):
    _update_metrics_info(metrics_dict)
    _update_metrics_data(metrics_dict)


def _update_metrics_info(metrics_dict):
    global _last_metrics_info_refresh

    metrics_age = datetime.now() - _last_metrics_info_refresh
    if metrics_age.seconds < METRICS_REFRESH_RATE:
        return

    logging.info('Updating metrics data.')
    #todo: handle exceptions
    available_metrics = set(m for m in os.listdir(METRICS_DIR)
                            if os.path.isdir(os.path.join(METRICS_DIR, m)))
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
            try:
                metrics_dict[new_metric] = Metric(new_metric)
            except LoadError as e:
                raise RefreshError(e)
    
    _last_metrics_info_refresh = datetime.now()


def _update_metrics_data(metrics_dict):
    # Do nothing here.  Metrics data will be updated on query.
    pass
