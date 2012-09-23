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

import logging
import metrics


def HandleLocaleQuery(locales_data, locale):
    """Verifies passed arguments and issues a lookup of locale data.

    todo: details
    """
    if locale not in locales_data:
        return {'error': 'Locale "%s" does not exist.' % locale}

    reply = {'locale': locales_data[locale].Describe()}
    if locales_data[locale].parent is None:
        if locale != 'world':
            reply['parent'] = {'name': 'world'}
    else:
        reply['parent'] = locales_data[locales_data[locale].parent].Describe()

    if len(locales_data[locale].children):
        reply['children'] = [locales_data[c].Describe() for c in
                             locales_data[locale].children]
    return reply


def HandleMetricQuery(metrics_data, metric, locale, year, month):
    """Verifies passed arguments and issues a lookup of metric data.

    todo: details
    """
    # Anticipate non-standard locale= requests for world data.
    if locale in ('', '""', "''", 'world', 'global'):
        locale = 'world'

    # Validate query parameters.
    if metric is None:
        return {'error': 'Must provide a GET parameter "name" identifying the'
                         ' metric you wish to query.'}

    if metric not in metrics_data:
        return {'error': 'Unknown metric "%s".  Valid metrics are %s' % 
                         (metric, ', '.join(metrics_data))}

    if year is None or month is None:
        return {'error': 'Must provide GET parameters "year" and "month"'
                         ' identifying the date you wish to query.'}

    if locale is None:
        locale_str = ', '.join('a %s (%s)' % (k, v)
                               for k, v in VALID_LOCALE_TYPES.iteritems())
        return {'error': 'Must provide a GET parameter "locale" identifying the'
                         ' locale you wish to query.  Valid locales are %s.  '
                         'For example, "", "100", "100_az", or "100_az_tucson".'
                         % locale_str}

    # Lookup & return the data.
    try:
        data = metrics_data[metric].Lookup(int(year), int(month), locale)
    except metrics.Error as e:
        return {'error': '%s' % e}

    return data


def HandleNearestNeighborQuery(locale_finder, lat, lon):
    """Verifies passed arguments and issues a nearest neighbor lookup.

    todo: details
    """
    try:
        lat = float(lat)
        lon = float(lon)
    except ValueError:
        pass

    if type(lat) != float or type(lon) != float:
        return {'error': 'Must provide parameters "lat" for latitude and "lon" '
                         'for longitude.'}

    return locale_finder.FindNearestNeighbors(lat, lon)
