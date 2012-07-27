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

import metrics

VALID_REQUEST_TYPES = {
    'help': 'explain the supported query types',
    'locale': 'query relationships & specifications for a given locale',
    'metric': 'query metric data for a given metric & parameters',
    'nearest': 'find the nearest locale to a given latitude & longitude'}

VALID_LOCALE_TYPES = {
    'city': 'a town or city; the finest level of granularity',
    'country': 'a country',
    'region': 'a state in the USA, or equivalent for other countries',
    'world': 'the world; the broadest level of granularity'}


#todo: handle queries for nearest-locale given a set of coordinates

def handle(params, locales_data, metrics_data, localefinder):
    request_type = params.get('query', None)

    if request_type is None or request_type not in VALID_REQUEST_TYPES:
        help_str = ', '.join('"%s" (%s)' % (k, v)
                             for k, v in VALID_REQUEST_TYPES.iteritems())
        return {'error': 'Must provide a GET parameter "query" identifying the'
                         ' type of query you wish to perform.  Valid requests'
                         ' are %s.' % help_str}

    if request_type == 'locale':
        return _handle_locale_query(params, locales_data)

    if request_type == 'nearest':
        return _handle_nearest_query(params, localefinder)

    if request_type == 'metric':
        return _handle_metric_query(params, metrics_data)

    return {'error': 'Unknown query type "%s".' % request_type}


def _handle_locale_query(params, locales_data):
    locale = params.get('locale', None)

    if locale is None:
        locale_str = ', '.join('a %s (%s)' % (k, v)
                               for k, v in VALID_LOCALE_TYPES.iteritems())
        return {'error': 'Must provide a GET parameter "locale" identifying the'
                         ' locale you wish to query.  Valid locales are %s.  '
                         'For example, "", "840", "840_az", or "840_az_tucson".'
                         % locale_str}

    if locale not in locales_data:
        return {'error': 'Locale "%s" does not exist.' % locale}

    reply = {'locale': locales_data[locale].Describe()}
    if locales_data[locale].parent is None:
        if locale != 'world':
            reply['parent'] = {'name': 'world'}
    else:
        reply['parent'] = locales_data[locale].parent.Describe()
    if len(locales_data[locale].children):
        reply['children'] = [c.Describe() for c in
                             locales_data[locale].children.itervalues()]

    return reply


def _handle_metric_query(params, metrics_data):
    metric_name = params.get('name', None)
    year = params.get('year', None)
    month = params.get('month', None)
    locale = params.get('locale', None)

    # Anticipate non-standard locale= requests for world data.
    if locale in ('', '""', "''", 'world', 'global'):
        locale = 'world'

    # Validate the params.
    if metric_name is None:
        return {'error': 'Must provide a GET parameter "name" identifying the'
                         ' metric you wish to query.'}

    if metric_name not in metrics_data:
        return {'error': 'Unknown metric "%s".  Valid metrics are %s' % 
                         (metric_name, ', '.join(metrics_data))}

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
        data = metrics_data[metric_name].Lookup(int(year), int(month), locale)
    except metrics.Error as e:
        return {'error': '%s' % e}

    return data


def _handle_nearest_query(params, localefinder):
    lat = params.get('lat', None)
    lon = params.get('lon', None)

    try:
        lat = float(lat)
        lon = float(lon)
    except ValueError:
        pass

    if type(lat) != float or type(lon) != float:
        return {'error': 'Must provide parameters "lat" for latitude and "lon" '
                         'for longitude.'}

    return localefinder.FindNearestNeighbors(lat, lon)
