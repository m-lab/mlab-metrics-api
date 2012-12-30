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

"""This module provides interfaces to the various supported api queries.

Specifically, there are functions to manage a request for more detail on a
locale (HandleLocaleQuery), on a metric for some specific region and date
(HandleMetricQuery), or on the nearest defined locales to a set of latitude
and longitude coordinates (HandleNearestNeighborQuery).
"""

import logging

from common import metrics


class Error(Exception):
    pass

class LookupError(Error):
    pass

class SyntaxError(Error):
    pass


def HandleLocaleQuery(locales_data, locale):
    """Verifies passed arguments and issues a lookup of locale data.

    Args:
        locales_data (dict): All data known about locales, keyed by locale name.
        locale (string): Name of the locale to be queried.

    Raises:
        LookupError: If an error occurred during lookup, e.g. the requested
        locale is unkown.

    Returns:
        (dict): Data about the requested locale.
    """
    if locale not in locales_data:
        raise LookupError('Locale "%s" does not exist.' % locale)

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

    Args:
        metrics_data (dict): All known data about metrics, keyed by metric name.
        metric (string): Name of the metric to be queried.
        locale (string): Locale of interest.
        year (int): Year of interest.
        month (int): Month of interest.

    Raises:
        LookupError: If an error occurred during lookup, e.g. the requested
        locale is unkown.
        SyntaxError: If expected parameters are not provided (are None).

    Returns:
        (dict): Data about the requested metric at the given year & month for
        the given locale.
    """
    # Anticipate non-standard locale= requests for world data.
    if locale in ('', '""', "''", 'world', 'global'):
        locale = 'world'

    # Validate query parameters.
    if metric is None:
        raise SyntaxError('Must provide a parameter "name" identifying the'
                          ' metric you wish to query.')

    if metric not in metrics_data:
        raise LookupError('Unknown metric "%s".  Valid metrics are %s' % 
                          (metric, ', '.join(metrics_data)))

    if year is None or month is None:
        raise SyntaxError('Must provide parameters "year" and "month"'
                          ' identifying the date you wish to query.')

    if locale is None:
        raise SyntaxError('Must provide a parameter "locale" identifying the'
                          ' locale you wish to query.  For example, "", "100",'
                          ' "100_az", or "100_az_tucson".')

    # Lookup & return the data.
    try:
        data = metrics_data[metric].Lookup(year, month, locale)
    except metrics.Error as e:
        raise LookupError(e)

    return data


def HandleNearestNeighborQuery(locale_finder, lat, lon):
    """Verifies passed arguments and issues a nearest neighbor lookup.

    Args:
        lat (float): Latitude of interest.
        lon (float): Longitude of interest.

    Raises:
        SyntaxError: If expected parameters are not provided (are None).

    Returns:
        (dict): The nearest city, region, and country to the provided latitude
        and longitude coordinates.
    """
    if lat is None or lon is None:
        raise SyntaxError('Must provide parameters "lat" and "lon" identifying'
                          ' the latitude and logitude of interest.')

    return locale_finder.FindNearestNeighbors(lat, lon)
