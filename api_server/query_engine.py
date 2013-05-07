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

"""This module provides interfaces to the various supported api queries.

Specifically, there are functions to manage a request for more detail on a
locale (HandleLocaleQuery), on a metric for some specific region and date
(HandleMetricQuery), or on the nearest defined locales to a set of latitude
and longitude coordinates (HandleNearestNeighborQuery).
"""

import logging

from common import metrics
from datetime import date

class Error(Exception):
    """Common exception that all other exceptions in this module inherit from.
    """
    pass

class LookupError(Error):
    """An error occurred during a lookup query.
    """
    pass
class SyntaxError(Error):
    """There was a syntax error in the request.
    """
    pass


def HandleLocaleQuery(locales_manager, locale):
    """Verifies passed arguments and issues a lookup of locale data.

    Args:
        locales_manager (LocalesManager object): Locale manager.
        locale (string): Name of the locale to be queried.

    Raises:
        LookupError: If an error occurred during lookup, e.g. the requested
        locale is unkown.

    Returns:
        (dict) Data about the requested locale.
    """
    try:
        locale = locales_manager.Locale(locale)
    except KeyError as e:
        raise LookupError(e)

    return {'locale': {'name': locale.name,
                       'long_name': locale.long_name,
                       'latitude': locale.latitude,
                       'longitude': locale.longitude
                      },
            'parent': locale.parent,
            'children': locale.children
           }


def HandleMetricQuery(metrics_manager, metric, locale, year, month):
    """Verifies passed arguments and issues a lookup of metric data.

    Args:
        metrics_manager (MetricsManager object): Metrics manager.
        metric (string): Name of the metric to be queried.
        locale (string): Locale of interest.
        year (int): Year of interest.
        month (int): Month of interest.

    Raises:
        LookupError: If an error occurred during lookup, e.g. the requested
        locale is unkown.
        SyntaxError: If expected parameters are not provided (are None).

    Returns:
        (dict) Data about the requested metric at the given year & month for
        the given locale.
    """
    # Anticipate non-standard locale= requests for world data.
    if locale in ('', '""', "''", 'world', 'global'):
        locale = 'world'

    # Validate query parameters.
    if metric is None:
        raise SyntaxError('Must provide a parameter "name" identifying the'
                          ' metric you wish to query.')

    if year is None or month is None:
        raise SyntaxError('Must provide parameters "year" and "month"'
                          ' identifying the date you wish to query.')

    if locale is None:
        raise SyntaxError('Must provide a parameter "locale" identifying the'
                          ' locale you wish to query.  For example, "", "100",'
                          ' "100_az", or "100_az_tucson".')

    # Lookup & return the data.
    try:
        logging.debug('getting ' + metric + ' for ' + str(year) + '-' +
                      str(month))
        data = metrics_manager.LookupResult(metric, year, month, locale)
    except metrics.Error as e:
        raise LookupError(e)

    return data


def HandleMultiMetricQuery(metrics_manager, metric, locale,
                           startyear, startmonth, endyear, endmonth):
    """Verifies passed arguments and issues a lookup of metric data.

    Args:
        metrics_manager (MetricsManager object): Metrics manager.
        metric (string): Name of the metric to be queried.
        locale (string): Locale of interest.
        startyear (int): Start year of interest.
        startmonth (int): Start month of interest.
        endyear (int): End year of interest.
        endmonth (int): End month of interest.

    Raises:
        LookupError: If an error occurred during lookup, eg, the requested
        locale is unknown.
        SyntaxError: If expected parameters are not provided (are None).

    Returns:
        (dict) Data about the requested metric for all years and months between
        startyear-startmonth and endyear-endmonth inclusive.
    """
    # Anticipate non-standard locale= requests for world data.
    if locale in ('', '""', "''", 'world', 'global'):
        locale = 'world'

    # Validate query parameters.
    if metric is None:
        raise SyntaxError('Must provide a parameter "name" identifying the'
                          ' metric you wish to query.')

    if startyear is None or startmonth is None:
        raise SyntaxError('Must provide parameters "startyear" and "startmonth"'
                          ' identifying the date you wish to query.')

    if endyear is None or endmonth is None:
        raise SyntaxError('Must provide parameters "endyear" and "endmonth"'
                          ' identifying the date you wish to query.')

    if endyear < startyear or endmonth < startmonth:
        raise SyntaxError('"endyear"-"endmonth" must be after'
                          ' "startyear"-"startmonth"')

    if locale is None:
        raise SyntaxError('Must provide a parameter "locale" identifying the'
                          ' locale you wish to query.  For example, "", "100",'
                          ' "100_az", or "100_az_tucson".')

    # Lookup & return the data.
    results = {}
    current_date = date(startyear, startmonth, 1)
    end_date = date(endyear, endmonth, 1)
    logging.debug('getting multiple from ' + str(current_date) + ' to ' +
                  str(end_date))
    while current_date <= end_date:
        try:
            logging.debug('getting ' + metric + ' for ' + str(current_date))
            key = str(current_date.year) + '-' + str(current_date.month)
            results[key] = metrics_manager.LookupResult(metric, current_date.year,
                                                        current_date.month, locale)
        except metrics.Error as e:
            raise LookupError(e)
        new_month = current_date.month + 1
        new_year = current_date.year
        if new_month == 13:
            new_month = 1
            new_year = new_year + 1
        current_date = date(new_year, new_month, 1)

    return results


def HandleNearestNeighborQuery(locale_finder, lat, lon):
    """Verifies passed arguments and issues a nearest neighbor lookup.

    Args:
        lat (float): Latitude of interest.
        lon (float): Longitude of interest.

    Raises:
        SyntaxError: If expected parameters are not provided (are None).

    Returns:
        (dict) The nearest city, region, and country to the provided latitude
        and longitude coordinates.
    """
    if lat is None or lon is None:
        raise SyntaxError('Must provide parameters "lat" and "lon" identifying'
                          ' the latitude and logitude of interest.')

    return locale_finder.FindNearestNeighbors(lat, lon)
