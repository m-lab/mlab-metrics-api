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
import math
import os
from scipy.spatial import KDTree

LOCALE_DIR = '../maps'

# Refresh metrics at most every X seconds.
LOCALE_REFRESH_RATE = 3600

_last_locale_refresh = datetime.fromtimestamp(0)


class Error(Exception):
    pass

class RefreshError(Error):
    pass


class Locale(object):
    def __init__(self, name, long_name=None, latitude=None, longitude=None):
        self.name = name
        self.long_name = long_name
        self.latitude = latitude
        self.longitude = longitude
        self.parent = None
        self.children = dict()

    def Describe(self):
        return {'name': self.name,
                'long_name': self.long_name,
                'latitude': self.latitude,
                'longitude': self.longitude}


class LocaleFinder(object):
    class GeoTree(object):
        """
        GeoTree implements a KD-Tree to store a set of Lat-Lon geo locations so
        that it can then resolve queries for a nearest neighbor to a given
        Lat-Lon coordinate.

        After construction GeoTree is immutable.
        
        GeoTree works by accepting a list of 'locales' (keys) and a dict of 
        'locale_data'.  For convenience, the 'locales' are presumed to be a
        subset of the 'locale_data'.  It then constructs a GeoTree by converting
        each item from Lat-Lon coordinates into Cartesian (3-D), and building a
        KD-Tree.  This allows relatively efficient lookup for finding the
        nearest neighbor to a given Lat-Lon coordinate.
        """
        def __init__(self, locales, locale_data):
            self._data = []  # pairs of (coordinates, locale)

            for locale in locales:
                cart = self._LatLonToCartesian(locale_data[locale].latitude,
                                               locale_data[locale].longitude)
                self._data.append((cart, locale))

            self._ReportCollisions()
            self._tree = KDTree([d[0] for d in self._data])

        def FindNearestNeighbor(self, lat, lon):
            cart = self._LatLonToCartesian(lat, lon)
            dist, key = self._tree.query(cart)
            return self._data[key][1]

        def _LatLonToCartesian(self, lat, lon):
            """Translate latitude & longitude to cartesian coordinates.

            x = r sin(lat) cos(lon)
            y = r sin(lat) sin(lon)
            z = r cos(lat)
            
            To optimize so that we store in 4 byte integers, let r = 2**31 - 1.
            The more bytes we use the higher precision we get, but we want to
            convert to ints to improve the speed of lookup comparisons.

            Args:
                lat: Latitude, where North is positive.
                lon: Longitude, where East is positive.
            Returns:
                tuple: 3-tuple representing x, y, z cartesian coordinates.
            """
            lat = math.radians(lat)
            lon = math.radians(lon)

            r = (2 ** 31) - 1
            x = r * math.sin(lat) * math.cos(lon)
            y = r * math.sin(lat) * math.sin(lon)
            z = r * math.cos(lat)

            return (int(x), int(y), int(z))

        def _ReportCollisions(self):
            collection = dict()
            for geo in self._data:
                if geo[0] in collection:
                    logging.warning('GeoTree collision between "%s" and "%s".'
                                    % (collection[geo[0]], geo[1]))
                else:
                    collection[geo[0]] = geo[1]

    def __init__(self):
        self._countries = None
        self._regions = None
        self._cities = None

    def FindNearestNeighbors(self, lat, lon):
        return {'country': self.FindNearestCountry(lat, lon),
                'region': self.FindNearestRegion(lat, lon),
                'city': self.FindNearestCity(lat, lon)}

    def FindNearestCountry(self, lat, lon):
        return self._countries.FindNearestNeighbor(lat, lon)

    def FindNearestRegion(self, lat, lon):
        return self._regions.FindNearestNeighbor(lat, lon)

    def FindNearestCity(self, lat, lon):
        return self._cities.FindNearestNeighbor(lat, lon)

    def UpdateCountries(self, countries, locale_data):
        self._countries = self.GeoTree(countries, locale_data)

    def UpdateRegions(self, regions, locale_data):
        self._regions = self.GeoTree(regions, locale_data)

    def UpdateCities(self, cities, locale_data):
        self._cities = self.GeoTree(cities, locale_data)


def refresh(locale_dict, localefinder):
    global _last_locale_refresh

    locale_age = datetime.now() - _last_locale_refresh
    if locale_age.seconds < LOCALE_REFRESH_RATE:
        return

    locales_by_type = {'country': [], 'region': [], 'city': []}

    #todo: empty & refill locale_dict because locales may have been removed
    if 'world' not in locale_dict:
        locale_dict['world'] = Locale('world')

    for locale_type in ('country', 'region', 'city'):
        fname = os.path.join(LOCALE_DIR, '%s_map.txt' % locale_type)
        try:
            with open(fname) as fd:
                lines = fd.readlines()
        except IOError as e:
            raise RefreshError('Could not load locale info from file: %s (%s)' %
                               (fname, e))
        
        for line in lines:
            file_data = line.strip().split(',')

            if locale_type == 'country':
                name, long_name, latitude, longitude = file_data
                parent_name = 'world'
            else:
                name, long_name, parent_name, latitude, longitude = file_data

            try:
                lat = float(latitude)
                lon = float(longitude)
            except ValueError:
                logging.error('Failed to parse %s locale map data: %s'
                              % (locale_type, line))
                continue

            locale_dict[name] = Locale(name, long_name, lat, lon)
            if parent_name in locale_dict:
                locale_dict[name].parent = locale_dict[parent_name]
                locale_dict[parent_name].children[name] = locale_dict[name]

            locales_by_type[locale_type].append(name)

    _last_locale_refresh = datetime.now()

    localefinder.UpdateCountries(locales_by_type['country'], locale_dict)
    localefinder.UpdateRegions(locales_by_type['region'], locale_dict)
    localefinder.UpdateCities(locales_by_type['city'], locale_dict)
