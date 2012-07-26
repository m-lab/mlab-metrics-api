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
from deps.kdtree import KDTree

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
    """Catalogues locale data for efficient lookup of nearest neighbors.

    Note that this class expects locale input as a dict mapping locale names
    to Locale objects.  Each Locale object should have public members 'latitude'
    and 'longitude'.  (See the example 'Locale' class defined in this file.)  It
    is expected that locale data is provided as countries, regions, and cities,
    though this is not strictly necessary.

    Once the locale data has been catalogued, this class supports efficient
    lookup of nearest locales neighboring a given set of latitude and logitude
    coordinates.
    """
    def __init__(self):
        """Constructor."""
        self._countries = None
        self._regions = None
        self._cities = None

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
            """Constructor.

            Args:
                locales (list): List of locales to pull out of the passed dict
                    of locale data.
                locale_data (dict): Collection of locale data, keyed on locale
                    name.
            """
            self._data = []  # pairs of (coordinates, locale)

            for locale in locales:
                cart = self._LatLonToCartesian(locale_data[locale].latitude,
                                               locale_data[locale].longitude)
                self._data.append((cart, locale))

            self._ReportCollisions()
            self._tree = KDTree(3, self._data)

        def FindNearestNeighbor(self, lat, lon):
            """Finds the nearest neighbor to a given latitude & longitude.

            Args:
                lat (float): Target latitude.
                lon (float): Target longitude.

            Returns:
                (string) The name of the locale located closest to the given
                latitude & longitude.
            """
            cart = self._LatLonToCartesian(lat, lon)
            _, locale, _ = self._tree.nearest_neighbor(cart)
            return locale

        def _LatLonToCartesian(self, lat, lon):
            """Translates latitude & longitude to cartesian coordinates.

            x = r sin(lat) cos(lon)
            y = r sin(lat) sin(lon)
            z = r cos(lat)
            
            To optimize so that we store in 4 byte integers, let r = 2**31 - 1.
            The more bytes we use the higher precision we get, but we want to
            convert to ints to improve the speed of lookup comparisons.

            Args:
                lat (float): Latitude, where North is positive.
                lon (float): Longitude, where East is positive.

            Returns:
                (tuple) 3-tuple representing x, y, z cartesian coordinates.
            """
            lat = math.radians(lat)
            lon = math.radians(lon)

            r = (2 ** 31) - 1
            x = r * math.sin(lat) * math.cos(lon)
            y = r * math.sin(lat) * math.sin(lon)
            z = r * math.cos(lat)

            return (int(x), int(y), int(z))

        def _ReportCollisions(self):
            """Logs collisions between locales.

            GeoTree maps lat,lon geo-coordinates to x,y,z cartesian coordinates,
            and stores the data by the cartesian coordinates for lookup.  For
            efficiency, GeoTree restricts the cartesian coordinates to an (int).
            This means that there could be two cities that collide at the same
            coordinates.

            If collisions occur, one can increase the radius term 'r' in
            _LatLonToCartesian(), which will increase precision.
            """
            collection = dict()
            for geo in self._data:
                if geo[0] in collection:
                    logging.warning(
                        'GeoTree collision between "%s" and "%s".  Consider'
                        'increasing the "r" term in _LatLonToCartesian().'
                        % (collection[geo[0]], geo[1]))
                else:
                    collection[geo[0]] = geo[1]

    def FindNearestNeighbors(self, lat, lon):
        """Finds the nearest city, region, and country to given coordinates.

        Args:
            lat (float): Target latitude.
            lon (float): Target longitude.

        Returns:
            (dict) Locale names for the nearest city, region, and country.  For
            example {'country': '123', 'region': '123_g', 'city': '123_g_abc'}.
        """
        return {'country': self.FindNearestCountry(lat, lon),
                'region': self.FindNearestRegion(lat, lon),
                'city': self.FindNearestCity(lat, lon)}

    def FindNearestCountry(self, lat, lon):
        """Finds the nearest country to given coordinates.

        Args:
            lat (float): Target latitude.
            lon (float): Target longitude.

        Returns:
            (string) Locale name for the nearest country.  For example '123'.
        """
        return self._countries.FindNearestNeighbor(lat, lon)

    def FindNearestRegion(self, lat, lon):
        """Finds the nearest region to given coordinates.

        Args:
            lat (float): Target latitude.
            lon (float): Target longitude.

        Returns:
            (string) Locale name for the nearest region.  For example '123_g'.
        """
        return self._regions.FindNearestNeighbor(lat, lon)

    def FindNearestCity(self, lat, lon):
        """Finds the nearest city to given coordinates.

        Args:
            lat (float): Target latitude.
            lon (float): Target longitude.

        Returns:
            (string) Locale name for the nearest city.  For example '123_g_abc'.
        """
        return self._cities.FindNearestNeighbor(lat, lon)

    def UpdateCountries(self, countries, locale_data):
        """Updates the list of known countries.

        Args:
            locales (list): Countries to pull out of the passed locale data.
            locale_data (dict): Collection of locale data, keyed on locale name.
        """
        self._countries = self.GeoTree(countries, locale_data)

    def UpdateRegions(self, regions, locale_data):
        """Updates the list of known regions.

        Args:
            locales (list): Regions to pull out of the passed locale data.
            locale_data (dict): Collection of locale data, keyed on locale name.
        """
        self._regions = self.GeoTree(regions, locale_data)

    def UpdateCities(self, cities, locale_data):
        """Updates the list of known cities.

        Args:
            locales (list): Cities to pull out of the passed locale data.
            locale_data (dict): Collection of locale data, keyed on locale name.
        """
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
