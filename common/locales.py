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

"""This module contains classes and functions for dealing with Locale data.
"""

from datetime import datetime
from datetime import timedelta
import logging
import math
import os

import backend as backend_interface
from deps.kdtree import KDTree

# Timeout when cached locales should be considered old.
LOCALE_REFRESH_RATE = timedelta(days=2)

_last_locale_refresh = datetime.fromtimestamp(0)


class Error(Exception):
    """Common exception that all other exceptions in this module inherit from.
    """
    pass

class RefreshError(Error):
    """An error occurred refreshing locale data.
    """
    pass


class Locale(object):
    """Simple object representing a locale.

    This object aims to present a simple representation for locales and
    guarantees, for any locale, the following members:
        name (string): Unique short/encoded name for this locale.
        long_name (string): Full name, ie 'San Bruno'.
        latitude (float): Latitude as a geographical center.
        longitude (float): Longitude as a geographical center.
        parent (string): Unique short/encoded name for the parent locale.
        children (dict): Collection of children of this locale, where the keys
            are short/encoded names and the values reference Locale objects.
    """
    def __init__(self, name, long_name=None, latitude=None, longitude=None,
                 parent=None):
        """Constructor.

        Args:
            name (string): Unique short/encoded name for this locale.
            long_name (string): Full name, ie 'San Bruno'.
            latitude (float): Latitude as a geographical center.
            longitude (float): Longitude as a geographical center.
            parent (string): Name of parent locale.
        """
        self.name = name
        self.long_name = long_name
        self.latitude = latitude
        self.longitude = longitude
        self.parent = parent
        self.children = []

    def Describe(self):
        """Describes the locale in terms that are considered useful.
        
        Returns:
            (dict) Representation of the locale as a dict.  Specifically,
            { 'name': (string) <locale ID>,
              'long_name': (string) <locale long/common name>,
              'latitude': (float) <latitude>,
              'longitude': (float) <longitude> }
        """
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
        """Constructor.
        """
        self._countries = None
        self._regions = None
        self._cities = None

    class GeoTree(object):
        """Tree that holds geographically located data.

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
            (dict) Names of the nearest locales for each locale type.
            Specifically,
            { 'country': (string) <nearest country ID>,
              'region': (string) <nearest region ID>,
              'city': (string) <nearest city ID>}.
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


def refresh(backend, locale_dict, localefinder):
    """Refreshes data in the passed locale dict and locale finder.

    Args:
        backend (Backend object): Datastore backend to refresh data from.
        locale_dict (dict): Dictionary to hold locale information.
        localefinder (LocaleFinder object): LocaleFinder to be refreshed.

    Raises:
        RefreshError: The locale data could not be refreshed.
    """
    #todo: move the "refresh" logic to its own file.
    global _last_locale_refresh

    locale_age = datetime.now() - _last_locale_refresh
    if locale_age < LOCALE_REFRESH_RATE:
        return

    locales_by_type = {'country': [], 'region': [], 'city': []}

    #todo: empty & refill locale_dict because locales may have been removed
    if 'world' not in locale_dict:
        locale_dict['world'] = Locale('world')

    # Must build Locales in largest-to-smallest order so that parent references
    # can be resolved.
    for locale_type in ('country', 'region', 'city'):
        try:
            info = backend.GetLocaleData(locale_type)
        except backend_interface.LoadError as e:
            raise RefreshError(e)

        # Parse and build Locales into the locale_dict.
        for row in info['data']:
            locale, name, parent, lat, lon = row

            locale_dict[locale] = Locale(
                locale, name, float(lat), float(lon), parent)
            locales_by_type[locale_type].append(locale)

            # Add child references if the parent exists.
            if parent in locale_dict:
                locale_dict[parent].children.append(locale)
            else:
                locale_dict[locale].parent = None

    _last_locale_refresh = datetime.now()

    # Build the LocaleFinder for quick nearest-neighbor lookup.
    localefinder.UpdateCountries(locales_by_type['country'], locale_dict)
    localefinder.UpdateRegions(locales_by_type['region'], locale_dict)
    localefinder.UpdateCities(locales_by_type['city'], locale_dict)
