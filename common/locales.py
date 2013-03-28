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


class LocalesManager(object):
    """Manage locale data, specifically hiding the details of data caching.
    """
    def __init__(self, backend):
        """Constructor.

        Args:
            backend (Backend object): Datastore backend.
        """
        self.disable_refresh = False
        self._backend = backend
        self._locales = None
        self._locales_by_type = None
        self._last_refresh = datetime.fromtimestamp(0)

    def Exists(self, locale):
        """Whether or not a given locale exists.

        Args:
            locale (string): Locale ID.

        Raises:
            RefreshError: An error occurred while refreshing the locale cache.

        Returns:
            (bool) True if the locale exists and can be queried, otherwise false.
        """
        self._Refresh()
        return locale in self._locales

    def Locale(self, locale):
        """Retrieves the given locale.

        Args:
            locale (string): Locale ID.

        Raises:
            KeyError: The locale doesn't exist.
            RefreshError: An error occurred while refreshing the locale cache.

        Returns:
            (Locale) The locale object.
        """
        if not self.Exists(locale):
            raise KeyError('Unknown locale: %s' % locale)

        return self._locales[locale]

    def LocalesByType(self, locale_type):
        """Retrieves all locale IDs for the specified type.

        Valid locale types are 'world', 'country', 'region', 'city'.

        Args:
            locale (string): Locale ID.

        Raises:
            KeyError: The locale type doesn't exist.
            RefreshError: An error occurred while refreshing the locale cache.

        Returns:
            (list) List of locale IDs, as strings.
        """
        self._Refresh()
        if locale_type not in self._locales_by_type:
            raise KeyError('Unknown locale type: %s' % locale_type)

        return self._locales_by_type[locale_type]

    def ForceRefresh(self):
        """Forces a refresh of the internal locale data.
        """
        self._last_refresh = datetime.fromtimestamp(0)
        self._Refresh()

    def _Refresh(self):
        """Refreshes LocalesManager data at most every 'LOCALE_REFRESH_RATE'.
        """
        if self.disable_refresh:
            return

        if datetime.now() - self._last_refresh < LOCALE_REFRESH_RATE:
            return
 
        # Start fresh, since locales may have been removed.
        locales_by_id = {}
        locales_by_type = {'world': ['world'], 'country': [], 'region': [], 'city': []}
        locales = {'world': Locale('world')}
 
        # Build Locales in largest-to-smallest order so that parent references
        # can be resolved.
        for locale_type in ('country', 'region', 'city'):
            try:
                info = self._backend.GetLocaleData(locale_type)
            except backend_interface.LoadError as e:
                logging.error('Failed to refresh locales: %s' % e)
                if self._locales is None:  # First refresh. Cannot fail silently.
                    raise RefreshError(e)
 
            # Parse and build Locales into the dict.
            for row in info['data']:
                locale_id, locale, name, parent_id, lat, lon = row
 
                locales[locale] = Locale(
                    locale, name, float(lat), float(lon), parent_id)
                locales_by_type[locale_type].append(locale)
                locales_by_id[locale_id] = locale
 
                # Add child references if the parent exists.
                if parent_id in locales_by_id:
                    locales[locales_by_id[parent]].children.append(locale)
                else:
                    locales[locale].parent = None
 
        # Update data members.
        self._locales = locales
        self._locales_by_type = locales_by_type
        self._last_refresh = datetime.now()


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
    def __init__(self, backend):
        """Constructor.
        """
        self._backend = backend
        self._countries = None
        self._regions = None
        self._cities = None
        self._last_refresh = datetime.fromtimestamp(0)

    class GeoTree(object):
        """Tree that holds geographically located data.

        GeoTree implements a KD-Tree to store a set of Lat-Lon geo locations so
        that it can then resolve queries for a nearest neighbor to a given
        Lat-Lon coordinate.

        After construction GeoTree is immutable.
        
        GeoTree works by accepting a list of 'target_locales' (IDs) and a
        'locales_manager' (LocalesManager) from which to request locale data.
        It then constructs a GeoTree by converting each locale from Lat-Lon
        coordinates into Cartesian (3-D), and building a KD-Tree.  This allows
        relatively efficient lookup for finding the nearest neighbor to a given
        Lat-Lon coordinate.
        """
        def __init__(self, target_locales, locales_manager):
            """Constructor.

            Args:
                target_locales (list): List of locales to pull out of the passed
                    LocaleManager.
                locale_manager (LocaleManager object): Locale manager.
            """
            self._data = []  # pairs of (coordinates, locale)

            for tgt in target_locales:
                locale = locales_manager.Locale(tgt)
                cart = self._LatLonToCartesian(locale.latitude, locale.longitude)
                self._data.append((cart, tgt))

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
        self._Refresh()
        return self._countries.FindNearestNeighbor(lat, lon)

    def FindNearestRegion(self, lat, lon):
        """Finds the nearest region to given coordinates.

        Args:
            lat (float): Target latitude.
            lon (float): Target longitude.

        Returns:
            (string) Locale name for the nearest region.  For example '123_g'.
        """
        self._Refresh()
        return self._regions.FindNearestNeighbor(lat, lon)

    def FindNearestCity(self, lat, lon):
        """Finds the nearest city to given coordinates.

        Args:
            lat (float): Target latitude.
            lon (float): Target longitude.

        Returns:
            (string) Locale name for the nearest city.  For example '123_g_abc'.
        """
        self._Refresh()
        return self._cities.FindNearestNeighbor(lat, lon)

    def _Refresh(self):
        """Refreshes LocaleFinder data at most every 'LOCALE_REFRESH_RATE'.
        """
        if datetime.now() - self._last_refresh < LOCALE_REFRESH_RATE:
            return

        lm = LocalesManager(self._backend)
        lm.ForceRefresh()
        lm.disable_refresh = True  # Not necessary to refresh from here on.

        countries = self.GeoTree(lm.LocalesByType('country'), lm)
        regions = self.GeoTree(lm.LocalesByType('region'), lm)
        cities = self.GeoTree(lm.LocalesByType('city'), lm)
 
        # Update data members.
        self._countries = countries
        self._regions = regions
        self._cities = cities
        self._last_refresh = datetime.now()
