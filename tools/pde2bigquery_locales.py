#!/usr/bin/python
#
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

"""This module ...

todo: Lots more text.
"""

import os

PDE_MAP_FILE = r'mlab_all_countries/%s_map.txt'
BIGQUERY_LOCALES_FILE = r'bigquery.locales/_locales.csv'
BIGQUERY_SCHEMA_FILE = r'bigquery.locales/SCHEMA.txt'

LOCALES = ('city', 'region', 'country')


def main():
    writers = dict()

    BigQueryLocalesWriter.WriteSchemaToFile()

    for locale in LOCALES:
        reader = PDEMapReader(locale)
        writer = BigQueryLocalesWriter(locale)
        writer.WriteIter(reader)

    print ('Use "bq" to upload the locale data to BigQuery:\n'
           '  bq load metrics_api_server._locales %s %s' %
           (BIGQUERY_LOCALES_FILE, BIGQUERY_SCHEMA_FILE))


def PDEMapReader(locale, filename=None):
    """Reads in a PDE CSV file for the given 'locale', yielding values.

    Args:
        locale (string): This locale type, eg 'city' or 'region'.
        filename (string): The locale file to be read.  Defaults to
            (PDE_MAP_FILE % locale).

    Yields: 
        (dict) Details about locales.  Specifically,
        { 'locale': (string) <locale id>,
          'name': (string) <locale name>,
          'parent': (string) <parent locale id>,
          'lat': (float) <latitude>,
          'lon': (float) <longitude> }
    """
    if filename is None:
        filename = PDE_MAP_FILE % locale

    with open(filename, 'r') as fd:
        # Grab the metric names from the 1st line of the PDE CSV file.
        header = fd.readline().strip().split(',')

        # For each subsequent line: Yield tuples describing the data as
        # outlined in the docstring, above.
        for line in fd:
            data = dict(zip(header, line.strip().split(',')))
            if locale == 'country':
                data['parent'] = 'world'
            else:
                data['parent'] = data[header[2]]

            yield {'locale': data[locale], 'name': data['name'],
                   'parent': data['parent'], 'lat': float(data['latitude']),
                   'lon': float(data['longitude'])}


class BigQueryLocalesWriter(object):
    """Object that maintains an open file handle for BigQuery locale data and
    receives data to be written to this file (always 'append'), in CSV.  The
    file is closed on demand or on destruction.

    Sample Usage:
        writer = BigQueryLocalesWriter('city')  # or other locale type
        writer.Write('123', 'USA', 'world', 40.4, -98.7)
        writer.Write('124', 'Mexico', 'world', 19.0, -99.0)
        writer.Close()  # or 'del writer', or just let 'writer' go out of scope
    """

    @staticmethod
    def WriteSchemaToFile(filename=BIGQUERY_SCHEMA_FILE):
        """Writes the BigQueryMetricsWriter schema out to the given file.

        Args:
            filename (string): Name of the file that the schema should be output
                to.  Defaults to (BIGQUERY_SCHEMA_FILE).
        """
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(filename, 'w') as fd:
            fd.write('[\n'
                     '    {"name": "type", "type": "string"},\n'
                     '    {"name": "locale", "type": "string"},\n'
                     '    {"name": "name", "type": "string"},\n'
                     '    {"name": "parent", "type": "string"},\n'
                     '    {"name": "lat", "type": "float"},\n'
                     '    {"name": "lon", "type": "float"}\n'
                     ']')

    def __init__(self, locale_type, filename=None):
        """Constructs a BigQueryLocalesWriter.

        Args:
            locale_type (string): The type of locale this writer will write.
            filename (string): The locale file to be written.  Defaults to the
                global variable BIGQUERY_LOCALES_FILE.
        """
        self._type = locale_type
        if filename is None:
            self._filename = BIGQUERY_LOCALES_FILE
        else:
            self._filename = filename

        self._Open()

    def __del__(self):
        """Closes the locale output file and destroys this object.
        """
        self.Close()

    def Write(self, locale, name, parent, lat, lon):
        """Writes the given data out to the locale file.

        Args:
            locale (string): Locale ID.
            name (string): Locale (common-) name.
            parent (string): Parent locale ID.
            lat (float): Latitude for this locale.
            lon (float): Logitude for this locale.
        """
        if self._fd is None:
            self._Open()

        self._fd.write('"%s","%s","%s","%s",%f,%f\n' %
                      (self._type, locale, name, parent, lat, lon))

    def WriteIter(self, data_gen):
        """Iterates over the given data generator (or list), writing it out to
        the locale file.

        Args:
            data_gen (list of dicts): Locate that this data represents.
        """
        for data in data_gen:
            self.Write(data['locale'], data['name'], data['parent'],
                       data['lat'], data['lon'])

    def Close(self):
        """Closes the locale output file.

        If Write() is called subsequently, the file will be opened again and the
        given data will be written out.  Close() will need to be called again in
        this case.
        """
        self._fd.close()

    def _Open(self):
        """Private method.
        
        Opens a file descriptor to the specified locale filename
        (self._filename) storing it in self._fd.
        """
        dirname = os.path.dirname(self._filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self._fd = open(self._filename, 'a')


if __name__ == '__main__':
    main()
