#!/usr/bin/python
#
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

import os
import time

INPUT_TIMESTAMP_FORMAT = '%m-%d-%Y'
OUTPUT_TIMESTAMP_FORMAT = '%Y-%m'

PDE_LOCALE_FILE = r'mlab_all_countries/%s_measurements.txt'
BIGQUERY_METRICS_FILE = r'bigquery.metrics/%s.csv'
BIGQUERY_SCHEMA_FILE = r'bigquery.metrics/SCHEMA.txt'

LOCALES = ('city', 'region', 'country', 'world')


def main():
    writers = dict()

    BigQueryMetricsWriter.WriteSchemaToFile()

    for locale in LOCALES:
        reader = PDELocaleReader(locale)

        for data in reader:
            if data['metric'] not in writers:
                writers[data['metric']] = BigQueryMetricsWriter(data['metric'])
            writers[data['metric']].Write(
                data['locale'], data['date'], data['value'])


def PDELocaleReader(locale, filename=None,
                    timestamp_fmt=INPUT_TIMESTAMP_FORMAT):
    """Reads in a PDE CSV file for the given 'locale', yielding values.

    Args:
        locale (string): This locale type, eg 'city' or 'region'.
        filename (string): The locale file to be read.  Defaults to
            (PDE_LOCALE_FILE % locale).
        timestamp_fmt (string): Expected timestamp format in the PDE CSV input.
            Defaults to (INPUT_TIMESTAMP_FORMAT).

    Yields: 
        Dict of
            { 'locale': <locale name>,
              'date': <date>,
              'metric': <metric name>,
              'value': <metric value> }
        where <date> is expressed as a standard python time.struct_time object.
    """
    if filename is None:
        filename = PDE_LOCALE_FILE % locale

    with open(filename, 'r') as fd:
        # Grab the metric names from the 1st line of the PDE CSV file.
        header = fd.readline().strip().split(',')
        metrics = tuple(h for h in header if h not in (locale, 'month'))

        # For each subsequent line: Match the metrics to the given data (not all
        # metrics will have data for every line) and yield tuples describing the
        # data as outlined in the docstring, above.
        for line in fd:
            data = dict(zip(header, line.strip().split(',')))
            data['date'] = time.strptime(data['month'], timestamp_fmt)
            if locale == 'world':
                data[locale] = 'world'

            for met in metrics:
                if data[met] == '':
                    continue
                yield {'locale': data[locale], 'date': data['date'],
                       'metric': met, 'value': float(data[met])}


class BigQueryMetricsWriter(object):
    """Object that maintains an open file handle for BigQuery metrics data and
    receives data to be written to this file (always 'append'), in CSV.  The
    file is closed on demand or on destruction.

    Sample Usage:
        writer = BigQueryMetricsWriter('my_fancy_metric')
        writer.Write('Sunnyvale, CA', time.struct_time(tm_year=2011, ...), 15.3)
        writer.Write('Sunnyvale, CA', time.struct_time(tm_year=2012, ...), 22.1)
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
                     '    {"name": "locale", "type": "string"},\n'
                     '    {"name": "date", "type": "string"},\n'
                     '    {"name": "value", "type": "float"}\n'
                     ']')

    def __init__(self, metric_name, filename=None,
                 timestamp_fmt=OUTPUT_TIMESTAMP_FORMAT):
        """Constructs a BigQueryMetricsWriter.

        Args:
            metric_name (string): Name of the metric this writer will write.
            filename (string): The metric file to be written.  Defaults to
                (BIGQUERY_METRICS_FILE % metric_name).
            timestamp_fmt (string): Requested output timestamp format.  Defaults
                to (OUTPUT_TIMESTAMP_FORMAT).
        """
        if filename is None:
            self.filename = BIGQUERY_METRICS_FILE % metric_name
        else:
            self.filename = filename
        self.timestamp_fmt = timestamp_fmt

        self._Open()

    def __del__(self):
        """Closes the metric output file and destroys this object.
        """
        self.Close()

    def Write(self, locale, date, value):
        """Writes the given data out to the metric file.

        Args:
            locale (string): Locate that this data represents.
            date (time.struct_time): The time that this data represents.
            value (float): The metric value for this data.
        """
        if self.fd is None:
            self._Open()

        self.fd.write('"%s","%s",%f\n' %
                      (locale, time.strftime(self.timestamp_fmt, date), value))

    def Close(self):
        """Closes the metric output file.

        If Write() is called subsequently, the file will be opened again and the
        given data will be written out.  Close() will need to be called again in
        this case.
        """
        self.fd.close()

    def _Open(self):
        """PRIVATE METHOD.
        
        Opens a file descriptor to the specified metric filename (self.filename)
        storing it in self.fd.
        """
        dirname = os.path.dirname(self.filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.fd = open(self.filename, 'a')


if __name__ == '__main__':
    main()
