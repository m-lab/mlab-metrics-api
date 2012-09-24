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

"""This module runs the Bottle web server to handle user & API requests.

The API Server manages the interface between UI templates (web pages) and the
backend (API data).  It also handles all JSON requests for API data, thus giving
the project name of "API Server".

The API Server is designed to run on Google AppEngine.
"""

import json
import os
import re

from deps import bottle
from deps.bottle import request
from deps.bottle import route
from deps.bottle import view
from google.appengine.ext.webapp.util import run_wsgi_app

import locales
import metrics
import query_engine

_bigquery = None
_locale_finder = locales.LocaleFinder()
_locales_data = dict()
_metrics_data = dict()


def start(bigquery):
    """Start the bottle web framework on AppEngine.

    This function never returns.
    """
    global _bigquery

    _bigquery = bigquery
    run_wsgi_app(bottle.default_app())


@route('/api/locale/<locale_name>')
def locale_api_query(locale_name):
    """Handle a locale API query and send a response in JSON.

    This function will return a dict which is then JSONified by Bottle. If the
    requested locale does not exist, a JSON error is returned.  Otherwise
    details are returned for this locale, for its direct parent, and for all of
    its direct children.

    Returns:
        (string) JSON describing either the requested locale or any lookup
        errors.
    """
    try:
        locales.refresh(_bigquery, _locales_data, _locale_finder)
    except locales.RefreshError as e:
        return {'error': '%s' % e}

    try:
        return query_engine.HandleLocaleQuery(_locales_data, locale_name)
    except query_engine.Error as e:
        return {'error': '%s' % e}


@route('/api/metric/<metric_name>')
def metric_api_query(metric_name):
    """Handle a metric API query and send a response in JSON.

    Expects GET params "locale", "year", and "month" to narrow the metric
    request.  For example one should ask for a specific metric for Tokyo on July
    2011.

    This function will return a dict which is then JSONified by Bottle. If the
    requested metric does not exist or if any expected GET parameters are not
    specified, a JSON error is returned.  Otherwise metric details are returned
    for the specific query.

    Returns:
        (string) JSON describing either the requested metric or any lookup
        errors.
    """
    #todo: allow 1 out of 3 GET params to be empty for broader queries
    year = request.GET.get('year', None)
    month = request.GET.get('month', None)
    locale = request.GET.get('locale', None)

    try:
        metrics.refresh(_bigquery, _metrics_data)
    except metrics.RefreshError as e:
        return {'error': '%s' % e}

    try:
        return query_engine.HandleMetricQuery(
            _metrics_data, metric_name, locale, int(year), int(month))
    except (query_engine.Error, ValueError) as e:
        return {'error': '%s' % e}


@route('/api/nearest')
def nearest_api_query():
    """Handle a nearest-neighbor API query and send a response in JSON.

    Expects GET params "lat" and "lon", the latitude and longitude coordinates
    that will be used for a nearest neighbor lookup.  If the lookup succeeds the
    user will receive the nearest country, region, and city.

    This function will return a dict which is then JSONified by Bottle. If there
    is a problem looking up nearest locales or if any expected GET parameters
    are not specified, a JSON error is returned.  Otherwise nearest locales are
    returned for the specified latitude and longitude coordinates.

    Returns:
        (string) JSON describing either the locales nearest to the specified
        coordinates, or any error while attempting lookup.
    """
    lat = request.GET.get('lat', None)
    lon = request.GET.get('lon', None)

    try:
        locales.refresh(_bigquery, _locales_data, _locale_finder)
    except locales.RefreshError as e:
        return {'error': '%s' % e}

    try:
        return query_engine.HandleNearestNeighborQuery(
            _locale_finder, float(lat), float(lon))
    except (query_engine.Error, ValueError) as e:
        return {'error': '%s' % e}


@route('/details')
@route('/details/<metric_name>')
@view('details')
def metric_details(metric_name=None):
    """Handle a page request for metric details.

    Args:
        metric_name (string): The metric to retrieve details about.

    Returns:
        (dict) If called directly, a dict with the details for the given metric.
        (string) If called through bottle at the specified @route, a web page
        (via the @view decorator) with details for the given metric.
    """
    view = {'metric': None, 'error': None}

    if metric_name is None:
        try:
            metric_name = request.GET['metric_name']
        except KeyError:
            #todo: redirect to the list page
            return view

    try:
        metrics.refresh(_bigquery, _metrics_data)
    except metrics.RefreshError as e:
        view['error'] = '%s' % e
        return view

    if metric_name not in _metrics_data:
        view['error'] = ('No such metric: <span id="metric_name">%s</span>'
                         % metric_name)
        return view

    # Generate sample API query and its response.
    sample_locale = '826_eng_london'
    sample_year = 2012
    sample_month = 1
    try:
        sample_api_response = query_engine.HandleMetricQuery(
            _metrics_data, metric_name, sample_locale, sample_year, sample_month)
    except query_engine.Error as e:
        sample_api_response = {'error': '%s' % e}

    view['metric'] = _metrics_data[metric_name].Describe()
    view['sample_api_query'] = (
        '/api/metric/%s?year=%d&month=%d&locale=%s' %
        (metric_name, sample_year, sample_month, sample_locale))
    view['sample_api_response'] = json.dumps(sample_api_response)

    return view


@route('/list')
@route('/metrics')
@view('list_metrics')
def list_metrics():
    """Handle a request for the "List Metrics" page.

    The "List Metrics" page contains the name and short description for each
    metric, and a link where more info can be retrieved.  It's intended only
    as a quick view of the metrics.

    Returns:
        (string) A web page (via the @view decorator) listing all available
        metrics.
    """
    view = {'metrics': [], 'error': None}

    try:
        metrics.refresh(_bigquery, _metrics_data)
    except metrics.RefreshError as e:
        view['error'] = '%s' % e
        return view

    if not len(_metrics_data):
        view['error'] = 'The metric database is empty.'
        return view

    for metric_name, metric in sorted(_metrics_data.iteritems()):
        view['metrics'].append({'name': metric_name,
                                'short_desc': metric.short_desc})

    return view


@route('/contact')
@view('contact')
def contact():
    """Handle a request for the "Contact Us" page.

    This function really doesn't do much, but it returns the data contained at
    views/contact.tpl which contains various contact information.

    Returns:
        (string) A web page (via the @view decorator) with contact information.
    """
    return {'error': None}


@route('/examples')
@view('examples')
def examples():
    """Handle a request for the "Examples" page.

    This function really doesn't do much, but it returns the data contained at
    views/examples.tpl which contains various example queries.

    Returns:
        (string) A web page (via the @view decorator) with example queries.
    """
    return {'error': None}


@route('/getting_started')
@view('getting_started')
def getting_started():
    """Handle a request for the "Getting Started" page.

    This function really doesn't do much, but it returns the data contained at
    views/getting_started.tpl which contains help on getting started with the
    API Server.

    Returns:
        (string) A web page (via the @view decorator) with help for getting
        started.
    """
    return {'error': None}


@route('/')
@route('/intro')
@view('introduction')
def introduction():
    """Handle a request for the "Introduction" page.

    This function really doesn't do much, but it returns the data contained at
    views/introduction.tpl which contains some detail on the project and useful
    links to other information.

    Returns:
        (string) A web page (via the @view decorator) an introduction to the
        project.
    """
    return {'error': None}
