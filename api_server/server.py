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

The API Server is designed to run on Google App Engine.
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
    _bigquery = bigquery
    run_wsgi_app(bottle.default_app())


@route('/query')
def api_query(params=None):
    """Handle an API query and send a response in JSON.

    Note that the results vary wildly depending on the query, so unfortunately I
    cannot give more detail here about what exactly is returned.

    Args:
        params (dict): Parameters of the query.  If None, parameters are taken
            from the bottle 'request.GET' variable.

    Returns:
        If called directly, a dict with the results from the query.
        If called through bottle at the specified @route, a valid JSON
        representation of the query results.
    """
    if params is None:
        params = request.GET

    try:
        metrics.refresh(_bigquery, _metrics_data)
        locales.refresh(_locales_data, _locale_finder)
    except (locales.RefreshError, metrics.RefreshError) as e:
        return {'error': '%s' % e}

    return query_engine.handle(params, _locales_data, _metrics_data,
                               _locale_finder)


@route('/details')
@route('/details/<metric_name>')
@view('details')
def metric_details(metric_name=None):
    """Handle a page request for metric details.
    
    Args:
        metric_name (string): The metric to retrieve details about.

    Returns:
        If called directly, a dict with the details for the given metric.
        If called through bottle at the specified @route, a web page (via the
        @view decorator) with details for the given metric.
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
    view['metric'] = _metrics_data[metric_name].Describe()
    view['sample_api_query'] = ('/query?query=metric&name=%s&year=2012&month=1'
                                '&locale=826_eng_london' % metric_name)
    params = view['sample_api_query'].split('?')[-1]
    params = dict(x.split('=') for x in params.split('&'))
    view['sample_api_response'] = json.dumps(api_query(params))

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
        A web page (via the @view decorator) listing all available metrics.
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
        A web page (via the @view decorator) with contact information.
    """
    return {'error': None}


@route('/examples')
@view('examples')
def examples():
    """Handle a request for the "Examples" page.

    This function really doesn't do much, but it returns the data contained at
    views/examples.tpl which contains various example queries.

    Returns:
        A web page (via the @view decorator) with example queries.
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
        A web page (via the @view decorator) with help for getting started.
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
        A web page (via the @view decorator) an introduction to the project.
    """
    return {'error': None}
