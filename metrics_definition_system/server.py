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

The Metrics Definition System ...

todo: comments
"""

import json
import os
import re

from deps import bottle
from deps.bottle import request
from deps.bottle import route
from deps.bottle import view
from google.appengine.ext.webapp.util import run_wsgi_app

import metrics

_bigquery = None

def start(bigquery):
    """Start the bottle web framework on AppEngine.

    This function never returns.
    """
    global _bigquery

    _bigquery = bigquery
    run_wsgi_app(bottle.default_app())


@route('/edit')
@route('/edit/<metric_name>')
@view('edit_metric')
def edit_metric(metric_name=None):
    """Handle a page request for the metric editor.
    
    Args:
        metric_name (string): The metric to edit.

    Returns:
        (string) A web page (via the @view decorator) listing details for the
        requested metric.
    """
    view = {'metric': [], 'error': None}

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

    view['metric'] = _metrics_data[metric_name].Describe()         
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

    # Pump the view with details for all metrics.
    for metric_name in _metrics_data:
        view['metrics'].append(_metrics_data[metric_name].Describe())
    return view


@route('/help')
@view('help')
def help():
    """Handle a request for the "Help" page.

    This function really doesn't do much, but it returns the data contained at
    views/help.tpl which contains various information on how to use the system.

    Returns:
        (string) A web page (via the @view decorator) with help information.
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
