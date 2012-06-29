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

todo: Lots more text.
"""

import bottle
from bottle import request
from bottle import route
from bottle import static_file
from bottle import view
import gflags
import json
import os
import re

import locales
import metrics
import query_engine


FLAGS = gflags.FLAGS

gflags.DEFINE_bool('bottle_debug', False, 'Enable debugging mode in bottle.')
gflags.DEFINE_string('host', 'localhost', 'The host to run the api server on.')
gflags.DEFINE_integer('port', 8080, 'The port to run the api server on.')


_locales_data = dict()
_metrics_data = dict()
_localefinder = locales.LocaleFinder()


@route('/static/<filename>')
def server_static(filename):
    #todo: define static file path
    return static_file(filename, root=os.path.join(os.curdir, 'static'))


@route('/query')
def api_query(params=None):
    if params is None:
        params = request.GET

    try:
        metrics.refresh(_metrics_data)
        locales.refresh(_locales_data, _localefinder)
    except (locales.RefreshError, metrics.RefreshError) as e:
        return {'error': '%s' % e}

    return query_engine.handle(params, _locales_data, _metrics_data,
                               _localefinder)


@route('/details')
@route('/details/<metric_name>')
@view('details')
def metric_details(metric_name=None):
    view = {'metric': None, 'error': None}

    if metric_name is None:
        try:
            metric_name = request.GET['metric_name']
        except KeyError:
            #todo: redirect to the list page
            return view

    try:
        metrics.refresh(_metrics_data)
    except metrics.RefreshError, e:
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
    view = {'metrics': [], 'error': None}

    try:
        metrics.refresh(_metrics_data)
    except metrics.RefreshError, e:
        view['error'] = '%s' % e
        return view

    if not len(_metrics_data):
        view['error'] = 'The metric database is empty.'
        return view

    for metric_name, metric in sorted(_metrics_data.iteritems()):
        view['metrics'].append({'name': metric_name,
                                'short_desc': metric.short_desc})

    return view


@route('/explorer')
@view('explorer')
def explorer():
    #todo: write a drop-down utility for exploring the api data
    return {'error': None}


@route('/contact')
@view('contact')
def contact():
    return {'error': None}


@route('/examples')
@view('examples')
def examples():
    return {'error': None}


@route('/getting_started')
@view('getting_started')
def getting_started():
    return {'error': None}


@route('/')
@route('/intro')
@view('introduction')
def introduction():
    return {'error': None}


def start():
    bottle.debug(FLAGS.bottle_debug)
    bottle.run(host=FLAGS.host, port=FLAGS.port)
