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

"""This module runs the web server to handle user & API requests.

The Metrics Definition System ...

todo: comments
"""

import logging
import os
import re

from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import OAuth2DecoratorFromClientSecrets

from common import backend as backend_interface
from common import metrics

_backend = None
_metrics_data = dict()
_client_secrets = OAuth2DecoratorFromClientSecrets(
    os.path.join(os.path.dirname(__file__), 'client_secrets.json'),
    scope='https://www.googleapis.com/auth/bigquery')


class Error(Exception):
    pass


class RefreshError(Error):
    pass


def _TemplateFile(filename):
    def wrapper(fn):
        def wrapped(self, *args):
            template_values = fn(self, *args)
            path = os.path.join(os.path.dirname(__file__), filename)
            self.response.out.write(template.render(path, template_values))
            return
        return wrapped
    return wrapper


def _RefreshMetricsData(http):
    try:
        _backend.SetClientHTTP(http)
        metrics.refresh(_backend, _metrics_data)
        _backend.SetClientHTTP(None)  #todo: finally?
    except metrics.RefreshError as e:
        raise RefreshError(e)

    if not len(_metrics_data):
        raise RefreshError('The metric database is empty.')


def start(backend):
    """Start the web framework on AppEngine.

    This function never returns.
    """
    global _backend

    _backend = backend
    application = webapp.WSGIApplication(
        [('/',        IntroPageHandler),
         ('/intro',   IntroPageHandler),
         ('/metrics', ListMetricsPageHandler),
         ('/edit',    EditMetricPageHandler),
         ('/delete',  DeleteMetricPageHandler),
         ('/new',     NewMetricPageHandler),
         ('/contact', ContactUsPageHandler)],
        debug=True)
    run_wsgi_app(application)


class IntroPageHandler(webapp.RequestHandler):
    """Handle a request for the "Introduction" page.

    This function really doesn't do much, but it returns the data contained at
    views/introduction.tpl which contains some detail on the project and useful
    links to other information.

    Returns:
        (string) A web page (via the @view decorator) an introduction to the
        project.
    """
    @_TemplateFile('views/introduction.tpl')
    def get(self):
        return {'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}


class ListMetricsPageHandler(webapp.RequestHandler):
    """Handle a request for the "List Metrics" page.

    The "List Metrics" page contains the name and short description for each
    metric, and a link where more info can be retrieved.  It's intended only
    as a quick view of the metrics.

    Returns:
        (string) A web page (via the @view decorator) listing all available
        metrics.
    """
    @_client_secrets.oauth_required
    @_TemplateFile('views/list_metrics.tpl')
    def get(self):
        view = {'metrics': [],
                'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}

        try:
            _RefreshMetricsData(_client_secrets.http())
        except RefreshError as e:
            view['error'] = '%s' % e
            return view

        # Pump the view with details for all metrics.
        for metric_name in _metrics_data:
            view['metrics'].append(_metrics_data[metric_name].Describe())
        logging.debug('ListMetrics view: %s' % view)
        return view


class EditMetricPageHandler(webapp.RequestHandler):
    """Handle a page request for the metric editor.
    """
    @_client_secrets.oauth_required
    @_TemplateFile('views/edit_metric.tpl')
    def get(self):
        """Handles "get" requests for the Edit Metric page.

        Returns:
            (string) A web page (via the @view decorator) listing details for
            the requested metric, in a form that allows details to be edited.
        """
        view = {'metric': [],
                'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}

        try:
            _RefreshMetricsData(_client_secrets.http())
        except RefreshError as e:
            view['error'] = '%s' % e
            return view

        metric_name = self.request.get('metric', default_value=None)
        if metric_name is None:
            self.redirect('/metrics')

        if metric_name not in _metrics_data:
            view['error'] = ('No such metric: <span id="metric_name">%s</span>'
                             % metric_name)
            return view

        view['metric'] = _metrics_data[metric_name].Describe()
        return view

    @_client_secrets.oauth_required
    def post(self):
        """Handles "post" requests for the Edit/New Metric page.
        
        Saves updated metric details, then redirects to the "List Metrics" page.
        """
        name = self.request.get('name', default_value=None)
        units = self.request.get('units', default_value=None)
        short_desc = self.request.get('short_desc', default_value=None)
        long_desc = self.request.get('long_desc', default_value=None)
        query = self.request.get('query', default_value=None)
        if name in _metrics_data:
            request_type = backend_interface.RequestType.EDIT
        else:
            request_type = backend_interface.RequestType.NEW

        logging.debug('POST: name="%s (%s)", units="%s", short_desc="%s", '
                      'long_desc="%s", query="%s"' %
                      (name, type(name), units, short_desc, long_desc, query))

        if None in (name, units, short_desc, long_desc, query):
            self.redirect('/metrics?error=Edit request was incomplete. Try '
                          'again or send us an email.')
            return

        try:
            _backend.SetClientHTTP(_client_secrets.http())
            metrics.edit_metric(request_type, _backend, _metrics_data, name,
                                units=units, short_desc=short_desc,
                                long_desc=long_desc, query=query)
            _backend.SetClientHTTP(None)  #todo: finally?
        except metrics.RefreshError as e:
            self.redirect('/metrics?error=%s' % e)
            return

        self.redirect('/metrics?note=Metric %s saved successfully.' % name)


class NewMetricPageHandler(webapp.RequestHandler):
    """Handle a page request for creating a metric.
    """
    @_client_secrets.oauth_required
    @_TemplateFile('views/edit_metric.tpl')
    def get(self):
        """Handles "get" requests for the New Metric page.

        Returns:
            (string) A web page (via the @view decorator) with input boxes
            for details for the metric to be created.
        """
        return {'metric': None,
                'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}


class DeleteMetricPageHandler(webapp.RequestHandler):
    """Handle a page request for metric deletion.
    """
    @_client_secrets.oauth_required
    @_TemplateFile('views/delete_metric.tpl')
    def get(self):
        """Handles "get" requests for the Delete Metric page.

        Returns:
            (string) A web page (via the @view decorator) requesting
            verification that the user wishes to delete the specified metric.
        """
        view = {'metric': self.request.get('metric', default_value=None),
                'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}

        if view['metric'] is None:
            self.redirect('/metrics')

        return view

    @_client_secrets.oauth_required
    def post(self):
        """Handles "post" requests for the Delete Metric page.
        
        Deletes the specified metric, then redirects to the "List Metrics" page.
        """
        name = self.request.get('name', default_value=None)

        try:
            _backend.SetClientHTTP(_client_secrets.http())
            metrics.edit_metric(backend_interface.RequestType.DELETE,
                                _backend, _metrics_data, name, delete=True)
            _backend.SetClientHTTP(None)  #todo: finally?
        except metrics.RefreshError as e:
            self.redirect('/metrics?error=%s' % e)
            return

        self.redirect('/metrics?note=Metric %s deleted successfully.' % name)


class ContactUsPageHandler(webapp.RequestHandler):
    """Handle a request for the "Contact Us" page.

    This function really doesn't do much, but it returns the data contained at
    views/contact.tpl which contains various contact information.

    Returns:
        (string) A web page (via the @view decorator) with contact information.
    """
    @_TemplateFile('views/contact.tpl')
    def get(self):
        return {'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}
