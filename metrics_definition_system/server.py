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
"""

import httplib
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
_metrics_manager = None
_client_secrets = OAuth2DecoratorFromClientSecrets(
    os.path.join(os.path.dirname(__file__), 'client_secrets.json'),
    scope='https://www.googleapis.com/auth/bigquery')
_COMPUTATION_SYSTEM = 'mlab-metrics-computation.appspot.com'


def start(backend):
    """Start the web framework on AppEngine.

    This function never returns.

    Args:
        backend (Backend object): Datastore backend.
    """
    global _backend
    global _metrics_manager

    # AppEngine restarts the app for every request, but global data persists
    # across restarts so there's rarely reason to recreate it.
    if None in (_backend, _metrics_manager):
        _backend = backend
        _metrics_manager = metrics.MetricsManager(_backend)

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


class Error(Exception):
    """Common exception that all other exceptions in this module inherit from.
    """
    pass

class RefreshError(Error):
    """An error occurred while refreshing data form the datastore backend.
    """
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


class IntroPageHandler(webapp.RequestHandler):
    """Handle a request for the "Introduction" page.
    """
    @_TemplateFile('views/introduction.tpl')
    def get(self):
        """Handles "get" requests for the Introduction page.

        This function really doesn't do much, but it returns the data contained
        at views/introduction.tpl which contains some detail on the project and
        useful links to other information.

        Returns:
            (string) A web page (via the @_TemplateFile decorator) with
            introductory information.
        """
        return {'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}


class ListMetricsPageHandler(webapp.RequestHandler):
    """Handle a request for the "List Metrics" page.
    """
    @_client_secrets.oauth_required
    @_TemplateFile('views/list_metrics.tpl')
    def get(self):
        """Handles "get" requests for the List Metric page.

        The "List Metrics" page contains the name and short description for each
        metric, and a link where more info can be retrieved.  It's intended only
        as a quick view of the metrics.

        Returns:
            (string) A web page (via the @_TemplateFile decorator) listing all
            metrics.
        """
        view = {'metrics': [],
                'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}

        _backend.SetClientHTTP(_client_secrets.http())
        try:
            metric_names = _metrics_manager.MetricNames()
        except metrics.RefreshError as e:
            view['error'] = '%s' % e
            return view
 
        if not len(metric_names):
            view['error'] = 'The metric database is empty.'
            return view

        # Pump the view with details for all metrics.
        for metric_name in metric_names:
            metric = _metrics_manager.Metric(metric_name)
            view['metrics'].append({'name'      : metric.name,
                                    'short_desc': metric.short_desc,
                                    'long_desc' : metric.long_desc,
                                    'units'     : metric.units,
                                    'query'     : metric.query})
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
            (string) A web page (via the @_TemplateFile decorator) listing
            details for the requested metric, in a form that allows details to
            be edited.
        """
        view = {'metric': [],
                'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}

        metric_name = self.request.get('metric', default_value=None)
        if metric_name is None:
            self.redirect('/metrics')

        _backend.SetClientHTTP(_client_secrets.http())
        try:
            metric = _metrics_manager.Metric(metric_name)
        except metrics.LookupError:
            view['error'] = ('No such metric: <span id="metric_name">%s</span>'
                             % metric_name)
            return view
        except metrics.Error as e:
            view['error'] = '%s' % e
            return view

        view['metric'] = {'name'      : metric.name,
                          'short_desc': metric.short_desc,
                          'long_desc' : metric.long_desc,
                          'units'     : metric.units,
                          'query'     : metric.query}
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

        if None in (name, units, short_desc, long_desc, query):
            self.redirect('/metrics?error=Edit request was incomplete. Try '
                          'again or send us an email.')
            return

        _backend.SetClientHTTP(_client_secrets.http())
        try:
            _metrics_manager.SetMetric(name, units, short_desc, long_desc, query)
        except metrics.Error as e:
            self.redirect('/metrics?error=%s' % e)
            return

        # Send update (recomputation) request to MetComp.
        try:
            conn = httplib.HTTPConnection(_COMPUTATION_SYSTEM, timeout=10)
            conn.request('GET', '/update?metric=%s' % name)
            res = conn.getresponse()
            logging.debug('Sent UPDATE request to MetDef: %s %s'
                          % (res.status, res.reason))
        except Exception as e:
            logging.error('Failed to send UPDATE request to MetDef: %s' % e)

        self.redirect('/metrics?note=Metric %s saved successfully.' % name)


class NewMetricPageHandler(webapp.RequestHandler):
    """Handle a page request for creating a metric.
    """
    @_client_secrets.oauth_required
    @_TemplateFile('views/edit_metric.tpl')
    def get(self):
        """Handles "get" requests for the New Metric page.

        Returns:
            (string) A web page (via the @_TemplateFile decorator) with input
            boxes for details for the metric to be created.
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
            (string) A web page (via the @_TemplateFile decorator) requesting
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

        _backend.SetClientHTTP(_client_secrets.http())
        try:
            _metrics_manager.DeleteMetric(name)
        except metrics.Error as e:
            self.redirect('/metrics?error=%s' % e)
            return

        self.redirect('/metrics?note=Metric %s deleted successfully.' % name)


class ContactUsPageHandler(webapp.RequestHandler):
    """Handle a request for the "Contact Us" page.
    """
    @_TemplateFile('views/contact.tpl')
    def get(self):
        """Handles "get" requests for the Contact Us page.

        This function really doesn't do much, but it returns the data contained
        at views/contact.tpl which contains various contact information.

        Returns:
            (string) A web page (via the @_TemplateFile decorator) with contact
            information.
        """
        return {'note': self.request.get('note', default_value=None),
                'error': self.request.get('error', default_value=None)}
