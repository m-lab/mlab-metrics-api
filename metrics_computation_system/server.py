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

"""This module runs the web server to handle HTTP requests.
todo: comments
"""

import logging

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app


class Error(Exception):
    pass


def start(handlers):
    """Start the web framework on AppEngine.

    This function never returns.
    """
    logging.getLogger().setLevel(logging.DEBUG)

    if not handlers:
        raise Error('Server startup failure: No handlers defined.')
    logging.info('Registering request handlers: %s' % handlers)

    application = webapp.WSGIApplication(handlers, debug=True)
    run_wsgi_app(application)
