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

"""This module defines an abstract interface for datastore backends.

Datastore backends should guarantee implementation of the methods detailed in
the Backend class, below. This allows datastore backend users a reliable
interface and an easy path for migrating between backends.

Similarly, datastore backends should raise exceptions defined here, instead of
raising exceptions defined in their own modules. This allows users to catch
these exceptions without concern when migrating between backends, and allows for
a common point of reference for exceptions.
"""


class Error(Exception):
    """Common exception that all other exceptions in this module inherit from.
    """
    pass

class DeleteError(Error):
    """An error occurred while deleting data from the datastore.
    """
    pass
class EditError(Error):
    """An error occurred while editing data in the datastore.
    """
    pass
class LoadError(Error):
    """An error occurred loading data from the datastore.
    """
    pass
class QueryError(Error):
    """An error occurred while querying the datastore.
    """
    pass


class RequestType:
    """Requests to the datastore backend can be referenced by these constants.
    """
    EDIT = 'edit'
    DELETE = 'delete'
    NEW = 'new'


class Backend(object):
    """An abstract interface that all datastore backends must honor.

    Refer to the specific datastore backend implementation for details of how
    each of these methods should be treated. Results may vary.
    """
    def SetClientHTTP(self, http):
        pass
    def ExistingDates(self):
        pass

    def DeleteMetricInfo(self, metric_name):
        pass
    def GetMetricInfo(self, metric_name=None):
        pass
    def SetMetricInfo(self, request_type, metric_name, metric_info):
        pass

    def CreateMetricDataTable(self, metric_name):
        pass

    def DeleteMetricData(self, metric_name, date=None):
        pass
    def GetMetricData(self, metric_name, date, locale):
        pass
    def SetMetricData(self, metric_name, date, locale, value):
        pass

    def GetLocaleData(self, locale_type):
        pass

