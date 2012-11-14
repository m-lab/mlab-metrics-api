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


class Error(Exception):
    pass

class DeleteError(Error):
    pass
class EditError(Error):
    pass
class LoadError(Error):
    pass


class RequestType:
    EDIT = 'edit'
    DELETE = 'delete'
    NEW = 'new'


class Backend(object):
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

