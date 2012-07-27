#!/usr/bin/env python
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

"""This module starts the M-Lab Metrics API Server on AppEngine."""

import logging

import server

import big_query_client

BIGQUERY_PROJECT_ID = 'measurement-lab'
BIGQUERY_DATASET = 'metrics_api_server'


def main():
    """Run the world.

    This function sets up logging, connects to BigQuery, and starts the API
    Server.  It never returns.
    """
    logging.getLogger().setLevel(logging.DEBUG)
    bigquery = big_query_client.BigQueryClient(
        BIGQUERY_PROJECT_ID, BIGQUERY_DATASET)
    server.start(bigquery)


if __name__ == '__main__':
    main()
