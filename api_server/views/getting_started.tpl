%# Copyright 2012 Google Inc. All Rights Reserved.
%#
%# Licensed under the Apache License, Version 2.0 (the "License");
%# you may not use this file except in compliance with the License.
%# You may obtain a copy of the License at
%#
%#     http://www.apache.org/licenses/LICENSE-2.0
%#
%# Unless required by applicable law or agreed to in writing, software
%# distributed under the License is distributed on an "AS IS" BASIS,
%# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%# See the License for the specific language governing permissions and
%# limitations under the License.
%#
%# Author: Dylan Curley

%include header onpage='getting_started',error=error

<div id="content">

<h2 class="sectionhead">Getting Started</h2>

<p>
This page explains the information we have on each metric, and the different API
calls to retrieve data from the server.  You can click on examples to see them
in action.
</p>

<h2 class="sectionhead">Explore the Metrics List</h2>

<p>
The <a href="/metrics">Metrics List Page</a> displays all the metrics currently
supported by the API Server.  You should go there first to explore the defined
metrics and view metric details, including:
</p>

<dl>
    <dt>Metric Name</dt>
    <dd>The system-name for a given metric.  This is the name used when querying
        the API Server (e.g. <code>download_throughput_max</code>).</dd>

    <dt>Units</dt>
    <dd>The units for a metric (e.g. <code>msec</code>).</dd>

    <dt>Short Description</dt>
    <dd>A short text description of what the metric measures.</dd>

    <dt>Long Description</dt>
    <dd>A long description containing details about how the metric was derived,
        why it's interesting, and suggested use cases.</dd>

    <dt>Query</dt>
    <dd>The internal query that produces this metric.</dd>

    <dt>Sample API Query</dt>
    <dd>A sample query to the API Server to retrieve data from this metric.</dd>

    <dt>Sample API Response</dt>
    <dd>A response (what should be expected) that the API Server would give for
        the sample query.</dd>
</dl>

<h2 class="sectionhead">Query the Metrics API</h2>

<p>
Example: <code>/api/metric/rtt_min?year=2011&month=7&locale=826_eng_london</code>
<a class="btn btn-mini btn-primary" href="/api/metric/rtt_min?year=2011&month=7&locale=826_eng_london">try it out</a>
</p>

<p>
The Metrics API query retrieves targeted metrics data from the API Server.  To
make a successful call, you'll need the following parameters:
</p>

<p><em>All parameters are required.</em></p>

<dl>
    <dt>year</dt>
    <dd>The year to be retrieved (e.g. <code>year=2011</code>).  The Metrics API
        currently has data for 2010, 2011, and 2012.</dd>

    <dt>month</dt>
    <dd>The month to be retrieved (e.g. <code>month=7</code>).  Valid months are
        1 through 12, though there might not be data for the current month.</dd>

    <dt>locale</dt>
    <dd>The city, locale, country, or global target (e.g.
        <code>locale=826_eng_london</code>).  The locale name can be determined
        using the Nearest Neighbor API and the Locale Info API, below.  To get
        global metrics use <code>locale=world</code>.</dd>
</dl>

<h2 class="sectionhead">Calling the Nearest Neighbor API</h2>

<p>
Example: <code>/api/nearest?lat=51.5171&lon=-0.1062</code>
<a class="btn btn-mini btn-primary" href="/api/nearest?lat=51.5171&lon=-0.1062">try it out</a>
</p>

<p>
The Nearest Neighbor API query retrieves the nearest city, locale, and country
to a given latitude and longitude.  You'll need the following parameters:
</p>

<p><em>All parameters are required.</em></p>

<dl>
    <dt>lat</dt>
    <dd>The latitude for the target location to identify (e.g.
        <code>lat=51.5171</code>).  Note that South latitudes are negative.</dd>

    <dt>lon</dt>
    <dd>The longitude for the target location to identify (e.g.
        <code>lon=-0.1062</code>).  Note that West longitudes are negative.</dd>
</dl>

<h2 class="sectionhead">Calling the Locale Info API</h2>

<p>
Example: <code>/api/locale/862_g</code>
<a class="btn btn-mini btn-primary" href="/api/locale/862_g">try it out</a>
</p>

<p>
The Locale Info API query retrieves detailed info for a given locale, for its
parent locale, and for all of its children locales.  There are no parameters.
</p>

%include footer
