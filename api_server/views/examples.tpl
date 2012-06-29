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

%include header onpage='examples',error=error

<p>
See the <a href="/getting_started">Getting Started Page</a> for details on how
these API calls work.
</p>

<h2 class="sectionhead">Example "metric" API Call</h2>
<p>
    Lookup the minimum round-trip-time (<code>rtt_min</code>) recorded for
    London, England in November, 2011.
    <pre>/query?query=metric&name=rtt_min&year=2011&month=7&locale=826_eng_london <a class="btn btn-mini btn-primary" href="/query?query=metric&name=rtt_min&year=2011&month=7&locale=826_eng_london">try it out</a></pre>
    Returns:
    <pre>
{
    "metric": "rtt_min",
    "units": "msec",
    "value": 31.0
}
</pre>
</p>

<h2 class="sectionhead">Example "nearest" API Call</h2>
<p>
    Lookup the nearest city, region, and country to latitude 51.5171&deg; N and
    longitude 0.1062&deg; W for which there is Measurement Lab metrics data.
    <pre>/query?query=nearest&lat=51.5171&lon=-0.1062 <a class="btn btn-mini btn-primary" href="/query?query=nearest&lat=51.5171&lon=-0.1062">try it out</a></pre>
    Returns:
    <pre>
{
    "city": "826_eng_london"
    "region": "826_eng",
    "country": "832",
}
</pre>
</p>

<h2 class="sectionhead">Example "locale" API Call</h2>
<p>
    Lookup the locale details for Carabobo, Venezuela (<code>862_g</code>).
    <pre>/query?query=locale&locale=862_g <a class="btn btn-mini btn-primary" href="/query?query=locale&locale=862_g">try it out</a></pre>
    Returns:
    <pre>
{
    "locale":
    {
        "name": "862_g",
        "long_name": "Carabobo",
        "latitude": 10.31854,
        "longitude": -68.080330000000004
    },
    "parent":
    {
        "name": "862",
        "long_name": "Venezuela",
        "latitude": 6.4237500000000001,
        "longitude": -66.589730000000003
    },
    "children":
    [
        {
            "name": "862_g_guacara",
            "long_name": "Guacara",
            "latitude": 10.25381,
            "longitude": -67.900413999999998
        },
        {
            "name": "862_g_puerto-cabello",
            "long_name": "Puerto-cabello",
            "latitude": 10.46828,
            "longitude": -68.011641999999995
        },
        {
            "name": "862_g_valencia",
            "long_name": "Valencia",
            "latitude": 10.17822,
            "longitude": -68.003112999999999
        }
    ]
}
</pre>
</p>

%include footer
