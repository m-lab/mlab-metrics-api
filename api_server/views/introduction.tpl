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

%include header onpage='intro',error=error

<script type="text/javascript" src="https://www.google.com/jsapi"></script>

<div id="content">

<h2 class="sectionhead">Introduction</h2>

<p>
Measurement Lab (M-Lab) is a project for measuring broadband Internet connectivity with open-source tools and data transparency.  Data collected by M-Lab is hosted in Google Storage and Amazon S3 and made available to the public.  However, <span class="standout">the total repository is over 500 Terabytes</span> which makes exploration somewhat cumbersome.
<p>

</p>
<a class="btn btn-small btn-primary" href="http://measurementlab.net">Learn more about Measurement Lab.</a>
</p>

<p>
The M-Lab Metrics API Server hosts a collection of metrics derived from the M-Lab repository, such as maximum download speed.  These high-level metrics are stored in-memory and made accessible to you via a RESTful JSON API.
</p>

<p class="standout">Now with quick, easy calls you can explore the M-Lab data at a high level, build visualizations, and track trends.
</p>

<h2 class="sectionhead">For Example</h2>
<p>
Suppose you're want to know <strong>the maximum upload throughput in Paris in January, 2012</strong>.  This can be queried using <a class="btn btn-mini" href="/api/metric/upload_throughput_max?year=2012&month=1&locale=250_j_paris">a simple GET statement</a> and returns <code>0.61 MBytes</code>.  <strong>The previous year?</strong> <code>0.52 MBytes</code>  <strong>And the year before that?</strong> <code>0.51 MBytes</code>
<p>

</p>
<a class="btn btn-small btn-primary" href="/examples">See more examples.</a>
</p>

<h2 class="sectionhead">Where Do I Start?</h2>
<p>
Jump right into the <a href="/getting_started">Getting Started Guide</a> for an in-depth tour of the API Server.  Take a look at the current metrics on the <a href="/metrics">Metrics List Page</a>.  Or get in touch via the mailing list on the <a href="/contact">Contact Us Page</a>.
</p>

<h2 class="sectionhead">Get Involved</h2>
<p>
So you want to get involved?  <strong>Great!</strong>
</p>

<p>
This project is entirely open-source.  If you want to check out what we've done or contribute some programming time, visit our Google Code page at:
</p>
<p class="lead">
<a href="http://code.google.com/p/mlab-metrics-api-server/">http://code.google.com/p/mlab-metrics-api-server/</a>
</p>

<p>
For everything else, head on over to the <a href="/contact">Contact Us Page</a>.
</p>

</div>

%include footer
