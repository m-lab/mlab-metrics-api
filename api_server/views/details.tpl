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

%include header onpage='metrics',error=error

<h2 class="sectionhead">Metric Details</h2>

<table class="table table-bordered">
	<tr><th class="lbl">Name</th>
		<td class="info">
			{{metric['name']}}
			</td>
		</tr>
	<tr><th class="lbl">Units</th>
		<td class="info">
			{{metric['units']}}
			</td>
		</tr>
	<tr><th class="lbl">Short Description</th>
		<td class="info">
			{{metric['short_desc']}}
			</td>
		</tr>
	<tr><th class="lbl">Long Description</th>
		<td class="info">
			{{metric['long_desc']}}
			</td>
		</tr>
	<tr><th class="lbl">Query</th>
		<td class="info">
			{{metric['query']}}
			</td>
		</tr>
</table>

Sample API Query:
<pre>{{sample_api_query}} <a class="btn btn-mini btn-primary" href="{{sample_api_query}}">try it out</a></pre>

Sample API Response:
<pre>{{sample_api_response}}</pre>

<a class="btn btn-large btn-primary" href="/metrics">Back to Metrics List</a>

%include footer
