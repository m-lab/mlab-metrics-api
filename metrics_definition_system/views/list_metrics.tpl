{% comment %}
Copyright 2012 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Dylan Curley
{% endcomment %}

{% with "metrics" as onpage %}
{% include "header.tpl" %}
{% endwith %}

<div id="content">

<div class="new-metric-button">
    <a class="btn btn-large btn-primary" href="/new_metric">Create a New Metric</a>
</div>

{% for metric in metrics %}
<div class="metric-details">
    <h3 class="metric-name">{{ metric.name }}</h3>
    <div class="edit-delete-metric-buttons">
        <a class="btn btn-mini btn-primary" href="/edit?metric={{ metric.name }}">Edit Metric</a>
        <a class="btn btn-mini btn-danger" href="/delete?metric={{ metric.name }}">Delete Metric</a>
    </div>
</div>
<table class="table table-bordered">
	<tr><th class="lbl">Units</th>
		<td class="info">
			{{ metric.units }}
			</td>
		</tr>
	<tr><th class="lbl">Short Description</th>
		<td class="info">
			{{ metric.short_desc }}
			</td>
		</tr>
	<tr><th class="lbl">Long Description</th>
		<td class="info">
			{{ metric.long_desc }}
			</td>
		</tr>
	<tr><th class="lbl">Query</th>
		<td class="info">
			{{ metric.query }}
			</td>
		</tr>
</table>
{% endfor %}

</div>

{% include "footer.tpl" %}
