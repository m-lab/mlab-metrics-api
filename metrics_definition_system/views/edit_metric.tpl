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
<h2 class="sectionhead">Edit Metric: {{ metric.name }}</h2>

<form class="form-horizontal" id="edit-metric-form" method="post" action="/edit" novalidate="novalidate">
    <fieldset>
        <div class="control-group">
            <label class="control-label" for="name">Metric name</label>
            <div class="controls">
                <input class="input-xlarge uneditable-input" id="name" name="name" type="text" value="{{ metric.name }}" />
            </div>
        </div>

        <div class="control-group">
            <label class="control-label" for="units">Units</label>
            <div class="controls">
                <input class="input-xlarge focused" id="units" name="units" type="text" value="{{ metric.units }}" />
            </div>
        </div>

        <div class="control-group">
            <label class="control-label" for="short_desc">Short description</label>
            <div class="controls">
                <input class="input-xlarge" id="short_desc" name="short_desc" type="text" value="{{ metric.short_desc }}" />
            </div>
        </div>

        <div class="control-group">
            <label class="control-label" for="long_desc">Long description</label>
            <div class="controls">
                <textarea class="input-xlarge" id="long_desc" name="long_desc" type="text" rows="4">{{ metric.long_desc }}</textarea>
            </div>
        </div>

        <div class="control-group">
            <label class="control-label" for="query">Query</label>
            <div class="controls">
                <textarea class="input-xlarge" id="query" name="query" type="text" rows="8">{{ metric.query }}</textarea>
            </div>
        </div>

        <div class="form-actions">
            <button class="btn btn-primary" type="submit">Save changes</button>
            <a class="btn" href="/metrics">Cancel</a>
        </div>
    </fieldset>
</form>

</div>

{% include "footer.tpl" %}
