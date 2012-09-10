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

{% with "intro" as onpage %}
{% include "header.tpl" %}
{% endwith %}

<div id="content">

<h2 class="sectionhead">Introduction</h2>

<p>
Measurement Lab (M-Lab) is a project for measuring broadband Internet connectivity with open-source tools and data transparency.  Data collected by M-Lab is hosted in Google Storage and Amazon S3 and made available to the public.  However, <span class="standout">the total repository is over 500 Terabytes</span> which makes exploration somewhat cumbersome.
<p>

</p>
<a class="btn btn-small btn-primary" href="http://measurementlab.net">Learn more about Measurement Lab.</a>
</p>

<p>
The M-Lab Metrics Definition System provides a user interface to the metrics that are exposed via the Metrics API Server.  Using this website one can, with the proper cedentials, create, modify, and delete M-Lab metrics.  Changes made here will be reflected in the Metrics API Server after 48 hours.
</p>

<h2 class="sectionhead">Where Do I Start?</h2>
<p>
The <a href="/metrics">Metrics List Page</a> lists all currently defined metrics.  From there you can create, modify, and delete metrics.  Or get in touch via the mailing list on the <a href="/contact">Contact Us</a>.
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

{% include "footer.tpl" %}
