{% comment %}
Copyright 2013 Google Inc. All Rights Reserved.

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

<!DOCTYPE html>
<html>
<head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <link rel="stylesheet" type="text/css" href="/static/bootstrap.css">
    <link rel="stylesheet" type="text/css" href="/static/common.css">
    <link rel="stylesheet" type="text/css" href="/static/metdef.css">
</head>

<body>
<div class="container">
    <div class="jumbotron subhead" id="titlebar">
        <h1>M-Lab Metrics Definition System</h1>
        <p class="lead">
            A simple server for managing Measurement Lab metrics.
        </p>
    </div>

    {% if error|default_if_none:"" != "" %}
    <div class="alert alert-error"><strong>Error:</strong> {{error}}</div>
    {% endif %}
    {% if note|default_if_none:"" != "" %}
    <div class="alert alert-success">{{note}}</div>
    {% endif %}

    <div class="row-fluid">
        <div class="span2 well" id="nav">
            <!-- sidebar -->
            <ul class="nav nav-list">
                <li
                    {% if onpage == "intro" %}
                    class="active"
                    {% endif %}
                >
                    <a href="/intro">Introduction</a></li>
                <li
                    {% if onpage == "metrics" %}
                    class="active"
                    {% endif %}
                >
                    <a href="/metrics">Metrics List</a></li>
                <li
                    {% if onpage == "contact" %}
                    class="active"
                    {% endif %}
                >
                    <a href="/contact">Contact Us</a></li>
            </ul>
        </div>

        <div class="span10">
