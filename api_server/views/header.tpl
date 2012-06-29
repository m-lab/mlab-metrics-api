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

<!DOCTYPE html>
<head>
    <link rel="stylesheet" type="text/css" href="/static/bootstrap.css">
    <link rel="stylesheet" type="text/css" href="/static/api_server.css">
</head>

<body>
<div class="container">
    <div class="jumbotron subhead" id="titlebar">
        <h1>M-Lab Metrics API Server</h1>
        <p class="lead">
            Opening Measurement Lab data to external research and analysis.
        </p>
    </div>

    <div class="row-fluid">
        <div class="span2 well" id="nav">
            <!-- sidebar -->
            <ul class="nav nav-list">
                <li
                    %if onpage == "intro":
                    class="active"
                    %end
                >
                    <a href="/intro">Introduction</a></li>
                <li
                    %if onpage == "getting_started":
                    class="active"
                    %end
                >
                    <a href="/getting_started">Getting Started</a></li>
                <li
                    %if onpage == "examples":
                    class="active"
                    %end
                >
                    <a href="/examples">Examples</a></li>
                <li class="divider"></li>
                <li
                    %if onpage == "metrics":
                    class="active"
                    %end
                >
                    <a href="/metrics">Metrics List</a></li>
                <li class="divider"></li>
                <li
                    %if onpage == "contact":
                    class="active"
                    %end
                >
                    <a href="/contact">Contact Us</a></li>
            </ul>
        </div>

        %if error is not None:
        <div class="span10 alert alert-error"><strong>Error:</strong> {{error}}</div>
        <div style="visible: false;">
        %else:
        <div class="span10">
        %end
