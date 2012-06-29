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

"""This module ...

todo: Lots more text.
"""

import gflags
import logging
import server
import sys

FLAGS = gflags.FLAGS

gflags.DEFINE_enum('logging_level', 'INFO',
                   ['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                   'Verbosity of logging.')


def main(argv):
    # Parse flags via the gflags module.
    try:
        argv = FLAGS(argv)
    except gflags.FlagsError, e:
        print '%s\nUsage: %s ARGS\n%s' % (e, sys.argv[0], FLAGS)
        sys.exit(1)

    # Grab the logging level from gflags.
    if FLAGS.logging_level == 'DEBUG':
        verbosity = logging.DEBUG
    elif FLAGS.logging_level == 'INFO':
        verbosity = logging.INFO
    elif FLAGS.logging_level == 'WARNING':
        verbosity = logging.WARNING
    else:
        verbosity = logging.ERROR

    # Set up logging and start the server.
    logging.basicConfig(level=verbosity)
    server.start()


if __name__ == '__main__':
    main(sys.argv)
