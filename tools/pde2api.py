#!/usr/bin/python
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

from collections import defaultdict
import os

X_FILE = r'../../data/mlab_all_countries/%s_measurements.txt'
LOCALES = ('city', 'region', 'country', 'world')

SKIP_COUNT = 0

def explode_pde_measurements(locale):
	global SKIP_COUNT

	with open(X_FILE % locale, 'r') as fd:
		hdr = fd.readline().strip().split(',')
		metrics = dict((h, defaultdict(list)) for h in hdr
		               if h not in (locale, 'month'))

		for line in fd:
			data = dict(zip(hdr, line.strip().split(',')))
			data['month'] = '%s-%s' % (data['month'][-4:], data['month'][:2])
			for met in metrics:
				if data[met] == '':
					SKIP_COUNT += 1
					continue
				mon = data['month']
				if locale in data:
					metrics[met][mon].append((data[locale], data[met]))
				else:
					metrics[met][mon].append((data[met],))
				

	for met in metrics:
		for mon in metrics[met]:
			path = 'data/%s/%s' % (met, mon)
			if not os.path.exists(path):
				os.makedirs(path)
			with open('%s/%s.csv' % (path, locale), 'w') as fd:
				for info in metrics[met][mon]:
					fd.write('%s\n' % ','.join(info))

for locale in LOCALES:
	SKIP_COUNT = 0
	explode_pde_measurements(locale)
	print('%s missing metrics: %d' % (locale, SKIP_COUNT))
