#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import csv
import logging

import apache_beam as beam


class ParseFileFn(beam.DoFn):
    def __init__(self, headers):
        self.headers = headers
        super(ParseFileFn, self).__init__()

    def process(self, elem):
        row = list(csv.reader([elem]))[0]
        dictionary = {key: value for (key, value) in zip(self.headers, row)}
        logging.info(dictionary)
        yield dictionary


class ParseFileTransform(beam.PTransform):
    def __init__(self, headers):
        self.headers = headers

    def expand(self, pcoll):
        return (pcoll
                | 'ParseFileFn' >> beam.ParDo(ParseFileFn(self.headers)))
