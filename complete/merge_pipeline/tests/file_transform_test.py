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

import logging
import unittest
from complete.merge_pipeline.joiner import file_transform

import apache_beam as beam


class BasicPipelineTest(unittest.TestCase):

    def test_format(self):
        input_rows = ['value1', 'value2']
        header_row = ['key']
        expected_output = [{'key': 'value1'}, {'key': 'value2'}]

        output_rows = input_rows | beam.ParDo(file_transform.ParseFileFn(header_row))

        self.assertEquals(expected_output, output_rows)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
