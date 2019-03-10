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

import argparse
import logging
import json

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ParsePubSubMessageFn(beam.DoFn):
    def __init__(self):
        super(ParsePubSubMessageFn, self).__init__()

    def process(self, elem):
        message = json.loads(elem)
        yield message


def run(argv=None):
    """Pipeline for reading data from a PubSub topic or a Cloud Storage bucket,
    redacting the data using Cloud DLP and writing the results to BigQuery"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='PubSub topic to read from.')
    parser.add_argument('--output',
                        dest='output',
                        help='BigQuery output dataset and table name in the format dataset.tablename')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 # 1. Read in the file from PubSub.
                 | 'ReadFromPubSub' >> ReadFromPubSub()

                 # 2. Process the JSON message from PubSub
                 | 'ParseMessage'

                 )

        average = (lines
                   | 'ApplyWindow'
                   )
        # 3. For each Key, sum up the values
        # 4. Format the as Python dictionaries for writing to BigQuery

        (lines
         # 4. Format the as Python dictionaries for writing to BigQuery
         | 'ConvertToDictionary'
         # 5. Write the output to BigQuery
         | 'WriteToBigQuery' >> WriteToBigQuery(
                    known_args.output,
                    schema='id:INTEGER, total:INTEGER',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
