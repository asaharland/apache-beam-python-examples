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
import csv

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ParseFileFn(beam.DoFn):
    def __init__(self, headers):
        self.headers = headers
        super(ParseFileFn, self).__init__()

    def process(self, elem):
        row = list(csv.reader([elem]))[0]
        dictionary = {key: value for (key, value) in zip(self.headers, row)}
        logging.info(dictionary)
        yield dictionary


def run(argv=None):
    """Pipeline for reading data from a Cloud Storage bucket and writing the results to BigQuery"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read in.')
    parser.add_argument('--output',
                        dest='output',
                        help='BigQuery output dataset and table name in the format dataset.tablename')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:

        user_request_header = ['id', 'first_name', 'last_name', 'email', 'gender']

        input_rows = (p
                      | 'ReadFile' >> ReadFromText(known_args.input, skip_header_lines=1)
                      | 'ParseFile' >> beam.ParDo(ParseFileFn(user_request_header))
                      )

        (input_rows
         | 'WriteToBigQuery' >> WriteToBigQuery(
                    known_args.output,
                    schema='id:INTEGER, first_name:STRING, last_name:STRING, email:STRING, gender:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
         )

        p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
