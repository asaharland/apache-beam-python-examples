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

from complete.merge_pipeline.joiner.file_transform import ParseFileTransform

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def join_info(name_info):
    (name, info) = name_info
    return '%s; %s; %s' % \
           (name, sorted(info['emails']), sorted(info['phones']))


def run(argv=None):
    """Pipeline for reading data from a Cloud Storage bucket and writing the results to BigQuery."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--credit_card_file',
                        dest='credit_card_file',
                        help="File containing a user's credit card details to read in.")
    parser.add_argument('--phone_file',
                        dest='phone_file',
                        help="File containing a user's phone number to read in.")
    parser.add_argument('--output',
                        dest='output',
                        help='Output path')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        phone_header = ['name', 'phone']
        phone_rows = (p
                      | 'ReadRequestFile' >> ReadFromText(known_args.email_file, skip_header_lines=1)
                      | 'ParseRequestFile' >> ParseFileTransform(phone_header)
                      | 'CreateRequestKVPairs' >> beam.Map(lambda x: (x['name'], x['phone']))
                      )

        credit_card_header = ['name', 'credit_card']
        credit_card_rows = (p
                      | 'ReadInformationFile' >> ReadFromText(known_args.phone_file, skip_header_lines=1)
                      | 'ParseInformationFile' >> ParseFileTransform(credit_card_header)
                      | 'CreateInformationKVPairs' >> beam.Map(lambda x: (x['name'], x['credit_card']))
                      )

        joined_set = ({'phones': phone_rows, 'credit_card': credit_card_rows}
                      | 'CoGroupById' >> beam.CoGroupByKey()
                      | 'Format' >> beam.Map(join_info)
                      )

        joined_set | 'WriteToFile' >> beam.io.WriteToText(known_args.output)

        p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
