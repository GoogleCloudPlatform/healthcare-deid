# Copyright 2017 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Beam pipeline that converts BigQuery data to MIST data in GCS.


Requires Apache Beam client:
pip install --upgrade apache_beam
"""

from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def map_to_mist_record(row):
  """Put the table row into our MIST format."""
  if 'patient_id' not in row or 'note' not in row:
    logging.error('Missing one or more of (patient_id, note): %s', row)
    return None
  # There is no offical "MIST Format", so we use:
  # ||||START_OF_RECORD||||<patient_id>\n<note>
  return '||||START_OF_RECORD||||%s\n%s' % (row['patient_id'], row['note'])


def run_pipeline(input_query, output_path, pipeline_args):
  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  _ = (p
       | 'read' >> beam.io.Read(beam.io.BigQuerySource(query=input_query))
       | 'to_mist' >> beam.Map(map_to_mist_record)
       | 'write' >> beam.io.WriteToText(output_path))
  result = p.run().wait_until_finish()

  logging.info('BigQuery to GCS result: %s', result)


def add_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument(
      '--input_query', type=str, required=True,
      help=('BigQuery query to provide input data. Must yield rows with 2 '
            'fields: (patient_id, note).'))


# Add arguments that won't be explicitly specified when this module is used as
# part of a larger program. These args are only needed when this is run as a
# stand-alone tool.
def add_all_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--output_path', type=str, required=True,
                      help='GCS path to write the output to.')
  add_args(parser)
