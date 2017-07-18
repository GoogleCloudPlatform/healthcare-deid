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

"""Beam pipeline that pushes PhysioNet records to BigQuery.

Requires Apache Beam client:
pip install --upgrade apache_beam
"""

from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from physionet import files_to_physionet_records as f2pn


def map_file_to_records(file_path):
  """Parse MIST records from the given file."""
  reader = beam.io.filesystems.FileSystems.open(file_path)
  text = ''.join([line for line in reader])
  for record in text.split('||||START_OF_RECORD||||'):
    if '\n' not in record:
      continue
    yield record


def map_to_bq_inputs(text):
  """Parse the PhysioNet text and get patient_id, record_number, and note."""
  # There is no offical "MIST Format", so we use:
  # ||||START_OF_RECORD||||<patient_id>\n<note>
  patient_id, note = text.split('\n', 1)
  if not patient_id or not note:
    return
  output = {
      'patient_id': int(patient_id),
      'note': note.strip()
  }
  return output


def run_pipeline(input_pattern, output_table, pipeline_args):
  """Read the records from GCS and write them to BigQuery."""
  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  _ = (p |
       'match_files' >> beam.Create(f2pn.match_files(input_pattern)) |
       'to_records' >> beam.FlatMap(map_file_to_records) |
       'map_to_bq_inputs' >> beam.Map(map_to_bq_inputs) |
       'write' >> beam.io.Write(beam.io.BigQuerySink(
           output_table,
           schema='patient_id:INTEGER, note:STRING',
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
  result = p.run().wait_until_finish()
  logging.info('GCS to BigQuery result: %s', result)


def add_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--output_table', type=str, required=True,
                      help='BigQuery table to store output data.')


# Add arguments that won't be explicitly specified when this module is used as
# part of a larger program. These args are only needed when this is run as a
# stand-alone tool.
def add_all_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--input_pattern', type=str, required=True,
                      help='GCS pattern to read input from.')
  add_args(parser)
