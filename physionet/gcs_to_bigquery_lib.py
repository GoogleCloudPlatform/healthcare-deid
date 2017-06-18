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

import argparse
import logging
import re
import sys

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions


def map_to_bq_inputs(text):
  """Parse the PhysioNet text and get patient_id, record_number, and note."""
  # The format is described at:
  # http://physionet.org/physiotools/deid/doc/DeidUserManual.pdf
  sep = r'\|\|\|\|'
  optional_date = r'(\d\d/\d\d/\d\d\d\d' + sep + r')?'
  pattern = (r'(\n)*START_OF_RECORD=(?P<patient_id>\d+?)' + sep +
             r'(?P<record_number>\d+?)' + sep + optional_date +r'(?P<text>.*)' +
             sep + r'END_OF_RECORD')
  match = re.match(pattern, text, re.MULTILINE | re.DOTALL)
  if not match:
    logging.error('Failed to parse record: "%s"', text)
    return

  output = {
      'patient_id': int(match.group('patient_id')),
      'record_number': int(match.group('record_number')),
      'note': match.group('text').strip()
  }
  return output


# TODO(b/62383313): Move these two functions to a shared library.
def map_file_to_records(file_path):
  """Separate full file contents into individual records."""
  reader = FileSystems.open(file_path)
  buf = ''
  for line in reader:
    buf += line
    if '||||END_OF_RECORD' in line:
      yield buf
      buf = ''


def match_files(input_path):
  """Find the list of absolute file paths that match the input path spec."""
  for match_result in FileSystems.match([input_path]):
    for metadata in match_result.metadata_list:
      logging.info('matched path: %s', metadata.path)
      yield metadata.path


def run_pipeline(input_pattern, output_table, pipeline_args):
  """Read the records from GCS and write them to BigQuery."""
  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  _ = (p |
       'match_files' >> beam.Create(match_files(input_pattern)) |
       'to_records' >> beam.FlatMap(map_file_to_records) |
       'map_to_bq_inputs' >> beam.Map(map_to_bq_inputs) |
       'write' >> beam.io.Write(beam.io.BigQuerySink(
           output_table,
           schema='patient_id:INTEGER, record_number:INTEGER, note:STRING',
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
