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

"""Beam pipeline that converts PhysioNet records to MAE format.

Requires Apache Beam client:
pip install --upgrade apache_beam
"""

from __future__ import absolute_import

import logging
import posixpath

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from common import gcsutil
from common import mae
from physionet import files_to_physionet_records as f2pn
from google.cloud import storage


def write_mae(mae_result, project, mae_dir):
  """Write the MAE results to GCS."""
  storage_client = storage.Client(project)
  filename = '{}.xml'.format(mae_result.record_id)
  gcs_name = gcsutil.GcsFileName.from_path(mae_dir)
  bucket = storage_client.get_bucket(gcs_name.bucket)
  blob = bucket.blob(posixpath.join(gcs_name.blob, filename))
  blob.upload_from_string(mae_result.mae_xml)


def run_pipeline(input_pattern, output_dir, mae_task_name, project,
                 pipeline_args):
  """Read the physionet records from GCS and write them out as MAE."""
  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  _ = (p |
       'match_files' >> beam.Create(f2pn.match_files(input_pattern)) |
       'to_records' >> beam.FlatMap(f2pn.map_phi_to_findings) |
       'generate_mae' >> beam.Map(mae.generate_mae, mae_task_name, {},
                                  ['patient_id', 'record_number']) |
       'write_mae' >> beam.Map(write_mae, project, output_dir)
      )
  result = p.run().wait_until_finish()
  logging.info('GCS to BigQuery result: %s', result)


def add_args(parser, include_project=True):
  """Add command-line arguments to the program."""
  parser.add_argument('--mae_output_dir', type=str, required=True,
                      help='GCS directory to store output data.')
  parser.add_argument('--mae_task_name', type=str, required=False,
                      help='Task name to use in generated MAE files.',
                      default='InspectPhiTask')
  if include_project:
    parser.add_argument('--project', type=str, required=True,
                        help='GCP project to run as.')


# Add arguments that won't be explicitly specified when this module is used as
# part of a larger program. These args are only needed when this is run as a
# stand-alone tool.
def add_all_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--input_pattern', type=str, required=True,
                      help='GCS pattern to read input from.')
  add_args(parser)
