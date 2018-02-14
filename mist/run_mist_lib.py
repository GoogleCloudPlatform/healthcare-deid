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

"""Run MIST on Google Cloud Platform.

All input/output files should be on Google Cloud Storage.

Requires Google Python API Client and Apache Beam client:
pip install --upgrade google-api-python-client
pip install --upgrade apache_beam
"""

import posixpath

from common import run_docker

POLL_INTERVAL_SECONDS = 5
DOCKER_IMAGE_NAME = 'gcr.io/genomics-api-test/mist:latest'


def run_deid(input_filename, output_directory, model_filename, project_id,
             log_directory, service_account, credentials, exceptions):
  """Calls Google APIs to run DeID on Docker and waits for the response."""

  cmds = [('$MAT_PKG_HOME/bin/MATManagePluginDirs install '
           '$MAT_PKG_HOME/sample/ne'),
          'cp model $MAT_PKG_HOME/../tasks/HIPAA/default_model',
          ('$MAT_PKG_HOME/bin/MATEngine '
           '--task "HIPAA Deidentification" --workflow Demo '
           '--steps clean,zone,tag,nominate,transform '
           '--input_file input.txt --input_file_type raw '
           '--output_file output.txt --output_file_type raw '
           '--tagger_local --replacer "clear -> DE-ID"'),
         ]

  inputs = [('input file', input_filename, 'input.txt'),
            ('model file', model_filename, 'model')]
  outputs = [('output file', 'output.txt',
              posixpath.join(output_directory,
                             posixpath.basename(input_filename)))]
  run_docker.run_docker(cmds, project_id, log_directory, DOCKER_IMAGE_NAME,
                        inputs, outputs, service_account, credentials,
                        exceptions)


def run_pipeline(input_pattern, output_directory, model_filename, project_id,
                 log_directory, max_num_threads, service_account,
                 storage_client, credentials):
  """Find the files in GCS, run DeID on them, and write output to GCS."""
  return run_docker.run_pipeline(input_pattern, run_deid,
                                 [output_directory, model_filename, project_id,
                                  log_directory, service_account],
                                 max_num_threads, storage_client, credentials)


def add_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--model_filename', type=str, required=True,
                      help='GCS path to read the model file from.')
  parser.add_argument('--project', type=str, required=True,
                      help='GCP project to run as.')
  parser.add_argument('--max_num_threads', type=int, default=10,
                      help='Run at most this many GCP pipeline jobs at once.')
  parser.add_argument('--service_account', type=str,
                      help=('Service account that should run de-id job(s).'))


# Add arguments that won't be explicitly specified when this module is used as
# part of a larger program. These args are only needed when this is run as a
# stand-alone tool.
def add_all_args(parser):
  add_args(parser)
  parser.add_argument('--input_pattern', type=str, required=True,
                      help='GCS pattern to read the input file(s) from. ')
  parser.add_argument('--output_directory', type=str, required=True,
                      help='GCS directory to write the output to.')
  parser.add_argument('--log_directory', type=str, required=True,
                      help='GCS directory where the logs should be written.')
