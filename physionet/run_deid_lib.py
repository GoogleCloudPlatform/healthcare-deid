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

"""Run Physionet DeID on Google Cloud Platform.

All input/output files should be on Google Cloud Storage.

Requires Google Python API Client and Google Cloud Storage client:
pip install --upgrade google-api-python-client
pip install --upgrade google-cloud-storage
"""

import posixpath

from common import run_docker

DOCKER_NAME_TEMPLATE = 'gcr.io/%s/physionet:latest'


def run_deid(input_filename, output_directory, config_file, project_id,
             log_directory, dict_directory, lists_directory, service_account,
             include_original_in_output, credentials, exceptions):
  """Calls Google APIs to run DeID on Docker and waits for the response."""
  output_filename = posixpath.join(output_directory,
                                   posixpath.basename(input_filename))

  cmds = []
  if dict_directory:
    cmds.append('gsutil -m cp %s dict/' % posixpath.join(dict_directory, '*'))
  if lists_directory:
    cmds.append('gsutil -m cp %s lists/' % posixpath.join(lists_directory, '*'))
  cmds.append('perl deid.pl input deid.config')

  inputs = [('config', config_file, 'deid.config'),
            ('input', input_filename, 'input.text')]
  outputs = [('output', 'input.res', output_filename),
             ('output phi', 'input.phi', output_filename + '.phi')]
  if include_original_in_output:
    outputs.append(('output text', 'input.text', output_filename + '.text'))
  docker_image_name = DOCKER_NAME_TEMPLATE % project_id
  run_docker.run_docker(cmds, project_id, log_directory, docker_image_name,
                        inputs, outputs, service_account, credentials,
                        exceptions)


def run_pipeline(input_pattern, output_directory, config_file, project_id,
                 log_directory, dict_directory, lists_directory,
                 max_num_threads, include_original_in_output=False,
                 service_account='', storage_client=None, credentials=None):
  """Find the files in GCS, run DeID on them, and write output to GCS."""
  return run_docker.run_pipeline(
      input_pattern, run_deid,
      [output_directory, config_file, project_id, log_directory, dict_directory,
       lists_directory, service_account, include_original_in_output],
      max_num_threads, storage_client, credentials)


def add_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument(
      '--config_file', type=str, required=True,
      help='GCS path to read the Physionet DeID config file from.')
  parser.add_argument('--project', type=str, required=True,
                      help='GCP project to run as.')
  parser.add_argument(
      '--dict_directory', type=str,
      help=('GCS directory containing dictionary files to use (see options at '
            'physionet.org/physiotools/deid/).'))
  parser.add_argument(
      '--lists_directory', type=str,
      help=('GCS directory containing list files to use (see options at '
            'physionet.org/physiotools/deid/).'))
  parser.add_argument('--max_num_threads', type=int, default=10,
                      help='Run at most this many GCP pipeline jobs at once.')
  parser.add_argument('--service_account', type=str,
                      help='Service account that should run de-id job(s).')
  parser.add_argument('--include_original_in_pn_output', type=bool,
                      default=False,
                      help=('If true, include the original note alongside the '
                            'redacted one in the PhysioNet DeID output.'))


# Add arguments that won't be explicitly specified when this module is used as
# part of a larger program. These args are only needed when this is run as a
# stand-alone tool.
def add_all_args(parser):
  """Add args used when this module is run as a stand-alone tool."""
  add_args(parser)
  parser.add_argument('--input_pattern', type=str, required=True,
                      help=('GCS pattern to read the input file(s) from. '
                            'Accepts ? and * as single and repeated wildcards, '
                            'respectively.'))
  parser.add_argument('--output_directory', type=str, required=True,
                      help='GCS directory to write the output to.')
  parser.add_argument('--log_directory', type=str, required=True,
                      help='GCS directory where the logs should be written.')
