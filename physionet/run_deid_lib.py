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

import logging
from multiprocessing.pool import ThreadPool
import os
import re
import time

from apiclient import discovery
from google.cloud import storage
from oauth2client.client import GoogleCredentials

POLL_INTERVAL_SECONDS = 5


def run_deid(input_filename, output_directory, config_file, project_id,
             log_directory, dict_directory, lists_directory):
  """Calls Google APIs to run DeID on Docker and waits for the response."""
  request = {
      'ephemeralPipeline': {
          'docker': {
              'imageName': 'gcr.io/genomics-api-test/physionet:latest',
          }
      },
      'pipelineArgs': {
          'logging': {}
      }
  }

  output_filename = os.path.join(output_directory,
                                 os.path.basename(input_filename))
  cmds = ['gsutil cp %s deid.config' % config_file,
          'gsutil cp %s input.text' % input_filename]
  if dict_directory:
    cmds.append('gsutil -m cp %s dict/' % os.path.join(dict_directory, '*'))
  if lists_directory:
    cmds.append('gsutil -m cp %s lists/' % os.path.join(lists_directory, '*'))
  cmds += ['perl deid.pl input deid.config',
           'gsutil cp input.res %s' % output_filename]
  request['ephemeralPipeline']['docker']['cmd'] = ' && '.join(cmds)
  request['ephemeralPipeline']['name'] = 'deid'
  request['ephemeralPipeline']['projectId'] = project_id
  request['pipelineArgs']['projectId'] = project_id
  request['pipelineArgs']['logging']['gcsPath'] = log_directory

  credentials = GoogleCredentials.get_application_default()
  service = discovery.build('genomics', 'v1alpha2', credentials=credentials)
  operation = service.pipelines().run(body=request).execute()
  operation_name = operation['name']
  while not operation['done']:
    time.sleep(POLL_INTERVAL_SECONDS)
    operation = service.operations().get(name=operation_name).execute()

  if 'error' in operation:
    print('Error for input file: %s:\n%s' % (input_filename,
                                             operation['error']['message']))


def run_pipeline(input_pattern, output_directory, config_file, project_id,
                 log_directory, dict_directory, lists_directory,
                 max_num_threads, storage_client=None):
  """Find the files in GCS, run DeID on them, and write output to GCS."""
  # Split the input path to get the bucket name and the path within the bucket.
  re_match = re.match(r'gs://([\w-]+)/(.*)', input_pattern)
  if not re_match or len(re_match.groups()) != 2:
    print('Failed to parse input pattern: "%s". Expected: '
          'gs://bucket-name/path/to/file' % input_pattern)
    return 1
  bucket_name = re_match.group(1)
  file_pattern = re_match.group(2)

  # The storage client doesn't take a pattern, just a prefix, so we presume here
  # that the only special/regex-like characters used are '?' and '*', and take
  # the longest prefix that doesn't contain either of those.
  file_prefix = file_pattern
  re_result = re.search(r'(.*?)[\?|\*]', file_pattern)
  if re_result:
    file_prefix = re_result.group(1)

  # Convert file_pattern to a regex by escaping the string, explicitly
  # converting the characters we want to treat specially (* and ?), and
  # appending '\Z' to the end of the pattern so we match only the full string.
  file_pattern_as_regex = (
      re.escape(file_pattern).replace('\\*', '.*').replace('\\?', '.') + r'\Z')

  thread_pool = ThreadPool(max_num_threads)
  if storage_client is None:
    storage_client = storage.Client(project_id)
  bucket = storage_client.lookup_bucket(bucket_name)
  found_files = False
  for blob in bucket.list_blobs(prefix=file_prefix):
    if not re.match(file_pattern_as_regex, blob.name):
      continue
    found_files = True
    path = os.path.join('gs://', bucket_name, blob.name)
    thread_pool.apply_async(run_deid,
                            (path, output_directory, config_file,
                             project_id, log_directory,
                             dict_directory, lists_directory))

  if not found_files:
    logging.error('Failed to find any files matching "%s"', input_pattern)
    return

  thread_pool.close()
  thread_pool.join()


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


# Add arguments that won't be explicitly specified when this module is used as
# part of a larger program. These args are only needed when this is run as a
# stand-alone tool.
def add_all_args(parser):
  add_args(parser)
  parser.add_argument('--input_pattern', type=str, required=True,
                      help=('GCS pattern to read the input file(s) from. '
                            'Accepts ? and * as single and repeated wildcards, '
                            'respectively.'))
  parser.add_argument('--output_directory', type=str, required=True,
                      help='GCS directory to write the output to.')
  parser.add_argument('--log_directory', type=str, required=True,
                      help='GCS directory where the logs should be written.')
