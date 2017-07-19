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

"""Library for running a docker container on Google Cloud Platform.

Requires Google Python API Client:
pip install --upgrade google-api-python-client
"""

from __future__ import absolute_import

import functools
import logging
from multiprocessing.pool import ThreadPool
import os
import re
import time

from apiclient import discovery
import google.auth

POLL_INTERVAL_SECONDS = 5


def capture_exceptions(function):
  """Wrapper to capture any exceptions in run_deid."""
  @functools.wraps(function)
  def wrapper(*args, **kwargs):
    try:
      function(*args, **kwargs)
    except Exception as e:  # pylint:disable=broad-except
      exceptions = []
      if 'exceptions' in kwargs:
        exceptions = kwargs['exceptions']
      else:
        exceptions = args[-1]
      exceptions.append(e)

  return wrapper


@capture_exceptions
def run_docker(commands, project_id, log_directory, docker_image_name, inputs,
               outputs, service_account, credentials,
               exceptions):  #  pylint: disable=unused-argument
  """Call Google APIs to run commands on Docker and wait for the response.

  Args:
    commands: List of commands to execute on the Docker instance.
    project_id: The project to run as.
    log_directory: GCS directory to store the logs for the Docker run.
    docker_image_name: URL of the docker image to run (usually gcr.io/...)
    inputs: List of (name, src, dest) tuples for files to be copied from GCS to
      the docker container before running.
    outputs: List of (name, src, dest) tuples for files to be copied out of the
      docker container to GCS after running.
    service_account: The service account to run as. May be ''.
    credentials: An oauth2client.client.GoogleCredentials objects.
    exceptions: List, populated by @capture_exceptions if this function
      throws an exception.
  """
  request = {
      'ephemeralPipeline': {
          'docker': {
              'imageName': docker_image_name,
          }
      },
      'pipelineArgs': {
          'logging': {},
          'serviceAccount': {}
      }
  }

  request['ephemeralPipeline']['docker']['cmd'] = ' && '.join(commands)
  request['ephemeralPipeline']['name'] = 'deid'
  request['ephemeralPipeline']['projectId'] = project_id
  request['pipelineArgs']['projectId'] = project_id
  request['pipelineArgs']['logging']['gcsPath'] = log_directory
  if inputs:
    request['ephemeralPipeline']['inputParameters'] = []
    for name, src, dest in inputs:
      request['ephemeralPipeline']['inputParameters'].append(
          {'name': name, 'default_value': src,
           'localCopy': {'path': dest, 'disk': 'boot'}})
  if outputs:
    request['ephemeralPipeline']['outputParameters'] = []
    for name, src, dest in outputs:
      request['ephemeralPipeline']['outputParameters'].append(
          {'name': name, 'default_value': dest,
           'localCopy': {'path': src, 'disk': 'boot'}})
  if service_account:
    request['pipelineArgs']['serviceAccount']['email'] = service_account

  if not credentials:
    credentials, _ = google.auth.default()
  service = discovery.build('genomics', 'v1alpha2', credentials=credentials)
  operation = service.pipelines().run(body=request).execute()
  operation_name = operation['name']
  while not operation['done']:
    time.sleep(POLL_INTERVAL_SECONDS)
    operation = service.operations().get(name=operation_name).execute()

  if 'error' in operation:
    logging.error('error: %s', operation['error']['message'])
    exceptions.append('error: %s' % operation['error']['message'])


def _find_files(pattern, storage_client):
  """Find files on GCS matching the given pattern."""
  # Split the input path to get the bucket name and the path within the bucket.
  re_match = re.match(r'gs://([\w-]+)/(.*)', pattern)
  if not re_match or len(re_match.groups()) != 2:
    logging.error('Failed to parse input pattern: "%s". Expected: '
                  'gs://bucket-name/path/to/file', pattern)
    return
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

  bucket = storage_client.lookup_bucket(bucket_name)
  for blob in bucket.list_blobs(prefix=file_prefix):
    if not re.match(file_pattern_as_regex, blob.name):
      continue
    yield os.path.join('gs://', bucket_name, blob.name)


def run_pipeline(input_pattern, function, args, max_num_threads,
                 storage_client, credentials):
  """Find files and run the given function on them.

  Args:
    input_pattern: String matching the files to be operated on.
    function: Function to be run on each input file. Must take an input filename
      as its first argument.
    args: Additional arguments to pass to the function.
    max_num_threads: Maximum amount of operations to run in parallel.
    storage_client: A google.cloud.storage.Client object.
    credentials: An oauth2client.client.GoogleCredentials objects.

  Returns:
    None on success, error string on failure.
  """
  thread_pool = ThreadPool(max_num_threads)
  exceptions = []
  args.append(credentials)
  args.append(exceptions)
  found_files = False
  for file_path in _find_files(input_pattern, storage_client):
    found_files = True
    logging.info('Found matching file: %s', file_path)
    args_copy = args[:]
    args_copy.insert(0, file_path)
    thread_pool.apply_async(function, args_copy)

  if not found_files:
    logging.error('Failed to find any files matching "%s"', input_pattern)
    return 'No matching files.'

  thread_pool.close()
  thread_pool.join()

  if exceptions:
    logging.error('Some requests failed:')
    for exception in exceptions:
      logging.error(exception)
    return '\n'.join([str(e) for e in exceptions])
