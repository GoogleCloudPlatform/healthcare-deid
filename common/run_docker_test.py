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

"""Tests for run_docker.py."""

from __future__ import absolute_import

import unittest

from common import gcsutil
from common import run_docker

from mock import Mock
from mock import patch

REQUIRED_ARGS = ['--config_file', 'my_file.config',
                 '--input_pattern', 'gs://input/file-??-of-??',
                 '--output_directory', 'gs://output',
                 '--project', 'my-project',
                 '--log_directory', 'gs://logs']


class RunDockerTest(unittest.TestCase):

  def testRunPipeline(self):
    credentials = Mock()
    mock_bucket = Mock()
    blobs = [Mock(), Mock(), Mock(), Mock()]
    # 'name' is a special keyword arg for Mock, so we have to set it like this,
    # instead of passing it to the constructor.
    blobs[0].name = 'file-0'
    blobs[1].name = 'file-1'
    blobs[2].name = 'file-2'
    blobs[3].name = 'unmatched'
    mock_bucket.list_blobs.return_value = blobs
    storage_client = Mock()
    storage_client.lookup_bucket.return_value = mock_bucket

    # Test function that concatenates the first two args, then adds them to the
    # third arg (which is a list).
    def Fn(input_file, arg1, results, unused_credentials, unused_exceptions):
      results.append('%s,%s' % (input_file, arg1))
    results = []
    run_docker.run_pipeline('gs://bucket/file-*', Fn, ['arg1', results],
                            1, storage_client, credentials)

    self.assertEqual(['gs://bucket/file-0,arg1', 'gs://bucket/file-1,arg1',
                      'gs://bucket/file-2,arg1'],
                     results)

  @patch('common.gcsutil.find_files')
  def testRunPipelineWithExceptions(self, mock_find_files):
    mock_find_files.return_value = [gcsutil.GcsFileName('bucket', 'file1')]

    # Test function that throws an exception.
    @run_docker.capture_exceptions
    def Fn(unused_input_file, unused_arg, unused_results,
           unused_credentials, unused_exceptions):
      raise Exception('Smash!')
    results = []
    error_str = run_docker.run_pipeline('input-pattern', Fn, ['arg1', results],
                                        max_num_threads=1, storage_client=None,
                                        credentials=None)

    self.assertEqual('Smash!', error_str)

  @patch('apiclient.discovery.build')
  def testRunDocker(self, mock_build_fn):
    """Test run_docker() which makes the actual call to the cloud API."""

    # Set up mocks so we can verify run() is called correctly.
    run_object = Mock()
    run_object.execute.return_value = {'done': True, 'name': 'op_name'}
    run_fn = Mock(return_value=run_object)
    mock_build_fn.return_value = Mock(
        **{'pipelines.return_value': Mock(run=run_fn)})

    # Run the pipeline.
    run_docker.run_docker(
        ['command1', 'command2'], 'my-project-id', 'logdir', 'DOCKER_IMAGE',
        [('input1', 'src1', 'dest1'), ('input2', 'src2', 'dest2')],
        [('output1', 'src1', 'dest1')], 'service-account',
        'fake-credentials', exceptions=[])

    # Check that run() was called with the expected request.
    expected_request_body = {
        'pipelineArgs': {
            'projectId': 'my-project-id',
            'logging': {
                'gcsPath': 'logdir'
            },
            'serviceAccount': {'email': 'service-account'}
        },
        'ephemeralPipeline': {
            'projectId': 'my-project-id',
            'docker': {
                'cmd': 'command1 && command2',
                'imageName': 'DOCKER_IMAGE'
            },
            'inputParameters': [
                {
                    'name': 'input1',
                    'default_value': 'src1',
                    'localCopy': {
                        'path': 'dest1',
                        'disk': 'boot'
                    }
                },
                {
                    'name': 'input2',
                    'default_value': 'src2',
                    'localCopy': {
                        'path': 'dest2',
                        'disk': 'boot'
                    }
                }
            ],
            'outputParameters': [
                {
                    'name': 'output1',
                    'default_value': 'dest1',
                    'localCopy': {
                        'path': 'src1',
                        'disk': 'boot'
                    }
                },
            ],
            'name': 'deid'
        }
    }
    run_fn.assert_called_once_with(body=expected_request_body)

  @patch('apiclient.discovery.build')
  def testRunDockerRaisesException(self, mock_build_fn):
    # Ensure we capture exceptions raised in run_docker().
    exception = Exception('BOOM!')
    mock_build_fn.side_effect = exception
    inputs = []
    outputs = []
    exceptions = []
    run_docker.run_docker(['command1', 'command2'], 'my-project-id', 'logdir',
                          'DOCKER_IMAGE', inputs, outputs, 'service-account',
                          'fake credentials', exceptions=exceptions)
    self.assertEqual([exception], exceptions)

    # Ensure it works without using kwargs.
    exceptions = []
    run_docker.run_docker(['command1', 'command2'], 'my-project-id', 'logdir',
                          'DOCKER_IMAGE', inputs, outputs, 'service-account',
                          'fake credentials', exceptions)
    self.assertEqual([exception], exceptions)

if __name__ == '__main__':
  unittest.main()
