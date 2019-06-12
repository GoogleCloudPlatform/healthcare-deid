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

"""Tests for run_deid_lib.py."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import unittest

from physionet import run_deid_lib

from mock import call
from mock import MagicMock
from mock import Mock
from mock import patch

REQUIRED_ARGS = ['--config_file', 'my_file.config',
                 '--input_pattern', 'gs://input/file-??-of-??',
                 '--output_directory', 'gs://output',
                 '--project', 'my-project',
                 '--log_directory', 'gs://logs']


class FakeCredentials(object):

  def get(self):  # pylint:disable=invalid-name
    return self

  # pylint:disable=invalid-name,unused-argument
  def authorize(self, http, **kw):
    return http


class RunDeidTest(unittest.TestCase):

  def parse_args(self, raw_args):
    parser = argparse.ArgumentParser()
    run_deid_lib.add_all_args(parser)
    return parser.parse_args(raw_args)

  def run_pipeline(self, raw_args, storage_client=None, credentials=None):
    args = self.parse_args(raw_args)
    return run_deid_lib.run_pipeline(
        args.input_pattern,
        args.output_directory,
        args.config_file,
        args.project,
        args.log_directory,
        args.dict_directory,
        args.lists_directory,
        args.max_num_threads,
        storage_client=storage_client,
        credentials=credentials)

  def testParseArgs(self):
    """Sanity-check for parse_args."""
    parsed = self.parse_args(REQUIRED_ARGS)
    self.assertEqual('my_file.config', parsed.config_file)

  def testInvalidInputPattern(self):
    """Program fails if --input_pattern is not as expected."""
    args = REQUIRED_ARGS[:]
    args[3] = 'gs://onlybucketname'
    with self.assertRaises(Exception):
      self.run_pipeline(args)

    args[3] = 's3://not-gcs/path'
    with self.assertRaises(Exception):
      self.run_pipeline(args)

  @patch('physionet.run_deid_lib.run_deid')
  def testBucketLookup(self, mock_run_deid):
    """Check that the file lookup works properly."""

    blobs = [Mock(), Mock(), Mock(), Mock()]
    # 'name' is a special keyword arg for Mock, so we have to set it like this,
    # instead of passing it to the constructor.
    blobs[0].name = 'file-00-of-02'
    blobs[1].name = 'file-01-of-02'
    blobs[2].name = 'file-02-of-02'
    blobs[3].name = 'unmatched'
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = blobs
    mock_storage_client = MagicMock()
    mock_storage_client.lookup_bucket.return_value = mock_bucket
    fake_creds = FakeCredentials()
    self.run_pipeline(REQUIRED_ARGS, mock_storage_client, fake_creds)

    mock_run_deid.assert_has_calls([
        call('gs://input/file-00-of-02', 'gs://output', 'my_file.config',
             'my-project', 'gs://logs', None, None, '', False, fake_creds, []),
        call('gs://input/file-01-of-02', 'gs://output', 'my_file.config',
             'my-project', 'gs://logs', None, None, '', False, fake_creds, []),
        call('gs://input/file-02-of-02', 'gs://output', 'my_file.config',
             'my-project', 'gs://logs', None, None, '', False, fake_creds, [])
    ],
                                   any_order=True)

  @patch('apiclient.discovery.build')
  @patch('oauth2client.client.GoogleCredentials.get_application_default')
  def testRunDeid(self, mock_credential_fn, mock_build_fn):
    """Test run_deid() which makes the actual call to the cloud API."""

    # Set up mocks so we can verify run() is called correctly.
    mock_credential_fn.return_value = 'fake-credentials'
    run_object = Mock()
    run_object.execute.return_value = {'done': True, 'name': 'op_name'}
    run_fn = Mock(return_value=run_object)
    mock_build_fn.return_value = Mock(
        **{'pipelines.return_value': Mock(run=run_fn)})

    # Run the pipeline.
    run_deid_lib.run_deid(
        'infile', 'outdir', 'test.config', 'my-project-id', 'logdir',
        dict_directory=None, lists_directory=None, service_account=None,
        include_original_in_output=False, credentials='fake', exceptions=[])

    # Check that run() was called with the expected request.
    expected_request_body = {
        'pipelineArgs': {
            'projectId': 'my-project-id',
            'logging': {
                'gcsPath': 'logdir'
            },
            'serviceAccount': {}
        },
        'ephemeralPipeline': {
            'projectId': 'my-project-id',
            'docker': {
                'cmd': 'perl deid.pl input deid.config',
                'imageName':
                    'gcr.io/my-project-id/physionet:latest'
            },
            'name': 'deid',
            'inputParameters': [
                {
                    'name': 'config',
                    'default_value': 'test.config',
                    'localCopy': {
                        'path': 'deid.config',
                        'disk': 'boot'
                    }
                },
                {
                    'name': 'input',
                    'default_value': 'infile',
                    'localCopy': {
                        'path': 'input.text',
                        'disk': 'boot'
                    }
                }
            ],
            'outputParameters': [
                {
                    'name': 'output',
                    'default_value': 'outdir/infile',
                    'localCopy': {
                        'path': 'input.res',
                        'disk': 'boot'
                    },
                },
                {
                    'name': 'output phi',
                    'default_value': 'outdir/infile.phi',
                    'localCopy': {
                        'path': 'input.phi',
                        'disk': 'boot'
                    }
                }
            ],
        },
    }
    run_fn.assert_called_once_with(body=expected_request_body)


if __name__ == '__main__':
  unittest.main()
