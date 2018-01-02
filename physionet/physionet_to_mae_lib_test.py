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

"""Tests for physionet.physionet_to_mae_lib."""

from __future__ import absolute_import

import unittest

from common import testutil
from physionet import physionet_to_mae_lib
from mock import Mock
from mock import patch


class PhysionetToMaeLibTest(unittest.TestCase):

  @patch('apache_beam.io.filesystems.FileSystems')
  @patch('google.cloud.storage.Client')
  def testRunPipeline(self, fake_client_fn, mock_filesystems_fn):
    result1 = Mock(metadata_list=[Mock(path='bucketname/file-00000-of-00001')])
    mock_filesystems_fn.match.return_value = [result1]
    mock_filesystems_fn.open = testutil.fake_open
    fake_client_fn.return_value = testutil.FakeStorageClient()
    testutil.set_gcs_file('bucketname/file-00000-of-00001.phi',
                          """
Patient 1\tNote 1
17\t17\t20
Patient 1\tNote 2
0\t0\t3
8\t8\t16""")
    testutil.set_gcs_file('bucketname/file-00000-of-00001.text',
                          """
START_OF_RECORD=1||||1||||
mundane text and PHI
||||END_OF_RECORD
START_OF_RECORD=1||||2||||
PHI and MORE PHI as well
||||END_OF_RECORD""")

    physionet_to_mae_lib.run_pipeline(
        'gs://bucketname/file-?????-of-?????', 'gs://bucketname/output/',
        'InspectPhiTask', 'project-id', pipeline_args=None)

    expected_file1 = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[mundane text and PHI]]></TEXT>
<TAGS>
<UNKNOWN_CLASSIFICATION_TYPE id="UNKNOWN_CLASSIFICATION_TYPE0" spans="17~20" />
</TAGS></InspectPhiTask>
"""
    expected_file2 = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[PHI and MORE PHI as well]]></TEXT>
<TAGS>
<UNKNOWN_CLASSIFICATION_TYPE id="UNKNOWN_CLASSIFICATION_TYPE0" spans="0~3" />
<UNKNOWN_CLASSIFICATION_TYPE id="UNKNOWN_CLASSIFICATION_TYPE1" spans="8~16" />
</TAGS></InspectPhiTask>
"""
    self.assertEqual(expected_file1,
                     testutil.get_gcs_file('bucketname/output/1-1.xml'))
    self.assertEqual(expected_file2,
                     testutil.get_gcs_file('bucketname/output/1-2.xml'))

if __name__ == '__main__':
  unittest.main()
