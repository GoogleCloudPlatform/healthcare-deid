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

"""Tests for file_pattern_to_physionet_records."""

from __future__ import absolute_import

import unittest

import files_to_physionet_records as ftp

from mock import Mock
from mock import patch


class FilePatternToPhysionetRecordsTest(unittest.TestCase):

  @patch('apache_beam.io.filesystems.FileSystems')
  def testMatchFiles(self, mock_filesystems):
    result1 = Mock(metadata_list=[Mock(path='a'), Mock(path='b')])
    result2 = Mock(metadata_list=[Mock(path='c')])
    mock_filesystems.match.return_value = [result1, result2]

    result = [f for f in ftp.match_files('path')]

    self.assertEqual(['a', 'b', 'c'], result)

  @patch('apache_beam.io.filesystems.FileSystems')
  def testMapFileToRecords(self, mock_filesystems):
    record1 = ['START_OF_RECORD||||1||||2||||\n', 'some\n', 'contents\n',
               '||||END_OF_RECORD\n']
    record2 = ['START_OF_RECORD||||11||||22||||\n', 'more\n', 'contents\n',
               '||||END_OF_RECORD\n']
    mock_filesystems.open.return_value = record1 + record2

    result = [r for r in ftp.map_file_to_records('path')]

    self.assertEqual([''.join(record1), ''.join(record2)], result)


if __name__ == '__main__':
  unittest.main()
