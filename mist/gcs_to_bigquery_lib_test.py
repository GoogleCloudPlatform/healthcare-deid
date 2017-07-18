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

"""Tests for gcs_to_bigquery_lib.py."""

from __future__ import absolute_import

import unittest

from mist import gcs_to_bigquery_lib

from mock import patch


class GcsToBigQueryTest(unittest.TestCase):

  def testMapToBqInputs(self):
    """Tests for map_to_bq_inputs."""
    correct_result = {'patient_id': 111,
                      'note': 'text input\nmultiple lines'}
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs("""111
text input
multiple lines"""))

    # Record may end with an extra newline.
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs("""111
text input
multiple lines
"""))

    # Text is empty, but the record is valid.
    correct_result['note'] = ''
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs('111\n '))

  @patch('apache_beam.io.filesystems.FileSystems')
  def testMapFileToRecords(self, mock_filesystems):
    """Tests for map_file_to_records."""
    record1 = ['||||START_OF_RECORD||||1\n', 'some\n', 'contents']
    record2 = ['||||START_OF_RECORD||||2\n', 'more\n', 'contents']
    mock_filesystems.open.return_value = record1 + record2

    result = [r for r in gcs_to_bigquery_lib.map_file_to_records('filename')]

    self.assertEqual(['1\nsome\ncontents', '2\nmore\ncontents'], result)

if __name__ == '__main__':
  unittest.main()
