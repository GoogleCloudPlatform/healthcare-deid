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

from physionet import files_to_physionet_records as ftp

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
    record1 = ['START_OF_RECORD=1||||2||||\n', 'some\n', 'contents\n',
               '||||END_OF_RECORD\n']
    record2 = ['START_OF_RECORD=11||||22||||\n', 'more\n', 'contents\n',
               '||||END_OF_RECORD\n']
    mock_filesystems.open.return_value = record1 + record2

    result = [r for r in ftp.map_file_to_records('path')]

    self.assertEqual([''.join(record1), ''.join(record2)], result)

  @patch('apache_beam.io.filesystems.FileSystems')
  def testMapPhiToFindings(self, mock_filesystems):
    dot_phi_text = """
Patient 11\tNote 232
29\t29\t38
Patient 1\tNote 22
23\t23\t32
62\t62\t67
68\t68\t78""".split('\n')
    original_note_text = [
        'START_OF_RECORD=11||||232||||\n', 'some\n', 'contents\n',
        '||||END_OF_RECORD\n', 'START_OF_RECORD=1||||22||||\n', 'more\n',
        'contents\n', '||||END_OF_RECORD\n']
    def _open(filename):
      if filename.endswith('.text'):
        return original_note_text
      return dot_phi_text

    mock_filesystems.open = _open

    results = [r for r in ftp.map_phi_to_findings('path')]
    expected_results = [
        {'patient_id': '11', 'record_number': '232',
         'result': {'findings': [
             {'infoType': {'name': ''},
              'location': {'codepointRange': {'end': 38, 'start': 29}}}]},
         'original_note': 'some\ncontents'},
        {'patient_id': '1', 'record_number': '22',
         'result': {'findings': [
             {'infoType': {'name': ''},
              'location': {'codepointRange': {'end': 32, 'start': 23}}},
             {'infoType': {'name': ''},
              'location': {'codepointRange': {'end': 67, 'start': 62}}},
             {'infoType': {'name': ''},
              'location': {'codepointRange': {'end': 78, 'start': 68}}}]},
         'original_note': 'more\ncontents'}]

    self.assertEqual(results, expected_results)

  def testParsePhysionetRecord(self):
    """Tests for map_to_bq_inputs."""
    correct_result = {'patient_id': 111, 'record_number': 222, 'note':
                      'text input\nmultiple lines'}
    self.assertEqual(
        correct_result,
        ftp.parse_physionet_record("""START_OF_RECORD=111||||222||||
text input
multiple lines
||||END_OF_RECORD"""))

    # We accept (but ignore) the optional date field.
    self.assertEqual(
        correct_result,
        ftp.parse_physionet_record(
            """START_OF_RECORD=111||||222||||08/06/2011||||
text input
multiple lines
||||END_OF_RECORD"""))

    # Record may end with an extra newline.
    self.assertEqual(
        correct_result,
        ftp.parse_physionet_record(
            """START_OF_RECORD=111||||222||||
text input
multiple lines
||||END_OF_RECORD\n"""))

    # Text is empty, but the record is valid.
    correct_result['note'] = ''
    self.assertEqual(
        correct_result,
        ftp.parse_physionet_record(
            """START_OF_RECORD=111||||222||||\n\n||||END_OF_RECORD"""))

    # Record starts with some extra newlines.
    correct_result['note'] = ''
    self.assertEqual(
        correct_result,
        ftp.parse_physionet_record(
            """\n\nSTART_OF_RECORD=111||||222||||\n\n||||END_OF_RECORD"""))

  def testMapToBqInputsInvalidRecords(self):
    """Tests for invalid arguments to map_to_bq_inputs."""

    # No start of record.
    self.assertIsNone(
        ftp.parse_physionet_record(
            """111||||222||||
text input
multiple lines
||||END_OF_RECORD\n"""))

    # No end-of-record.
    self.assertIsNone(
        ftp.parse_physionet_record(
            """START_OF_RECORD=111||||222||||
text input
multiple lines
"""))

    # No record number.
    self.assertIsNone(
        ftp.parse_physionet_record(
            """START_OF_RECORD=111||||
text input
multiple lines
||||END_OF_RECORD\n"""))

if __name__ == '__main__':
  unittest.main()
