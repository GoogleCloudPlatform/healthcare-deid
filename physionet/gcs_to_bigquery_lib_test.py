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

from healthcare_deid.physionet import gcs_to_bigquery_lib


class GcsToBigQueryTest(unittest.TestCase):

  def testMapToBqInputs(self):
    """Tests for map_to_bq_inputs."""
    correct_result = {'patient_id': 111, 'record_number': 222, 'note':
                      'text input\nmultiple lines'}
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs("""START_OF_RECORD=111||||222||||
text input
multiple lines
||||END_OF_RECORD"""))

    # We accept (but ignore) the optional date field.
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """START_OF_RECORD=111||||222||||08/06/2011||||
text input
multiple lines
||||END_OF_RECORD"""))

    # Record may end with an extra newline.
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """START_OF_RECORD=111||||222||||
text input
multiple lines
||||END_OF_RECORD\n"""))

    # Text is empty, but the record is valid.
    correct_result['note'] = ''
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """START_OF_RECORD=111||||222||||||||END_OF_RECORD"""))

    # Record starts with some extra newlines.
    correct_result['note'] = ''
    self.assertEqual(
        correct_result,
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """\n\nSTART_OF_RECORD=111||||222||||||||END_OF_RECORD"""))

  def testMapToBqInputsInvalidRecords(self):
    """Tests for invalid arguments to map_to_bq_inputs."""

    # No start of record.
    self.assertIsNone(
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """111||||222||||
text input
multiple lines
||||END_OF_RECORD\n"""))

    # No end-of-record.
    self.assertIsNone(
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """START_OF_RECORD=111||||222||||
text input
multiple lines
"""))

    # No record number.
    self.assertIsNone(
        gcs_to_bigquery_lib.map_to_bq_inputs(
            """START_OF_RECORD=111||||
text input
multiple lines
||||END_OF_RECORD\n"""))


if __name__ == '__main__':
  unittest.main()
