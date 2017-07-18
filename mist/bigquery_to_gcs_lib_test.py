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

"""Tests for bigquery_to_gcs_lib."""

from __future__ import absolute_import

import unittest

from mist import bigquery_to_gcs_lib


class BigqueryToGcsTest(unittest.TestCase):

  def test_to_mist_record(self):
    row = {'patient_id': 999, 'note': 'test note'}
    self.assertEqual(
        '||||START_OF_RECORD||||999\ntest note',
        bigquery_to_gcs_lib.map_to_mist_record(row))

  def test_to_mist_record_bad_input(self):
    self.assertIsNone(bigquery_to_gcs_lib.map_to_mist_record(
        {'patient_id': 999, 'record_number': 1}))


if __name__ == '__main__':
  unittest.main()
