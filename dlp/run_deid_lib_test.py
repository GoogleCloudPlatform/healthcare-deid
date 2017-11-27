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

"""Tests for google3.third_party.py.dlp.run_deid_lib."""

from __future__ import absolute_import

import json
import os
import unittest

from apache_beam.io import iobase
from common import testutil
from dlp import run_deid_lib

from mock import Mock
from mock import patch

TESTDATA_DIR = 'dlp/'
fake_db = {}


class FakeSource(iobase.BoundedSource):

  def __init__(self):
    self._records = []

  def get_range_tracker(self, unused_a, unused_b):
    return None

  def read(self, unused_range_tracker):
    for record in self._records:
      yield record


class FakeWriter(iobase.Writer):

  def __init__(self, table_name):
    self._table_name = table_name

  def write(self, value):
    fake_db[self._table_name] = value

  def close(self):
    pass


class FakeSink(iobase.Sink):

  def __init__(self, table_name):
    self._writer = FakeWriter(table_name)

  def initialize_write(self):
    pass

  def open_writer(self, unused_init_result, unused_uid):
    return self._writer

  def finalize_write(self, unused_init_result, unused_writer_results):
    pass


class ResultStorage(object):

  values = []

  def add(self, value):
    self.values.append(value)


class FakeBqResults(object):

  def __init__(self, schema, results):
    self.schema = schema
    self.results = results

  def __iter__(self):
    return self.results.__iter__()


def ordered(obj):
  if isinstance(obj, dict):
    return sorted((k, ordered(v)) for k, v in obj.items())
  if isinstance(obj, list):
    return sorted(ordered(x) for x in obj)
  else:
    return obj


class RunDeidLibTest(unittest.TestCase):

  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testE2E(self, mock_bq_source_fn, mock_bq_sink_fn, mock_build_fn):
    def make_sink(table_name, schema, write_disposition):  # pylint: disable=unused-argument
      return FakeSink(table_name)
    mock_bq_sink_fn.side_effect = make_sink

    mock_bq_source_fn.return_value = FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'first_name': 'Boaty', 'last_name': 'McBoatface',
         'note': 'text and PID and MORE PID',
         'patient_id': '111', 'record_number': '1'}]

    deid_response = {'items': [{'value': 'deid_resp_val'}]}
    findings = {'findings': [
        {'location': {'byteRange': {'start': '17', 'end': '25'}},
         'infoType': {'name': 'PHONE_NUMBER'}},
        {'location': {'byteRange': {'start': '9', 'end': '12'}},
         'infoType': {'name': 'US_CENSUS_NAME'}},
        {'location': {'byteRange': {'start': '9', 'end': '12'}},
         'infoType': {'name': 'US_MALE_NAME'}}]}
    inspect_response = {'results': [findings]}
    fake_content = Mock()
    fake_content.inspect.return_value = Mock(
        execute=Mock(return_value=inspect_response))
    fake_content.deidentify.return_value = Mock(
        execute=Mock(return_value=deid_response))
    fake_dlp = Mock(content=Mock(return_value=fake_content))
    mock_build_fn.return_value = fake_dlp

    query_job = Mock()
    # 'name' is a special keyword arg for Mock, so we have to set it like this,
    # instead of passing it to the constructor.
    schema = [Mock(), Mock()]
    schema[0].name = 'first_name'
    schema[1].name = 'last_name'
    rows = [['Boaty', 'McBoatface', 'note', 'id', 'recordnum']]
    results_table = FakeBqResults(schema, rows)
    query_job.destination.fetch_data.return_value = results_table
    bq_client = Mock()
    bq_client.run_async_query.return_value = query_job

    deid_cfg = os.path.join(TESTDATA_DIR, 'testdata/config.json')
    storage_client_fn = lambda x, y: testutil.FakeStorageClient()
    run_deid_lib.run_pipeline(
        'input_query', None, 'deid_tbl', 'findings_tbl', 'annotations_tbl',
        'gs://mae-bucket/mae-dir', deid_cfg, 'InspectPhiTask',
        'fake-credentials', 'project', storage_client_fn, bq_client, None,
        'dlp', pipeline_args=None)

    request_body = {}
    with open(os.path.join(TESTDATA_DIR, 'testdata/request.json')) as f:
      request_body = json.load(f)
    fake_content.deidentify.assert_called_once()
    _, kwargs = fake_content.deidentify.call_args
    self.maxDiff = 10000
    self.assertEqual(ordered(request_body), ordered(kwargs['body']))

    with open(os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.xml')) as f:
      contents = f.read()
      self.assertEqual(testutil.get_gcs_file('mae-bucket/mae-dir/111-1.xml'),
                       contents)
    with open(
        os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.dtd')) as f:
      contents = f.read()
      self.assertEqual(
          testutil.get_gcs_file('mae-bucket/mae-dir/classification.dtd'),
          contents)

    self.assertEqual(
        fake_db['deid_tbl'],
        {'patient_id': '111', 'record_number': 1, 'note': 'deid_resp_val'})
    self.assertEqual(
        fake_db['findings_tbl'],
        {'patient_id': '111', 'record_number': 1, 'findings': str(findings)})
    self.assertEqual(
        fake_db['annotations_tbl'],
        {'patient_id': '111', 'record_number': 1,
         'note': ('text and <finding info_types="US_CENSUS_NAME,US_MALE_NAME">'
                  'PID</finding> and <finding info_types="PHONE_NUMBER">'
                  'MORE PID</finding>')})

if __name__ == '__main__':
  unittest.main()
