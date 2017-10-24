"""Tests for google3.third_party.py.dlp.run_deid_lib."""

from __future__ import absolute_import

import os
import unittest

from apache_beam.io import iobase
from dlp import run_deid_lib

from mock import Mock
from mock import patch

TESTDATA_DIR = 'dlp/'
fake_db = {}
fake_gcs = {}


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


class FakeBlob(object):

  def __init__(self, bucket_name, file_name):
    self._file_name = os.path.join(bucket_name, file_name)

  def upload_from_string(self, contents):
    print contents[:100]
    fake_gcs[self._file_name] = contents


class FakeBucket(object):

  def __init__(self, bucket_name):
    self._bucket_name = bucket_name

  def blob(self, file_name):
    return FakeBlob(self._bucket_name, file_name)


class RunDeidLibTest(unittest.TestCase):

  @patch('google.cloud.storage.Client')
  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testE2E(self, mock_bq_source_fn, mock_bq_sink_fn, mock_build_fn,
              mock_client_fn):
    def make_sink(table_name, schema, write_disposition):  # pylint: disable=unused-argument
      return FakeSink(table_name)
    mock_bq_sink_fn.side_effect = make_sink

    mock_bq_source_fn.return_value = FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'note': 'text and PID and MORE PID',
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

    def make_bucket(bucket_name):
      return FakeBucket(bucket_name)
    client = Mock()
    client.get_bucket = Mock(side_effect=make_bucket)
    mock_client_fn.return_value = client

    deid_cfg = os.path.join(TESTDATA_DIR, 'sample_deid_config.json')
    run_deid_lib.run_pipeline(
        'input_query', None, 'deid_tbl', 'findings_tbl', 'annotations_tbl',
        'gs://mae-bucket/mae-dir', deid_cfg, 'InspectPhiTask', 'project-name',
        'fake-credentials', pipeline_args=None)

    self.maxDiff = 10000
    with open(os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.xml')) as f:
      contents = f.read()
      self.assertEqual(fake_gcs['mae-bucket/mae-dir/111-1.xml'], contents)
    with open(
        os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.dtd')) as f:
      contents = f.read()
      self.assertEqual(fake_gcs['mae-bucket/mae-dir/classification.dtd'],
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
