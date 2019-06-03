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

"""Tests for google3.dlp.run_deid_lib."""

from __future__ import absolute_import

from datetime import datetime
from functools import partial
import json
import os
import tempfile
import unittest

from apiclient import errors
from common import beam_testutil
from common import testutil
from dlp import run_deid_lib
import httplib2

from mock import Mock
from mock import patch

TESTDATA_DIR = 'dlp/'


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


DEID_TIMESTAMP = datetime.utcnow()

TIMESTAMP_STRING = datetime.strftime(DEID_TIMESTAMP, '%Y-%m-%d %H:%M:%S')

EXPECTED_DEID_RESULT = [{
    'note': 'note1 redacted',
    'patient_id': '111',
    'record_number': '1',
    run_deid_lib.DLP_DEID_TIMESTAMP: TIMESTAMP_STRING
},
                        {
                            'note': 'note2 redacted',
                            'patient_id': '222',
                            'record_number': '2',
                            run_deid_lib.DLP_DEID_TIMESTAMP: TIMESTAMP_STRING
                        }]

EXPECTED_MAE1 = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[text and PID and MORE PID]]></TEXT>
<TAGS>
<PHONE id="PHONE0" spans="9~12" />
</TAGS></InspectPhiTask>
"""
EXPECTED_MAE2 = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[note2 text]]></TEXT>
<TAGS>
<FIRST_NAME id="FIRST_NAME0" spans="17~25" />
</TAGS></InspectPhiTask>
"""

DEID_HEADERS = [
    {'name': 'first_name'}, {'name': 'last_name'}, {'name': 'note'},
    {'name': 'patient_id'}, {'name': 'record_number'},
    {'name': run_deid_lib.DLP_DEID_TIMESTAMP}]


def bq_schema():
  # 'name' is a special keyword arg for Mock, so we have to set it like this,
  # instead of passing it to the constructor.
  schema = [Mock(), Mock(), Mock(), Mock(), Mock(), Mock()]
  schema[0].name = 'first_name'
  schema[1].name = 'last_name'
  schema[2].name = 'note'
  schema[3].name = 'patient_id'
  schema[4].name = 'record_number'
  schema[5].name = run_deid_lib.DLP_DEID_TIMESTAMP
  return schema


def sval(x):
  return {'stringValue': x}


_TABLE_TO_SCHEMA = {
    'findings_tbl': ('patient_id:STRING, record_number:INTEGER, '
                     'findings:STRING, dlp_findings_timestamp:TIMESTAMP'),
    'mae_tbl': 'record_id:STRING,xml:STRING',
    'deid_tbl': ('patient_id:STRING, record_number:INTEGER, note:STRING, '
                 'dlp_deid_timestamp:TIMESTAMP')}


class RunDeidLibTest(unittest.TestCase):

  def make_sink(self, table_to_schema, table_name, schema, write_disposition):  # pylint: disable=unused-argument
    expected_schema = set(table_to_schema[table_name].split(', '))
    result_schema = set(schema.split(', '))
    self.assertEqual(expected_schema, result_schema)
    return beam_testutil.FakeSink(table_name)

  def make_csv_output(self, file_path_prefix,
                      file_name_suffix='',
                      append_trailing_newlines=True, num_shards=0,
                      shard_name_template=None, coder=None,
                      compression_type='auto', header=None):
    # pylint: disable=unused-argument

    return beam_testutil.DummyWriteTransform(file_path_prefix)

  @patch('apiclient.discovery.build')
  def testDeidError(self, mock_build_fn):
    deid_response = {
        'item': {'table': {
            'rows': [{'values': [{'stringValue': 'deid_resp_val'},
                                 {'stringValue': 'transformed!!'}]}],
            'headers': [{'name': 'note'}, {'name': 'field_transform_col'}]
        }},
        'overview': {
            'transformationSummaries': [{
                'field': {'name': 'MyField'},
                'results': [{'code': 'ERROR', 'details': 'some details'}]
            }]
        }
    }
    fake_content = Mock()
    fake_content.deidentify.return_value = Mock(
        execute=Mock(return_value=deid_response))
    fake_projects = Mock(content=Mock(return_value=fake_content))
    fake_dlp = Mock(projects=Mock(return_value=fake_projects))
    mock_build_fn.return_value = fake_dlp

    self.assertRaisesRegexp(
        Exception, r'Deidentify\(\) failed: MyField: "some details"',
        run_deid_lib.deid,
        [], 'project', {}, {}, [], [], [], 'api')

  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testE2E(self, mock_bq_source_fn, mock_bq_sink_fn, mock_build_fn):
    table_to_schema = _TABLE_TO_SCHEMA.copy()
    table_to_schema['deid_tbl'] += ', field_transform_col:STRING'
    mock_bq_sink_fn.side_effect = partial(self.make_sink, table_to_schema)

    mock_bq_source_fn.return_value = beam_testutil.FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'first_name': 'Boaty', 'last_name': 'McBoatface',
         'note': 'text and PID and MORE PID',
         'patient_id': '111', 'record_number': '1',
         'field_transform_col': 'transform me!'}]

    deid_response = {'item': {'table': {
        'rows': [{'values': [{'stringValue': 'deid_resp_val'},
                             {'stringValue': 'transformed!!'}]}],
        'headers': [{'name': 'note'}, {'name': 'field_transform_col'}]
    }}}
    empty_locations = [{'recordLocation': {'tableLocation': {}}}]
    findings = {'findings': [
        {'location': {'codepointRange': {'start': '17', 'end': '25'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'PHONE_NUMBER'}},
        {'location': {'codepointRange': {'start': '9', 'end': '12'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'US_CENSUS_NAME'}},
        {'location': {'codepointRange': {'start': '9', 'end': '12'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'US_MALE_NAME'}}]}
    inspect_response = {'result': findings}
    fake_content = Mock()
    fake_content.inspect.return_value = Mock(
        execute=Mock(return_value=inspect_response))
    fake_content.deidentify.return_value = Mock(
        execute=Mock(return_value=deid_response))
    fake_projects = Mock(content=Mock(return_value=fake_content))
    fake_dlp = Mock(projects=Mock(return_value=fake_projects))
    mock_build_fn.return_value = fake_dlp

    query_job = Mock()
    rows = [['Boaty', 'McBoatface', 'note', 'id', 'recordnum', DEID_TIMESTAMP]]
    results_table = FakeBqResults(bq_schema(), rows)
    query_job.destination.fetch_data.return_value = results_table
    bq_client = Mock()
    bq_client.run_async_query.return_value = query_job

    deid_cfg = os.path.join(TESTDATA_DIR, 'testdata/config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg)
    dtd_dir = tempfile.mkdtemp()
    run_deid_lib.run_pipeline(
        'input_query', None, 'deid_tbl', 'findings_tbl',
        'gs://mae-bucket/mae-dir', 'mae_tbl', deid_cfg_json, 'InspectPhiTask',
        'project', testutil.FakeStorageClient, bq_client, None, 'dlp',
        batch_size=1, dtd_dir=dtd_dir, input_csv=None, output_csv=None,
        timestamp=DEID_TIMESTAMP, pipeline_args=None)

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
      self.assertEqual(beam_testutil.get_table('mae_tbl'),
                       [{'record_id': '111-1', 'xml': contents}])
    with open(
        os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.dtd')) as f:
      contents = f.read()
      self.assertEqual(
          testutil.get_gcs_file('mae-bucket/mae-dir/classification.dtd'),
          contents)
      with open(os.path.join(dtd_dir, 'classification.dtd')) as local_dtd:
        self.assertEqual(local_dtd.read(), contents)

    self.assertEqual(
        beam_testutil.get_table('deid_tbl'), [{
            'patient_id': '111',
            'record_number': '1',
            'note': 'deid_resp_val',
            'field_transform_col': 'transformed!!',
            run_deid_lib.DLP_DEID_TIMESTAMP: TIMESTAMP_STRING
        }])
    self.assertEqual(
        beam_testutil.get_table('findings_tbl'), [{
            'patient_id': '111',
            'record_number': '1',
            'findings': json.dumps(findings),
            run_deid_lib.DLP_FINDINGS_TIMESTAMP: TIMESTAMP_STRING
        }])

  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.textio.WriteToText')
  def testCSV(self, mock_w2t_fn, mock_build_fn):
    mock_w2t_fn.side_effect = partial(self.make_csv_output)

    deid_response = {'item': {'table': {
        'rows': [{'values': [{'stringValue': 'deid_resp_val'}]}],
        'headers': [{'name': 'note'}]
    }}}
    fake_content = Mock()
    fake_content.deidentify.return_value = Mock(
        execute=Mock(return_value=deid_response))
    fake_projects = Mock(content=Mock(return_value=fake_content))
    fake_dlp = Mock(projects=Mock(return_value=fake_projects))
    mock_build_fn.return_value = fake_dlp

    deid_cfg = os.path.join(TESTDATA_DIR, 'sample_deid_config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg)
    input_csv = os.path.join(TESTDATA_DIR, 'testdata/input.csv')
    run_deid_lib.run_pipeline(
        None,
        None,
        None,
        None,
        None,
        None,
        deid_cfg_json,
        'InspectPhiTask',
        'project',
        testutil.FakeStorageClient,
        None,
        None,
        'dlp',
        batch_size=1,
        dtd_dir=None,
        input_csv=input_csv,
        output_csv='output-csv',
        timestamp=DEID_TIMESTAMP,
        pipeline_args=None)

    fake_content.deidentify.assert_called_once()
    self.assertEqual(
        testutil.get_gcs_file('output-csv').strip(),
        '222,1,deid_resp_val,' + TIMESTAMP_STRING)

  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testMultiColumnDeid(self, mock_bq_source_fn, mock_bq_sink_fn,
                          mock_build_fn):
    table_to_schema = _TABLE_TO_SCHEMA.copy()
    table_to_schema['deid_tbl'] += ', last_name:STRING'
    mock_bq_sink_fn.side_effect = partial(self.make_sink, table_to_schema)

    mock_bq_source_fn.return_value = beam_testutil.FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'first_name': 'Boaty', 'last_name': 'McBoatface',
         'note': 'text and PID and MORE PID',
         'patient_id': '111', 'record_number': '1'}]

    deid_response = {'item': {'table': {
        'rows': [{'values': [{'stringValue': 'deidtext'},
                             {'stringValue': 'myname'}]}],
        'headers': [{'name': 'note'}, {'name': 'last_name'}]
    }}}
    empty_locations = [{'recordLocation': {'tableLocation': {}}}]
    findings = {'findings': [
        {'location': {'codepointRange': {'start': '17', 'end': '25'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'PHONE_NUMBER'}},
        {'location': {'codepointRange': {'start': '9', 'end': '12'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'US_CENSUS_NAME'}},
        {'location': {'codepointRange': {'start': '9', 'end': '12'},
                      'contentLocations': [{'recordLocation': {
                          'tableLocation': {'rowIndex': '0'}}}]},
         'infoType': {'name': 'US_MALE_NAME'}}]}
    inspect_response = {'result': findings}
    fake_content = Mock()
    fake_content.inspect.return_value = Mock(
        execute=Mock(return_value=inspect_response))
    fake_content.deidentify.return_value = Mock(
        execute=Mock(return_value=deid_response))
    fake_projects = Mock(content=Mock(return_value=fake_content))
    fake_dlp = Mock(projects=Mock(return_value=fake_projects))
    mock_build_fn.return_value = fake_dlp

    query_job = Mock()
    rows = [['Boaty', 'McBoatface', 'note', 'id', 'recordnum', DEID_TIMESTAMP]]
    results_table = FakeBqResults(bq_schema(), rows)
    query_job.destination.fetch_data.return_value = results_table
    bq_client = Mock()
    bq_client.run_async_query.return_value = query_job

    deid_cfg_file = os.path.join(TESTDATA_DIR,
                                 'testdata/multi_column_config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg_file)

    mae_dir = ''  # Not compatible with multi-column.
    mae_table = ''  # Not compatible with multi-column.
    run_deid_lib.run_pipeline(
        'input_query', None, 'deid_tbl', 'findings_tbl',
        mae_dir, mae_table, deid_cfg_json, 'InspectPhiTask',
        'project', testutil.FakeStorageClient, bq_client, None, 'dlp',
        batch_size=1, dtd_dir=None, input_csv=None, output_csv=None,
        timestamp=DEID_TIMESTAMP, pipeline_args=None)

    request_body = {}
    with open(os.path.join(
        TESTDATA_DIR, 'testdata/multi_column_request.json')) as f:
      request_body = json.load(f)
    fake_content.deidentify.assert_called_once()
    _, kwargs = fake_content.deidentify.call_args
    self.maxDiff = 10000
    self.assertEqual(ordered(request_body), ordered(kwargs['body']))

    self.assertEqual(
        beam_testutil.get_table('deid_tbl'), [{
            'patient_id': '111',
            'record_number': '1',
            'note': 'deidtext',
            'last_name': 'myname',
            run_deid_lib.DLP_DEID_TIMESTAMP: TIMESTAMP_STRING
        }])

  # De-id two notes and batch them together so each of inspect() and deid() is
  # still only called once.
  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testBatchDeid(self, mock_bq_source_fn, mock_bq_sink_fn, mock_build_fn):
    mock_bq_sink_fn.side_effect = partial(self.make_sink, _TABLE_TO_SCHEMA)

    mock_bq_source_fn.return_value = beam_testutil.FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'first_name': 'Boaty', 'last_name': 'McBoatface',
         'note': 'text and PID and MORE PID',
         'patient_id': '111', 'record_number': '1'},
        {'first_name': 'Zephod', 'last_name': 'Beeblebrox',
         'note': 'note2 text', 'patient_id': '222', 'record_number': '2'}]

    deid_response = {'item': {'table': {
        'rows': [{'values': [sval('Boaty'), sval('McBoatface'),
                             sval('note1 redacted'), sval('111'), sval('1')]},
                 {'values': [sval('Zephod'), sval('Beeblebrox'),
                             sval('note2 redacted'), sval('222'), sval('2')]}],
        'headers': DEID_HEADERS
    }}}
    findings = {'findings': [
        {'location': {'codepointRange': {'start': '9', 'end': '12'},
                      'contentLocations': [{'recordLocation': {
                          'tableLocation': {'rowIndex': '0'}}}]},
         'infoType': {'name': 'PHONE_NUMBER'}},
        {'location': {'codepointRange': {'start': '17', 'end': '25'},
                      'contentLocations': [{'recordLocation': {
                          'tableLocation': {'rowIndex': '1'}}}]},
         'infoType': {'name': 'US_MALE_NAME'}}]}
    inspect_response = {'result': findings}
    fake_content = Mock()
    fake_content.inspect.return_value = Mock(
        execute=Mock(return_value=inspect_response))
    fake_content.deidentify.return_value = Mock(
        execute=Mock(return_value=deid_response))
    fake_projects = Mock(content=Mock(return_value=fake_content))
    fake_dlp = Mock(projects=Mock(return_value=fake_projects))
    mock_build_fn.return_value = fake_dlp

    query_job = Mock()
    rows = [[
        'Boaty', 'McBoatface', 'text and PID and MORE PID', '111', '1',
        DEID_TIMESTAMP
    ], ['Zephod', 'Beeblebrox', 'note2 text', '222', '2', DEID_TIMESTAMP]]
    results_table = FakeBqResults(bq_schema(), rows)
    query_job.destination.fetch_data.return_value = results_table
    bq_client = Mock()
    bq_client.run_async_query.return_value = query_job

    deid_cfg_file = os.path.join(TESTDATA_DIR, 'testdata/batch_config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg_file)

    run_deid_lib.run_pipeline(
        'input_query', None, 'deid_tbl', 'findings_tbl',
        'gs://mae-bucket/mae-dir', 'mae_tbl', deid_cfg_json, 'InspectPhiTask',
        'project', testutil.FakeStorageClient, bq_client, None, 'dlp',
        batch_size=2, dtd_dir=None, input_csv=None, output_csv=None,
        timestamp=DEID_TIMESTAMP, pipeline_args=None)

    expected_request_body = {}
    with open(os.path.join(TESTDATA_DIR, 'testdata/batch_request.json')) as f:
      expected_request_body = json.load(f)
    fake_content.deidentify.assert_called_once()
    _, kwargs = fake_content.deidentify.call_args
    self.maxDiff = 10000
    self.assertEqual(ordered(expected_request_body), ordered(kwargs['body']))

    self.assertEqual(beam_testutil.get_table('deid_tbl'), EXPECTED_DEID_RESULT)
    self.assertEqual(EXPECTED_MAE1,
                     testutil.get_gcs_file('mae-bucket/mae-dir/111-1.xml'))
    self.assertEqual(EXPECTED_MAE2,
                     testutil.get_gcs_file('mae-bucket/mae-dir/222-2.xml'))

  # De-id two notes and batch them together so each of inspect() and deid() is
  # still only called once.
  # However, there are too many findings for a single request, so we re-try the
  # request as two separate requests.
  @patch('apiclient.discovery.build')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testReBatchDeid(self, mock_bq_source_fn, mock_bq_sink_fn, mock_build_fn):
    mock_bq_sink_fn.side_effect = partial(self.make_sink, _TABLE_TO_SCHEMA)

    mock_bq_source_fn.return_value = beam_testutil.FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'first_name': 'Boaty', 'last_name': 'McBoatface',
         'note': 'text and PID and MORE PID',
         'patient_id': '111', 'record_number': '1'},
        {'first_name': 'Zephod', 'last_name': 'Beeblebrox',
         'note': 'note2 text', 'patient_id': '222', 'record_number': '2'}]

    deid_response1 = {'item': {'table': {
        'rows': [{'values': [sval('Boaty'), sval('McBoatface'),
                             sval('note1 redacted'), sval('111'), sval('1')]}],
        'headers': DEID_HEADERS}}}
    deid_response2 = {'item': {'table': {
        'rows': [{'values': [sval('Zephod'), sval('Beeblebrox'),
                             sval('note2 redacted'), sval('222'), sval('2')]}],
        'headers': DEID_HEADERS}}}

    empty_locations = [{'recordLocation': {'tableLocation': {}}}]
    findings1 = {'findings': [
        {'location': {'codepointRange': {'start': '9', 'end': '12'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'PHONE_NUMBER'}}]}
    findings2 = {'findings': [
        {'location': {'codepointRange': {'start': '17', 'end': '25'},
                      'contentLocations': empty_locations},
         'infoType': {'name': 'US_MALE_NAME'}}]}
    inspect_response_truncated = {'result': {'findingsTruncated': 'True'}}
    inspect_responses = [inspect_response_truncated, {'result': findings1},
                         {'result': findings2}]
    def inspect_execute():
      response = inspect_responses[inspect_execute.call_count]
      inspect_execute.call_count += 1
      return response
    inspect_execute.call_count = 0
    fake_content = Mock()
    fake_content.inspect.return_value = Mock(execute=inspect_execute)

    deid_responses = ['Exception', deid_response1, deid_response2]
    def deid_execute():
      response = deid_responses[deid_execute.call_count]
      deid_execute.call_count += 1
      if response == 'Exception':
        content = ('{"error": {"message": "Too many findings to de-identify. '
                   'Retry with a smaller request."}}').encode('utf-8')
        raise errors.HttpError(httplib2.Response({'status': 400}), content)
      return response
    deid_execute.call_count = 0
    fake_content.deidentify.return_value = Mock(execute=deid_execute)

    fake_projects = Mock(content=Mock(return_value=fake_content))
    fake_dlp = Mock(projects=Mock(return_value=fake_projects))
    mock_build_fn.return_value = fake_dlp

    query_job = Mock()
    rows = [[
        'Boaty', 'McBoatface', 'text and PID and MORE PID', '111', '1',
        DEID_TIMESTAMP
    ], ['Zephod', 'Beeblebrox', 'note2 text', '222', '2', DEID_TIMESTAMP]]
    results_table = FakeBqResults(bq_schema(), rows)
    query_job.destination.fetch_data.return_value = results_table
    bq_client = Mock()
    bq_client.run_async_query.return_value = query_job

    deid_cfg_file = os.path.join(TESTDATA_DIR, 'testdata/batch_config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg_file)

    run_deid_lib.run_pipeline(
        'input_query', None, 'deid_tbl', 'findings_tbl',
        'gs://mae-bucket/mae-dir', 'mae_tbl', deid_cfg_json, 'InspectPhiTask',
        'project', testutil.FakeStorageClient, bq_client, None, 'dlp',
        batch_size=2, dtd_dir=None, input_csv=None, output_csv=None,
        timestamp=DEID_TIMESTAMP, pipeline_args=None)

    expected_request_body = {}
    with open(os.path.join(TESTDATA_DIR, 'testdata/batch_request.json')) as f:
      expected_request_body = json.load(f)
    fake_content.deidentify.assert_called()
    _, kwargs = fake_content.deidentify.call_args_list[0]
    self.maxDiff = 10000
    self.assertEqual(ordered(expected_request_body), ordered(kwargs['body']))

    self.assertEqual(beam_testutil.get_table('deid_tbl'), EXPECTED_DEID_RESULT)
    self.assertEqual(EXPECTED_MAE1,
                     testutil.get_gcs_file('mae-bucket/mae-dir/111-1.xml'))
    self.assertEqual(EXPECTED_MAE2,
                     testutil.get_gcs_file('mae-bucket/mae-dir/222-2.xml'))

  def testGenerateSchema(self):
    self.assertEqual(
        'str:STRING, int:INTEGER, int2:INTEGER, float:FLOAT, bool:BOOLEAN',
        run_deid_lib._generate_schema([
            {'name': 'str', 'type': 'stringValue'},
            {'name': 'int', 'type': 'integerValue'},
            {'name': 'int2', 'type': 'integerValue'},
            {'name': 'float', 'type': 'floatValue'},
            {'name': 'bool', 'type': 'booleanValue'}]))

    self.assertEqual(
        'str:STRING',
        run_deid_lib._generate_schema([{'name': 'str', 'type': 'stringValue'}]))

  def testGenerateDeidConfig(self):
    info_type_transformations = [
        {'infoTypes': [{'name': 'NAME'}],
         'primitiveTransformation': {'replaceWithInfoTypeConfig': {}}},
        {'infoTypes': [{'name': 'AGE'}, {'name': 'DATE'}],
         'primitiveTransformation': {'otherConfig': {}}}
    ]
    field_transformations = [
        {'fields': [{'name': 'field1'}, {'name': 'field2'}],
         'primitiveTransformation': {'aTransformConfig': {}}},
        {'fields': [{'name': 'field3'}],
         'primitiveTransformation': {'otherConfig': {}}}
    ]
    target_columns = [
        {'name': 'patient_name', 'type': 'stringValue'},
        {'name': 'date', 'type': 'stringValue', 'infoTypesToDeId': ['DATE']}]

    gen = run_deid_lib._generate_deid_config(
        info_type_transformations, target_columns, field_transformations)

    expected = {'recordTransformations': {
        'fieldTransformations': field_transformations + [
            {'fields': [{'name': 'date'}],
             'infoTypeTransformations': {'transformations': [
                 {'infoTypes': [{'name': 'DATE'}],
                  'primitiveTransformation': {'otherConfig': {}}}]}},
            {'fields': [{'name': 'patient_name'}],
             'infoTypeTransformations': {'transformations': [
                 {'infoTypes': [{'name': 'NAME'}],
                  'primitiveTransformation': {'replaceWithInfoTypeConfig': {}}},
                 {'infoTypes': [{'name': 'AGE'}, {'name': 'DATE'}],
                  'primitiveTransformation': {'otherConfig': {}}}]}
            }
        ]}}

    self.maxDiff = 3000
    self.assertEqual(gen, expected)

  def testGenerateDtdLocal(self):
    deid_cfg = os.path.join(TESTDATA_DIR, 'testdata/config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg)
    dtd_dir = tempfile.mkdtemp()
    run_deid_lib.run_pipeline(
        None, None, None, None, None, None, deid_cfg_json, 'InspectPhiTask',
        'project', testutil.FakeStorageClient, None, None, 'dlp',
        batch_size=1, dtd_dir=dtd_dir, input_csv=None, output_csv=None,
        timestamp=None, pipeline_args=None)

    with open(
        os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.dtd')) as f:
      with open(os.path.join(dtd_dir, 'classification.dtd')) as generated_dtd:
        self.assertEqual(generated_dtd.read(), f.read())

  def testGenerateDtdGcs(self):
    deid_cfg = os.path.join(TESTDATA_DIR, 'testdata/config.json')
    deid_cfg_json = run_deid_lib.parse_config_file(deid_cfg)
    dtd_dir = 'gs://dtd-dir'
    run_deid_lib.run_pipeline(
        None, None, None, None, None, None, deid_cfg_json, 'InspectPhiTask',
        'project', testutil.FakeStorageClient, None, None, 'dlp',
        batch_size=1, dtd_dir=dtd_dir, input_csv=None, output_csv=None,
        timestamp=None, pipeline_args=None)

    with open(
        os.path.join(TESTDATA_DIR, 'mae_testdata', 'sample.dtd')) as f:
      self.assertEqual(testutil.get_gcs_file('dtd-dir/classification.dtd'),
                       f.read())


if __name__ == '__main__':
  unittest.main()
