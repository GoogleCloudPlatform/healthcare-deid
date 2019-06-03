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

"""Tests for google3.eval.run_pipeline_lib."""

from __future__ import absolute_import

import math
import os
import unittest

import apache_beam as beam
from common import beam_testutil
from common import testutil
from eval import results_pb2
from eval import run_pipeline_lib
from mock import patch

from google.protobuf import descriptor
from google.protobuf import text_format

TESTDATA_DIR = 'eval/testdata/'

xml_template = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[word1   w2 w3  wrd4 5 word6   word7 multi token entity w8]]></TEXT>
<TAGS>
{0}
</TAGS></InspectPhiTask>
"""

tag_template = '<{0} id="{0}0" spans="{1}~{2}" />'


def normalize_dict_floats(d):
  for k, v in d.items():
    if isinstance(v, float):
      d[k] = round(v, 6)
  return d


def normalize_floats(pb):
  for desc, values in pb.ListFields():
    is_repeated = True
    if desc.label is not descriptor.FieldDescriptor.LABEL_REPEATED:
      is_repeated = False
      values = [values]

    normalized_values = None
    if desc.type == descriptor.FieldDescriptor.TYPE_FLOAT:
      normalized_values = []
      for x in values:
        if math.isnan(x):
          # NaN != NaN, so use infinity to make equality comparisons work.
          normalized_values.append(float('inf'))
        else:
          normalized_values.append(round(x, 6))

    if normalized_values is not None:
      if is_repeated:
        pb.ClearField(desc.name)
        getattr(pb, desc.name).extend(normalized_values)
      else:
        setattr(pb, desc.name, normalized_values[0])

    if (desc.type == descriptor.FieldDescriptor.TYPE_MESSAGE and
        desc.message_type.has_options and
        desc.message_type.GetOptions().map_entry):
      # This is a map; only recurse if the values have a message type.
      if (desc.message_type.fields_by_number[2].type ==
          descriptor.FieldDescriptor.TYPE_MESSAGE):
        for v in values.values():
          normalize_floats(v)
    elif desc.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
      for v in values:
        normalize_floats(v)
  return pb


class RunPipelineLibTest(unittest.TestCase):

  @patch('eval.run_pipeline_lib._get_utcnow')
  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testE2eBigquery(self, mock_bq_source_fn, mock_bq_sink_fn, mock_utcnow_fn):
    def make_sink(table_name, schema, write_disposition):  # pylint: disable=unused-argument
      return beam_testutil.FakeSink(table_name)
    mock_bq_sink_fn.side_effect = make_sink
    now = 'current time'
    mock_utcnow_fn.return_value = now

    tp_tag = tag_template.format('TypeA', 0, 5)
    fp_tag = tag_template.format('TypeA', 8, 10)
    fn_tag = tag_template.format('TypeA', 11, 13)
    fn2_tag = tag_template.format('TypeA', 15, 19)
    findings_tags = '\n'.join([tp_tag, fp_tag])
    golden_tags = '\n'.join([tp_tag, fn_tag, fn2_tag])

    mock_bq_source_fn.return_value = beam_testutil.FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'findings_record_id': '111-1',
         'findings_xml': xml_template.format(findings_tags),
         'golden_xml': xml_template.format(golden_tags)}]

    types_to_ignore = ['ignore']
    # These features are tested in testE2eGCS.
    input_pattern, golden_dir, results_dir, per_note_table, debug_table = (
        None, None, None, None, None)
    mae_input_query = 'SELECT * from [project.dataset.table]'
    mae_golden_table = 'project.dataset.golden_table'
    run_pipeline_lib.run_pipeline(
        input_pattern, golden_dir, results_dir, mae_input_query,
        mae_golden_table, False, 'results_table', per_note_table, debug_table,
        types_to_ignore, timestamp=None, pipeline_args=None)

    # Check that we generated the query correctly.
    mock_bq_source_fn.assert_called_with(query=(
        'SELECT findings.record_id, findings.xml, golden.xml FROM '
        '(SELECT * from [project.dataset.table]) AS findings '
        'LEFT JOIN [project.dataset.golden_table] AS golden '
        'ON findings.record_id=golden.record_id'))

    # Check we wrote the correct results to BigQuery.
    expected_results = [
        {'info_type': 'ALL', 'recall': 0.333333, 'precision': 0.5,
         'f_score': 0.4, 'true_positives': 1,
         'false_positives': 1, 'false_negatives': 2},
        {'info_type': u'TypeA', 'recall': 0.333333,
         'precision': 0.5, 'f_score': 0.4,
         'true_positives': 1, 'false_positives': 1, 'false_negatives': 2}]
    for r in expected_results:
      r.update({'timestamp': now})
    actual_results = sorted(beam_testutil.get_table('results_table'),
                            key=lambda x: x['info_type'])
    self.assertEqual([normalize_dict_floats(r) for r in expected_results],
                     [normalize_dict_floats(r) for r in actual_results])

  @patch('apache_beam.io.BigQuerySink')
  @patch('apache_beam.io.BigQuerySource')
  def testNonMatchingNotes(self, mock_bq_source_fn, mock_bq_sink_fn):
    # Pipeline raises an exception if the note text does not match the golden
    # note text.

    def make_sink(table_name, schema, write_disposition):  # pylint: disable=unused-argument
      return beam_testutil.FakeSink(table_name)
    mock_bq_sink_fn.side_effect = make_sink

    xml_text_template = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[{0}]]></TEXT>
<TAGS></TAGS></InspectPhiTask>
"""

    mock_bq_source_fn.return_value = beam_testutil.FakeSource()
    mock_bq_source_fn.return_value._records = [
        {'findings_record_id': '111-1',
         'findings_xml': xml_text_template.format('some text'),
         'golden_xml': xml_text_template.format('different text')}]

    (input_pattern, golden_dir, results_dir, per_note_table, debug_table,
     types_to_ignore, pipeline_args) = None, None, None, None, None, None, None
    mae_input_query = 'SELECT * from [project.dataset.table]'
    mae_golden_table = 'project.dataset.golden_table'
    self.assertRaisesRegexp(
        Exception, 'Note text is different from golden for record \"111-1\"',
        run_pipeline_lib.run_pipeline,
        input_pattern, golden_dir, results_dir, mae_input_query,
        mae_golden_table, False, 'results_table', per_note_table, debug_table,
        types_to_ignore, None, pipeline_args)

  @patch('eval.run_pipeline_lib._get_utcnow')
  @patch('apache_beam.io.BigQuerySink')
  @patch('google.cloud.storage.Client')
  def testE2eGCS(self, fake_client_fn, mock_bq_sink_fn, mock_utcnow_fn):
    def make_sink(table_name, schema, write_disposition):  # pylint: disable=unused-argument
      return beam_testutil.FakeSink(table_name)
    mock_bq_sink_fn.side_effect = make_sink
    now = 'current time'
    mock_utcnow_fn.return_value = now

    input_pattern = 'gs://bucketname/input/*'
    golden_dir = 'gs://bucketname/goldens'
    results_dir = 'gs://bucketname/results'
    storage_client = testutil.FakeStorageClient()
    fake_client_fn.return_value = storage_client

    tp_tag = tag_template.format('TypeA', 0, 5)
    fp_tag = tag_template.format('TypeA', 8, 10)
    fn_tag = tag_template.format('TypeA', 11, 13)
    fn2_tag = tag_template.format('TypeA', 15, 19)
    findings_tags = '\n'.join([tp_tag, fp_tag])
    golden_tags = '\n'.join([tp_tag, fn_tag, fn2_tag])
    testutil.set_gcs_file('bucketname/input/1-1.xml',
                          xml_template.format(findings_tags))
    testutil.set_gcs_file('bucketname/goldens/1-1.xml',
                          xml_template.format(golden_tags))

    tp2_tag = tag_template.format('TypeB', 20, 21)
    # False negative + false positive for entity matching, but true positive for
    # binary token matching.
    entity_fp_tag = tag_template.format('TypeX', 30, 35)
    entity_fn_tag = tag_template.format('TypeY', 30, 35)
    # Two tokens are tagged as one in the golden. This is not a match for entity
    # matching, but is two matches for binary token matching.
    partial_tag1 = tag_template.format('TypeA', 36, 41)
    partial_tag2 = tag_template.format('TypeA', 42, 47)
    partial_tag3 = tag_template.format('TypeA', 48, 54)
    multi_token_tag = tag_template.format('TypeA', 36, 54)
    ignored_tag = tag_template.format('ignore', 55, 57)
    findings_tags = '\n'.join([tp_tag, tp2_tag, entity_fp_tag, partial_tag1,
                               partial_tag2, partial_tag3, ignored_tag])
    golden_tags = '\n'.join([tp_tag, tp2_tag, entity_fn_tag, multi_token_tag])
    testutil.set_gcs_file('bucketname/input/1-2.xml',
                          xml_template.format(findings_tags))
    testutil.set_gcs_file('bucketname/goldens/1-2.xml',
                          xml_template.format(golden_tags))
    self.old_write_to_text = beam.io.WriteToText
    beam.io.WriteToText = beam_testutil.DummyWriteTransform
    types_to_ignore = ['ignore']
    mae_input_query = None
    mae_golden_table = None
    run_pipeline_lib.run_pipeline(input_pattern, golden_dir, results_dir,
                                  mae_input_query, mae_golden_table, True,
                                  'results_table', 'per_note_results_table',
                                  'debug_output_table', types_to_ignore, None,
                                  pipeline_args=None)
    beam.io.WriteToText = self.old_write_to_text

    # Check we wrote the correct results to BigQuery.
    expected_results = [
        {'info_type': 'ALL', 'recall': 0.7777777777777778, 'precision': 0.875,
         'f_score': 0.823529411764706, 'true_positives': 7,
         'false_positives': 1, 'false_negatives': 2},
        {'info_type': u'TypeA', 'recall': 0.7142857142857143,
         'precision': 0.8333333333333334, 'f_score': 0.7692307692307694,
         'true_positives': 5, 'false_positives': 1, 'false_negatives': 2},
        {'info_type': u'TypeB', 'recall': 1.0, 'precision': 1.0, 'f_score': 1.0,
         'true_positives': 1, 'false_positives': 0, 'false_negatives': 0},
        {'info_type': u'TypeY', 'recall': 1.0, 'precision': 1.0, 'f_score': 1.0,
         'true_positives': 1, 'false_positives': 0, 'false_negatives': 0}]
    for r in expected_results:
      r.update({'timestamp': now})
    actual_results = sorted(beam_testutil.get_table('results_table'),
                            key=lambda x: x['info_type'])
    self.assertEqual([normalize_dict_floats(r) for r in expected_results],
                     [normalize_dict_floats(r) for r in actual_results])

    full_text = 'word1   w2 w3  wrd4 5 word6   word7 multi token entity w8'
    def debug_info(record_id, classification, text, info_type, start, end):
      location = full_text.find(text)
      context = (full_text[0:location] + '{[--' + text + '--]}' +
                 full_text[location + len(text):])
      return {'record_id': record_id, 'classification': classification,
              'text': text, 'info_type': info_type, 'context': context,
              'start': start, 'end': end}
    expected_debug_info = [
        debug_info('1-1', 'true_positive', 'word1', 'TypeA', 0, 5),
        debug_info('1-1', 'false_positive', 'w2', 'TypeA', 8, 10),
        debug_info('1-1', 'false_negative', 'w3', 'TypeA', 11, 13),
        debug_info('1-1', 'false_negative', 'wrd4', 'TypeA', 15, 19),
        debug_info('1-2', 'true_positive', 'word1', 'TypeA', 0, 5),
        debug_info('1-2', 'true_positive', '5', 'TypeB', 20, 21),
        debug_info('1-2', 'true_positive', 'word7', 'TypeY', 30, 35),
        debug_info('1-2', 'true_positive', 'multi', 'TypeA', 36, 41),
        debug_info('1-2', 'true_positive', 'token', 'TypeA', 42, 47),
        debug_info('1-2', 'true_positive', 'entity', 'TypeA', 48, 54),
    ]
    for r in expected_debug_info:
      r.update({'timestamp': now})
    def s(l):
      return sorted(l, key=lambda x: x['record_id'] + x['context'])
    self.assertEqual(s(expected_debug_info),
                     s(beam_testutil.get_table('debug_output_table')))

    expected_per_note = [
        {'record_id': '1-1', 'precision': 0.5, 'recall': 0.3333333333333333,
         'f_score': 0.4, 'true_positives': 1, 'false_positives': 1,
         'false_negatives': 2},
        {'record_id': '1-2', 'precision': 1.0, 'recall': 1.0, 'f_score': 1.0,
         'true_positives': 6, 'false_positives': 0, 'false_negatives': 0}]
    for r in expected_per_note:
      r.update({'timestamp': now})
    actual_results = sorted(beam_testutil.get_table('per_note_results_table'),
                            key=lambda x: x['record_id'])
    self.assertEqual([normalize_dict_floats(r) for r in expected_per_note],
                     [normalize_dict_floats(r) for r in actual_results])

    # Check we wrote the correct results to GCS.
    expected_text = ''
    with open(os.path.join(TESTDATA_DIR, 'expected_results')) as f:
      expected_text = f.read()
    expected_results = results_pb2.Results()
    text_format.Merge(expected_text, expected_results)
    results = results_pb2.Results()
    text_format.Merge(
        testutil.get_gcs_file('bucketname/results/aggregate_results.txt'),
        results)
    self.assertEqual(normalize_floats(expected_results),
                     normalize_floats(results))

    # Check the per-file results were written correctly.
    expected_result1 = results_pb2.IndividualResult()
    text_format.Merge("""
record_id: "1-1"
stats {
  true_positives: 1
  false_positives: 1
  false_negatives: 2
  precision: 0.5
  recall: 0.333333333333
  f_score: 0.4
}""",
                      expected_result1)
    expected_result2 = results_pb2.IndividualResult()
    text_format.Merge("""
record_id: "1-2"
stats {
  true_positives: 6
  precision: 1.0
  recall: 1.0
  f_score: 1.0
}""",
                      expected_result2)
    normalize_floats(expected_result1)
    normalize_floats(expected_result2)
    full_text = testutil.get_gcs_file('bucketname/results/per-note-results')
    actual_results = []
    for record in sorted(full_text.split('\n\n')):
      if not record:
        continue
      actual_result = results_pb2.IndividualResult()
      text_format.Merge(record, actual_result)
      actual_results.append(normalize_floats(actual_result))

    self.assertEqual([expected_result1, expected_result2],
                     actual_results)

  def testCompareFindings(self):
    # Test that we don't throw an exception when there is a difference of one
    # trailing character.
    run_pipeline_lib.compare_findings([], [], 'id', 'note text', 'note text\n')
    run_pipeline_lib.compare_findings([], [], 'id', 'note text\n', 'note text')
    self.assertRaisesRegexp(
        Exception, 'Note text is different from golden for record \"123\"',
        run_pipeline_lib.compare_findings, [], [], '123', 'note text',
        'some other note text')
    self.assertRaisesRegexp(
        Exception, 'Note text is different from golden for record \"123\"',
        run_pipeline_lib.compare_findings, [], [], '123', 'note text',
        'note text for longer note')

if __name__ == '__main__':
  unittest.main()
