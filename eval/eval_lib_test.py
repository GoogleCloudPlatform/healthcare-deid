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

"""Tests for google3.eval.eval_lib."""

from __future__ import absolute_import

import math
import unittest

from eval import eval_lib
from eval import results_pb2

from google.protobuf import descriptor

TESTDATA_DIR = 'eval/testdata/'

xml_template = """<?xml version="1.0" encoding="UTF-8" ?>
<InspectPhiTask>
<TEXT><![CDATA[word1   w2 w3  wrd4 5 word6   word7 multi token entity w8]]></TEXT>
<TAGS>
{0}
</TAGS></InspectPhiTask>
"""

tag_template = '<{0} id="{0}0" spans="{1}~{2}" />'


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
        for v in values.itervalues():
          normalize_floats(v)
    elif desc.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
      for v in values:
        normalize_floats(v)
  return pb


class EvalLibTest(unittest.TestCase):

  def testHMean(self):
    self.assertAlmostEqual(2.0, eval_lib.hmean(1, 2, 4, 4))
    self.assertTrue(math.isnan(eval_lib.hmean(0, 1, 2)))

  def testCalculateStats(self):
    stats = results_pb2.Stats()
    stats.true_positives = 12
    stats.false_positives = 8
    stats.false_negatives = 3
    eval_lib.calculate_stats(stats)
    self.assertAlmostEqual(.6, stats.precision)
    self.assertAlmostEqual(.8, stats.recall)
    self.assertAlmostEqual(.6857142857142856, stats.f_score)

    stats = results_pb2.Stats()
    eval_lib.calculate_stats(stats)
    self.assertTrue(math.isnan(stats.precision))
    self.assertTrue(math.isnan(stats.recall))
    self.assertTrue(math.isnan(stats.f_score))
    self.assertEqual(
        'Precision has denominator of zero. Recall has denominator of zero. '
        'f-score is NaN',
        stats.error_message)

  def testMacroStats(self):
    macro_stats = eval_lib._MacroStats()
    macro_stats.count = 50
    macro_stats.precision_sum = 40
    macro_stats.recall_sum = 45

    stats = macro_stats.calculate_stats()
    self.assertAlmostEqual(.8, stats.precision)
    self.assertAlmostEqual(.9, stats.recall)
    self.assertAlmostEqual(.8470588235294118, stats.f_score)

    macro_stats = eval_lib._MacroStats()
    stats = macro_stats.calculate_stats()
    self.assertTrue(math.isnan(stats.precision))
    self.assertTrue(math.isnan(stats.recall))
    self.assertTrue(math.isnan(stats.f_score))

  def testTokenizeSet(self):
    finding = eval_lib.Finding
    full_text = 'hi: two tokens and: three\tmore  tokens'
    findings = [finding('TYPE_A', 0, 2, 'hi', 0, full_text),
                finding('TYPE_B', 4, 14, 'two tokens', 0, full_text),
                finding('TYPE_C', 20, 38, 'three\tmore  tokens', 0, full_text),
                # 'tokens' is a duplicate, so it's not added to the results.
                finding('TYPE_D', 32, 46, 'tokens overlap')]
    tokenized = eval_lib.tokenize_set(findings)
    expected_tokenized = set([finding('TYPE_A', 0, 2, 'hi'),
                              finding('TYPE_B', 4, 7, 'two'),
                              finding('TYPE_B', 8, 14, 'tokens'),
                              finding('TYPE_C', 20, 25, 'three'),
                              finding('TYPE_C', 26, 30, 'more'),
                              finding('TYPE_C', 32, 38, 'tokens'),
                              finding('TYPE_D', 39, 46, 'overlap')])
    self.assertEqual(expected_tokenized, tokenized)

  def testContext(self):
    full_text = 'small sample text sentence'
    f = eval_lib.Finding.from_tag('TYPE_A', '6~17', full_text)
    self.assertEqual('small {[--sample text--]} sentence', f.context())

    findings = sorted(eval_lib.tokenize_finding(f), key=str)
    self.assertEqual('small {[--sample--]} text sentence',
                     findings[1].context())
    self.assertEqual('small sample {[--text--]} sentence',
                     findings[0].context())

  def testLooseMatching(self):
    finding = eval_lib.Finding
    findings = set([finding('TYPE_A', 0, 3, 'one'),
                    finding('TYPE_B', 5, 8, 'two'),
                    finding('TYPE_C', 20, 25, 'three')])
    golden_findings = set([finding('TYPE_A', 0, 3, 'hit'),
                           finding('TYPE_B', 7, 10, 'hit'),
                           finding('TYPE_C', 25, 29, 'miss')])
    result = eval_lib.count_matches(
        findings, golden_findings, record_id='', strict=False)

    expected_stats = results_pb2.Stats()
    expected_stats.true_positives = 2
    expected_stats.false_positives = 1
    expected_stats.false_negatives = 1
    expected_stats.precision = .66666667
    expected_stats.recall = .66666667
    expected_stats.f_score = .66666667
    self.assertEqual(normalize_floats(expected_stats),
                     normalize_floats(result.stats))

    a = results_pb2.Stats()
    a.true_positives = 1
    b = results_pb2.Stats()
    b.true_positives = 1
    c = results_pb2.Stats()
    c.false_positives = 1
    c.false_negatives = 1
    expected_per_type = {'TYPE_A': a, 'TYPE_B': b, 'TYPE_C': c}
    self.assertEqual(expected_per_type, result.per_type)

  def testInvalidSpans(self):
    with self.assertRaises(Exception):
      eval_lib.Finding.from_tag('invalid', '4~2', 'full text')
    with self.assertRaises(Exception):
      eval_lib.Finding.from_tag('out_of_range', '0~10', 'full text')

if __name__ == '__main__':
  unittest.main()
