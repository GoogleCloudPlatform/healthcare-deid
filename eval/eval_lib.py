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

"""Evaluate DeID findings on Google Cloud."""

from __future__ import absolute_import

import collections
import itertools
import logging
import math

from eval import results_pb2


class Finding(object):
  """Class to hold category, span, and text of a PHI finding."""

  def __init__(self, category, start, end, text, context_start=0, context=''):
    self.category = category
    self.start = start
    self.end = end
    self.text = text
    self.context_start = context_start
    self.raw_context = context

  @classmethod
  def from_tag(cls, category, spans, full_text):
    """Initialize a Finding from MAE tag data (plus the full text)."""
    startstr, endstr = spans.split('~')
    start = int(startstr)
    end = int(endstr)
    if start >= end:
      raise Exception('Invalid span "{}"'.format(spans))
    if end > len(full_text):
      raise Exception('Span "{}" out of range (0-{}).'.format(
          spans, len(full_text)))
    # Store up to 100 characters of context on either side of the token.
    context_start = max(0, start-100)
    token_len = end - start
    context_end = context_start + 200 + token_len
    context = full_text[context_start:context_end]
    return cls(category, start, end, full_text[start:end], context_start,
               context)

  def has_same_indices_as_any_of(self, findings):
    """Returns True if self has the same range as any of the given Findings."""
    for finding in findings:
      if self.start == finding.start and self.end == finding.end:
        return True
    return False

  def intersects(self, findings):
    """Return True if self overlaps with any of the given list of Findings."""
    for finding in findings:
      if ((self.start <= finding.start and self.end > finding.start) or
          (finding.start <= self.start and finding.end > self.start)):
        return True
    return False

  def intersects_with_category(self, findings):
    """Returns True if self intersects with a Finding of the same category."""
    for finding in findings:
      if (((self.start <= finding.start and self.end > finding.start) or
           (finding.start <= self.start and finding.end > self.start)) and
          finding.category == self.category):
        return True
    return False

  def context(self):
    """Return the context with the token made more visible."""
    relative_start = self.start - self.context_start
    context_first_half = self.raw_context[:relative_start]
    context_second_half = self.raw_context[relative_start+len(self.text):]
    return (context_first_half + '{[--' + self.text + '--]}' +
            context_second_half)

  def __hash__(self):
    return hash((self.category, self.start, self.end))

  def __eq__(self, other):
    return (self.category == other.category and self.start == other.start and
            self.end == other.end)

  def __repr__(self):
    return '{}: {}~{} "{}" ({})'.format(
        self.category, self.start, self.end, self.text, self.context())


def hmean(*args):
  """Calculate the harmonic mean of the given values.

  http://en.wikipedia.org/wiki/Harmonic_mean

  Args:
    *args: List of numbers to take the harmonic mean of.
  Returns:
    Harmonic mean of args, or NaN if an arg is <= 0.
  """
  for val in args:
    if val <= 0:
      return float('NaN')
  return len(args) / sum(1. / val for val in args)


def calculate_stats(stats):
  """Calculate derived stats and put them into the given results_pb2.Stats."""
  stats.error_message = ''
  if stats.true_positives + stats.false_positives:
    stats.precision = (float(stats.true_positives) /
                       (stats.true_positives + stats.false_positives))
  else:
    stats.precision = float('NaN')
    stats.error_message += 'Precision has denominator of zero. '

  if stats.true_positives + stats.false_negatives:
    stats.recall = (float(stats.true_positives) /
                    (stats.true_positives + stats.false_negatives))
  else:
    stats.recall = float('NaN')
    stats.error_message += 'Recall has denominator of zero. '

  stats.f_score = hmean(stats.precision, stats.recall)
  if math.isnan(stats.f_score):
    stats.error_message += 'f-score is NaN'

  return stats


def tokenize_finding(finding):
  """Turn the finding into multiple findings split by whitespace."""
  tokenized = set()
  tokens = finding.text.split()
  cursor = 0
  # Note that finding.start and finding.end refer to the location in the overall
  # text, but finding.text is just the text for this finding.
  for token in tokens:
    start = finding.text.find(token, cursor)
    cursor = end = start + len(token)
    tokenized.add(Finding(
        finding.category, start + finding.start, end + finding.start, token,
        finding.context_start, finding.raw_context))
  return tokenized


def tokenize_set(findings):
  """Split the findings on whitespace and return the tokenized set."""
  all_tokenized = set()
  findings = list(findings)
  for f in findings:
    for tokenized in tokenize_finding(f):
      # Add to all_tokenized, unless there's already an equivalent finding (i.e.
      # one with the same start and end).
      found = False
      for existing_finding in all_tokenized:
        if (existing_finding.start == tokenized.start and
            existing_finding.end == tokenized.end):
          found = True
      if not found:
        all_tokenized.add(tokenized)

  return all_tokenized


def _deserialize_individual_result(record_id, serialized_stats,
                                   per_type_serialized_stats,
                                   serialized_typeless):
  ir = IndividualResult()
  ir.record_id = record_id
  ir.stats.ParseFromString(serialized_stats)
  for type_name, stats in per_type_serialized_stats.items():
    ir.per_type[type_name].ParseFromString(stats)
  ir.typeless.ParseFromString(serialized_typeless)

  return ir


class IndividualResult(object):
  """An individual record results."""

  def __init__(self):
    self.record_id = ''
    self.stats = results_pb2.Stats()
    self.per_type = collections.defaultdict(results_pb2.Stats)
    self.typeless = results_pb2.Stats()
    self.debug_info = []

  # Dataflow's pickling gets confused if it has to deal with raw protos, so we
  # serialize them here.
  def __reduce__(self):
    per_type_serialized = {}
    for type_name, stats in self.per_type.items():
      per_type_serialized[type_name] = stats.SerializeToString()
    return (_deserialize_individual_result, (self.record_id,
                                             self.stats.SerializeToString(),
                                             per_type_serialized,
                                             self.typeless.SerializeToString()))


def count_matches(findings, golden_findings, record_id, strict, ignore_type):
  """Calculate the true/false positive/negatives for the given findings.

  Args:
    findings: List of Finding objects to count matches for.
    golden_findings: List of Finding objects to compare against.
    record_id: str; Unique identifier for this set of findings.
    strict: bool; If True, use strict matching, i.e. it's only a match if the
      type matches and the text is exactly the same. Otherwise, two findings
      match if they have at least one character in common.
    ignore_type: bool; If True, ignore type when calculating the per-type
      results.

  Returns:
    An IndividualResult object containing the counts and derived stats.
  """
  result = IndividualResult()
  result.record_id = record_id
  for finding in findings:
    if ((strict and finding not in golden_findings) or
        (not strict and not finding.intersects_with_category(golden_findings))):
      result.stats.false_positives += 1
      if strict:
        result.debug_info.append({
            'record_id': record_id,
            'classification': 'false_positive',
            'text': finding.text,
            'context': finding.context(),
            'info_type': finding.category,
            'start': finding.start,
            'end': finding.end
        })
      if not ignore_type:
        result.per_type[finding.category].false_positives += 1

    if ((strict and not finding.has_same_indices_as_any_of(golden_findings)) or
        (not strict and not finding.intersects(golden_findings))):
      result.typeless.false_positives += 1
      if not strict:
        result.debug_info.append({
            'record_id': record_id,
            'classification': 'false_positive',
            'text': finding.text,
            'context': finding.context(),
            'info_type': finding.category,
            'start': finding.start,
            'end': finding.end
        })
      if ignore_type:
        result.per_type[finding.category].false_positives += 1

  for golden_finding in golden_findings:
    if ((strict and golden_finding in findings) or
        (not strict and golden_finding.intersects_with_category(findings))):
      result.stats.true_positives += 1
      if not ignore_type:
        result.per_type[golden_finding.category].true_positives += 1
    else:
      if strict:
        result.debug_info.append({
            'record_id': record_id,
            'classification': 'false_negative',
            'text': golden_finding.text,
            'context': golden_finding.context(),
            'info_type': golden_finding.category
        })
      if not ignore_type:
        result.per_type[golden_finding.category].false_negatives += 1
      result.stats.false_negatives += 1

    if ((strict and golden_finding.has_same_indices_as_any_of(findings)) or
        (not strict and golden_finding.intersects(findings))):
      if not strict:
        result.debug_info.append({
            'record_id': record_id,
            'classification': 'true_positive',
            'text': golden_finding.text,
            'context': golden_finding.context(),
            'info_type': golden_finding.category,
            'start': golden_finding.start,
            'end': golden_finding.end
        })
      result.typeless.true_positives += 1
      if ignore_type:
        result.per_type[golden_finding.category].true_positives += 1
    else:
      result.typeless.false_negatives += 1
      if not strict:
        result.debug_info.append({
            'record_id': record_id,
            'classification': 'false_negative',
            'text': golden_finding.text,
            'context': golden_finding.context(),
            'info_type': golden_finding.category,
            'start': golden_finding.start,
            'end': golden_finding.end
        })
      if ignore_type:
        result.per_type[golden_finding.category].false_negatives += 1

  calculate_stats(result.stats)
  calculate_stats(result.typeless)
  return result


def binary_token_compare(findings, golden_findings, record_id):
  tokenized_findings = tokenize_set(findings)
  tokenized_goldens = tokenize_set(golden_findings)
  return count_matches(
      tokenized_findings,
      tokenized_goldens,
      record_id,
      strict=False,
      ignore_type=True)


def typed_token_compare(findings, golden_findings, record_id):
  """Same as before, but do not ignore the type in the per_type listing."""
  tokenized_findings = tokenize_set(findings)
  tokenized_goldens = tokenize_set(golden_findings)
  return count_matches(
      tokenized_findings,
      tokenized_goldens,
      record_id,
      strict=False,
      ignore_type=False)


def strict_entity_compare(findings, golden_findings, record_id):
  return count_matches(
      findings, golden_findings, record_id, strict=True, ignore_type=False)


def _sum_typed_stats(per_type, stats):
  """Sums the true/false positives/negatives of each category into stats."""
  for category_stats in per_type.values():
    stats.true_positives += category_stats.true_positives
    stats.false_positives += category_stats.false_positives
    stats.false_negatives += category_stats.false_negatives


def _map_index_to_type(findings, ignore_nonalphanumerics):
  result = {}
  for finding in findings:
    for index in range(finding.start, finding.end):
      if (not ignore_nonalphanumerics) or finding.text[index -
                                                       finding.start].isalnum():
        result[index] = finding.category
  return result


def characters_count_compare(findings,
                             golden_findings,
                             record_id,
                             ignore_nonalphanumerics=False):
  """Calculates the characters count true/false positives/negatives.

  Args:
    findings: List of Finding objects to count matches for.
    golden_findings: List of Finding objects to compare against.
    record_id: str; Unique identifier for this set of findings.
    ignore_nonalphanumerics: bool; If True, ignores any non-alphanumeric
      character in the calculation. Otherwise, every single character is
      included in the calculation.

  Returns:
    An IndividualResult object containing the counts and derived stats.
  """
  result = IndividualResult()
  result.record_id = record_id
  findings_map = _map_index_to_type(findings, ignore_nonalphanumerics)
  golden_findings_map = _map_index_to_type(golden_findings,
                                           ignore_nonalphanumerics)
  all_indices = set(findings_map.keys()).union(golden_findings_map.keys())
  for index in all_indices:
    category = findings_map.get(index, None)
    golden_category = golden_findings_map.get(index, None)
    if category == golden_category:
      result.per_type[category].true_positives += 1
      result.typeless.true_positives += 1
    elif category is not None:
      result.per_type[category].false_positives += 1
      if golden_category is not None:
        result.per_type[golden_category].false_negatives += 1
        result.typeless.true_positives += 1
      else:
        result.typeless.false_positives += 1
    else:
      result.per_type[golden_category].false_negatives += 1
      result.typeless.false_negatives += 1
  _sum_typed_stats(result.per_type, result.stats)
  calculate_stats(result.stats)
  calculate_stats(result.typeless)
  return result


def _count_intervals(findings_indices, golden_findings_indices, stats):
  """Counts the intervals of the different types into stats."""
  max_char = max(itertools.chain(findings_indices, golden_findings_indices))
  for (finding_in_char, golden_finding_in_char), _ in itertools.groupby(
      (i in findings_indices, i in golden_findings_indices)
      for i in range(max_char + 1)):
    if golden_finding_in_char:
      if finding_in_char:
        stats.true_positives += 1
      else:
        stats.false_negatives += 1
    elif finding_in_char:
      stats.false_positives += 1


def intervals_count_compare(findings, golden_findings, record_id):
  """Calculates the intervals count true/false positives/negatives.

  An interval is defined as a maximal string of consecutive characters that all
  have the same classification as each other in the findings, and have the same
  classification as each other in the goldens. The finding classification may
  not match the golden classification - this determines whether the interval is
  a true/false positive/negative.

  Args:
    findings: List of Finding objects to count matches for.
    golden_findings: List of Finding objects to compare against.
    record_id: str; Unique identifier for this set of findings.

  Returns:
    An IndividualResult object containing the counts and derived stats.
  """
  result = IndividualResult()
  result.record_id = record_id
  findings_map = _map_index_to_type(findings, False)
  golden_findings_map = _map_index_to_type(golden_findings, False)
  all_categories = {
      finding.category for finding in itertools.chain(findings, golden_findings)
  }
  for category in all_categories:
    findings_indices = {
        index for index, finding_category in findings_map.items()
        if finding_category == category
    }
    golden_findings_indices = {
        index for index, finding_category in golden_findings_map.items()
        if finding_category == category
    }
    _count_intervals(findings_indices, golden_findings_indices,
                     result.per_type[category])
  _count_intervals(
      set(findings_map.keys()), set(golden_findings_map.keys()),
      result.typeless)
  _sum_typed_stats(result.per_type, result.stats)
  calculate_stats(result.stats)
  calculate_stats(result.typeless)
  return result


class _MacroStats(object):

  def __init__(self):
    self.count = 0
    self.precision_sum = 0
    self.recall_sum = 0
    self.error_message = ''

  def calculate_stats(self):
    """Generate a resuts_pb2.Stats message with the macro-averaged results."""
    stats = results_pb2.Stats()
    if not self.count:
      stats.precision = float('NaN')
      stats.recall = float('NaN')
      stats.f_score = float('NaN')
      stats.error_message = 'Averaging over zero results.'
      return stats
    stats.precision = float(self.precision_sum) / self.count
    stats.recall = float(self.recall_sum) / self.count
    stats.f_score = hmean(stats.precision, stats.recall)
    stats.error_message = self.error_message
    return stats


def _deserialize_accumulated_results(
    serialized_micro_stats, macro, per_type_serialized_stats,
    serialized_typeless_micro_stats, typeless_macro):
  ar = AccumulatedResults()
  ar.micro.ParseFromString(serialized_micro_stats)
  ar.macro = macro
  for type_name, stats in per_type_serialized_stats.items():
    ar.per_type[type_name].ParseFromString(stats)
  ar.typeless_micro.ParseFromString(serialized_typeless_micro_stats)
  ar.typeless_macro = typeless_macro

  return ar


class AccumulatedResults(object):
  """Accumulates micro and macro averages."""

  def __init__(self):
    self.micro = results_pb2.Stats()
    self.macro = _MacroStats()
    # Map from info type name to Stats pb.
    self.per_type = collections.defaultdict(results_pb2.Stats)
    self.typeless_micro = results_pb2.Stats()
    self.typeless_macro = _MacroStats()

  # Dataflow's pickling gets confused if it has to deal with raw protos, so we
  # serialize them here.
  def __reduce__(self):
    per_type_serialized = {}
    for type_name, stats in self.per_type.items():
      per_type_serialized[type_name] = stats.SerializeToString()
    return (_deserialize_accumulated_results,
            (self.micro.SerializeToString(), self.macro, per_type_serialized,
             self.typeless_micro.SerializeToString(), self.typeless_macro))

  def add_result(self, result):
    """Add an individual result to the AccumulatedResults.

    Args:
      result: IndividualResult to add.
    """
    self.micro.true_positives += result.stats.true_positives
    self.micro.false_positives += result.stats.false_positives
    self.micro.false_negatives += result.stats.false_negatives
    for info_type, stats in result.per_type.items():
      self.per_type[info_type].true_positives += stats.true_positives
      self.per_type[info_type].false_positives += stats.false_positives
      self.per_type[info_type].false_negatives += stats.false_negatives
    self.typeless_micro.true_positives += result.typeless.true_positives
    self.typeless_micro.false_positives += result.typeless.false_positives
    self.typeless_micro.false_negatives += result.typeless.false_negatives

    if (math.isnan(result.stats.precision) or
        math.isnan(result.stats.recall)):
      self.macro.error_message += 'Ignored results for {0} '.format(
          result.record_id)
      logging.warning('Macro average ignoring results for %s', result.record_id)
    else:
      self.macro.count += 1
      self.macro.precision_sum += result.stats.precision
      self.macro.recall_sum += result.stats.recall

    if (math.isnan(result.typeless.precision) or
        math.isnan(result.typeless.recall)):
      self.typeless_macro.error_message += 'Ignored results for {0} '.format(
          result.record_id)
      logging.warning('Typeless macro average ignoring results for %s',
                      result.record_id)
    else:
      self.typeless_macro.count += 1
      self.typeless_macro.precision_sum += result.typeless.precision
      self.typeless_macro.recall_sum += result.typeless.recall

  def per_type_protos(self):
    """Return the per-type stats as a list of PerTypeStats protos."""
    protos = []
    for info_type, stats in sorted(self.per_type.items()):
      pb = results_pb2.PerTypeStats()
      pb.info_type_category = info_type
      pb.stats.CopyFrom(calculate_stats(stats))
      protos.append(pb)
    return protos

  def __add__(self, other):
    new = AccumulatedResults()
    new.micro.true_positives = (
        self.micro.true_positives + other.micro.true_positives)
    new.micro.false_positives = (
        self.micro.false_positives + other.micro.false_positives)
    new.micro.false_negatives = (
        self.micro.false_negatives + other.micro.false_negatives)
    for info_type, stats in itertools.chain(self.per_type.items(),
                                            other.per_type.items()):
      new.per_type[info_type].true_positives += stats.true_positives
      new.per_type[info_type].false_positives += stats.false_positives
      new.per_type[info_type].false_negatives += stats.false_negatives

    new.macro.count = self.macro.count + other.macro.count
    new.macro.precision_sum = (
        self.macro.precision_sum + other.macro.precision_sum)
    new.macro.recall_sum = self.macro.recall_sum + other.macro.recall_sum
    new.macro.error_message = (
        self.macro.error_message + other.macro.error_message)

    new.typeless_micro.true_positives = (
        self.typeless_micro.true_positives +
        other.typeless_micro.true_positives)
    new.typeless_micro.false_positives = (
        self.typeless_micro.false_positives +
        other.typeless_micro.false_positives)
    new.typeless_micro.false_negatives = (
        self.typeless_micro.false_negatives +
        other.typeless_micro.false_negatives)

    new.typeless_macro.count = (
        self.typeless_macro.count + other.typeless_macro.count)
    new.typeless_macro.precision_sum = (
        self.typeless_macro.precision_sum + other.typeless_macro.precision_sum)
    new.typeless_macro.recall_sum = (
        self.typeless_macro.recall_sum + other.typeless_macro.recall_sum)
    new.typeless_macro.error_message = (
        self.typeless_macro.error_message + other.typeless_macro.error_message)

    return new
