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

import logging
import math
import os
import xml.etree.ElementTree as XmlTree

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from common import gcsutil
from eval import results_pb2


class Finding(object):
  """Class to hold category, span, and text of a PHI finding."""

  def __init__(self, category, start, end, text):
    self.category = category
    self.start = start
    self.end = end
    self.text = text

  @classmethod
  def from_tag(cls, category, spans, full_text):
    startstr, endstr = spans.split('~')
    start = int(startstr)
    end = int(endstr)
    if start >= end:
      raise Exception('Invalid span "{}"'.format(spans))
    if end > len(full_text):
      raise Exception('Span "{}" out of range.'.format(spans))
    return cls(category, start, end, full_text[start:end])

  def __hash__(self):
    return hash((self.category, self.start, self.end))

  def __eq__(self, other):
    return (self.category == other.category and self.start == other.start and
            self.end == other.end)

  def __repr__(self):
    return '{}: {}~{} "{}"'.format(
        self.category, self.start, self.end, self.text)


def _get_findings(filename, storage_client):
  """Parse findings from the given MAE XML file."""
  bucket = storage_client.lookup_bucket(filename.bucket)
  blob = bucket.get_blob(filename.blob)
  contents = blob.download_as_string()
  tree = XmlTree.fromstring(contents)
  text = tree.find('TEXT').text
  findings = set()
  if tree.find('TAGS') is not None:
    for tag_elem in tree.find('TAGS'):
      findings.add(Finding.from_tag(tag_elem.tag, tag_elem.get('spans'), text))
  return findings


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
        None, start + finding.start, end + finding.start, token))
  return tokenized


def tokenize_set(findings):
  tokenized = set()
  findings = list(findings)
  for f in findings:
    tokenized |= tokenize_finding(f)

  return tokenized


def count_matches(findings, golden_findings, record_id):
  """Calculate the true/false positive/negatives for the given findings."""
  result = results_pb2.IndividualResult()
  result.record_id = record_id
  for finding in findings:
    if finding in golden_findings:
      result.stats.true_positives += 1
    else:
      result.stats.false_positives += 1

  for golden_finding in golden_findings:
    if golden_finding not in findings:
      result.stats.false_negatives += 1

  calculate_stats(result.stats)
  return result


def compare(filename, golden_dir, project, credentials):
  """Load data from the file and the golden file and compare.

  Args:
    filename: Name of the file to compare.
    golden_dir: Directory with golden findings to compare against. Must contain
      a file with the same basename as filename.
    project: project ID used to access the files.
    credentials: credentials used to access the files.
  Returns:
    (results_pb2.IndividualResult, results_pb2.IndividualResult), where the
    first is for strict entity matching and the second binary token matching.
  """
  storage_client = storage.Client(project, credentials)
  golden_file = gcsutil.GcsFileName.from_path(
      os.path.join(golden_dir, os.path.basename(filename.blob)))

  findings = _get_findings(filename, storage_client)
  golden_findings = _get_findings(golden_file, storage_client)
  record_id = os.path.basename(filename.blob)
  if record_id.endswith('.xml'):
    record_id = record_id[:-4]

  strict_entity_results = count_matches(findings, golden_findings, record_id)

  # Binary token matching calculations.
  tokenized_findings = tokenize_set(findings)
  tokenized_goldens = tokenize_set(golden_findings)
  binary_token_results = count_matches(
      tokenized_findings, tokenized_goldens, record_id)

  return strict_entity_results, binary_token_results


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


class AccumulatedResults(object):
  """Accumulates micro and macro averages."""

  def __init__(self):
    self.micro = results_pb2.Stats()
    self.macro = _MacroStats()

  def add_result(self, result):
    self.micro.true_positives += result.stats.true_positives
    self.micro.false_positives += result.stats.false_positives
    self.micro.false_negatives += result.stats.false_negatives

    if (math.isnan(result.stats.precision) or
        math.isnan(result.stats.recall)):
      self.macro.error_message += 'Ignored results for {0} '.format(
          result.record_id)
      logging.warning('Macro average ignoring results for %s', result.record_id)
    else:
      self.macro.count += 1
      self.macro.precision_sum += result.stats.precision
      self.macro.recall_sum += result.stats.recall

  def __add__(self, other):
    new = AccumulatedResults()
    new.micro.true_positives = (
        self.micro.true_positives + other.micro.true_positives)
    new.micro.false_positives = (
        self.micro.false_positives + other.micro.false_positives)
    new.micro.false_negatives = (
        self.micro.false_negatives + other.micro.false_negatives)

    new.macro.count = self.macro.count + other.macro.count
    new.macro.precision_sum = (
        self.macro.precision_sum + other.macro.precision_sum)
    new.macro.recall_sum = self.macro.recall_sum + other.macro.recall_sum
    new.macro.error_message = (
        self.macro.error_message + other.macro.error_message)
    return new


class OverallResults(object):
  """Class to hold and accumulate the summarized results to output."""

  def __init__(self):
    self.strict_entity_matching = AccumulatedResults()
    self.binary_token_matching = AccumulatedResults()

  def __add__(self, other):
    new = OverallResults()
    new.strict_entity_matching = (
        self.strict_entity_matching + other.strict_entity_matching)
    new.binary_token_matching = (
        self.binary_token_matching + other.binary_token_matching)
    return new

  def to_results_proto(self):
    """Convert to results_pb2.Results."""
    results = results_pb2.Results()
    calculate_stats(self.strict_entity_matching.micro)
    results.strict_entity_matching_results.micro_average_results.CopyFrom(
        self.strict_entity_matching.micro)
    results.strict_entity_matching_results.macro_average_results.CopyFrom(
        self.strict_entity_matching.macro.calculate_stats())

    calculate_stats(self.binary_token_matching.micro)
    results.binary_token_matching_results.micro_average_results.CopyFrom(
        self.binary_token_matching.micro)
    results.binary_token_matching_results.macro_average_results.CopyFrom(
        self.binary_token_matching.macro.calculate_stats())

    return results


class CombineResultsFn(beam.CombineFn):
  """CombineFn to take individual results and aggregate them."""

  def create_accumulator(self):
    return OverallResults()

  def add_input(self, overall_results, individual_results):
    strict_entity_result, binary_token_result = individual_results
    overall_results.strict_entity_matching.add_result(strict_entity_result)
    overall_results.binary_token_matching.add_result(binary_token_result)

    return overall_results

  def merge_accumulators(self, accumulators):
    overall_results = OverallResults()
    for a in accumulators:
      overall_results += a
    return overall_results

  def extract_output(self, overall_results):
    if overall_results is None:
      return None
    return overall_results.to_results_proto()


def write_aggregate_results(results, results_dir, project, credentials):
  """Write the aggregate results to results_dir."""
  storage_client = storage.Client(project, credentials)

  logging.info('Aggregate results:\n%s', results)

  filename = gcsutil.GcsFileName.from_path(
      os.path.join(results_dir, 'aggregate_results.txt'))
  logging.info('Writing aggregate results to %s', filename.string())
  bucket = storage_client.lookup_bucket(filename.bucket)
  blob = bucket.blob(filename.blob)
  blob.upload_from_string(str(results))


def get_binary_token_result(entity_and_binary_result_pair):
  _, binary_token_result = entity_and_binary_result_pair
  return binary_token_result


def run_pipeline(mae_input_pattern, mae_golden_dir, results_dir,
                 output_per_note_stats, credentials, project, pipeline_args):
  """Evaluate the input files against the goldens."""
  filenames = []
  storage_client = storage.Client(project, credentials)
  for f in gcsutil.find_files(mae_input_pattern, storage_client):
    if os.path.dirname(f.string()) != os.path.dirname(mae_input_pattern):
      # Ignore subdirectories.
      continue
    filenames.append(f)

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))

  per_note_results = (p |
                      beam.Create(filenames) |
                      beam.Map(compare, mae_golden_dir, project, credentials))
  _ = (per_note_results |
       beam.CombineGlobally(CombineResultsFn()) |
       beam.Map(write_aggregate_results, results_dir, project, credentials))

  if output_per_note_stats:
    _ = (per_note_results |
         beam.Map(get_binary_token_result) |
         beam.io.WriteToText(os.path.join(results_dir, 'per-note-results')))

  result = p.run().wait_until_finish()

  logging.info('Eval result: %s', result)
  return []


def add_all_args(parser):
  """Add command-line arguments to the parser."""
  parser.add_argument(
      '--mae_input_pattern', type=str, required=True,
      help='GCS directory with MAE files to compare against goldens')
  parser.add_argument(
      '--mae_golden_dir', type=str, required=True,
      help=('GCS directory with "golden" MAE files to use as a baseline for '
            'comparison.'))
  parser.add_argument('--results_dir', type=str, required=True,
                      help='Directory to write results to.')
  parser.add_argument('--output_per_note_stats', type=bool, default=False,
                      help='Also write per-note binary token matching results.')
  parser.add_argument('--project', type=str, required=True,
                      help='GCP project to run as.')
