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
  """Class to hold category and span of a PHI finding."""

  def __init__(self, category, spans):
    self.category = category
    startstr, endstr = spans.split('~')
    self.start = int(startstr)
    self.end = int(endstr)

  def __hash__(self):
    return hash((self.category, self.start, self.end))

  def __eq__(self, other):
    return (self.category == other.category and self.start == other.start and
            self.end == other.end)


def _get_findings(filename, storage_client):
  bucket = storage_client.lookup_bucket(filename.bucket)
  blob = bucket.get_blob(filename.blob)
  contents = blob.download_as_string()
  tree = XmlTree.fromstring(contents)
  findings = set()
  if tree.find('TAGS') is not None:
    for tag_elem in tree.find('TAGS'):
      findings.add(Finding(tag_elem.tag, tag_elem.get('spans')))
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


def compare(filename, golden_dir, project, credentials):
  """Load data from the file and the golden file and compare.

  Args:
    filename: Name of the file to compare.
    golden_dir: Directory with golden findings to compare against. Must contain
      a file with the same basename as filename.
    project: project ID used to access the files.
    credentials: credentials used to access the files.
  Returns:
    results_pb2.IndividualResult
  """
  storage_client = storage.Client(project, credentials)
  golden_file = gcsutil.GcsFileName.from_path(
      os.path.join(golden_dir, os.path.basename(filename.blob)))

  findings = _get_findings(filename, storage_client)
  golden_findings = _get_findings(golden_file, storage_client)
  record_id = os.path.basename(filename.blob)
  if record_id.endswith('.xml'):
    record_id = record_id[:-4]

  # Strict entity matching calculations.
  strict_entity_results = results_pb2.IndividualResult()
  strict_entity_results.record_id = record_id
  for finding in findings:
    if finding in golden_findings:
      strict_entity_results.stats.true_positives += 1
    else:
      strict_entity_results.stats.false_positives += 1

  for golden_finding in golden_findings:
    if golden_finding not in findings:
      strict_entity_results.stats.false_negatives += 1

  calculate_stats(strict_entity_results.stats)

  return strict_entity_results


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


class CombineResultsFn(beam.CombineFn):
  """CombineFn to take individual results and aggregate them."""

  def create_accumulator(self):
    return results_pb2.Stats(), _MacroStats()

  def add_input(self, averages, individual_result):
    micro_average, macro_average = averages
    micro_average.true_positives += individual_result.stats.true_positives
    micro_average.false_positives += individual_result.stats.false_positives
    micro_average.false_negatives += individual_result.stats.false_negatives

    if (math.isnan(individual_result.stats.precision) or
        math.isnan(individual_result.stats.recall)):
      macro_average.error_message += 'Ignored results for {0} '.format(
          individual_result.record_id)
      logging.warning('Macro average ignoring results for %s',
                      individual_result.record_id)
    else:
      macro_average.count += 1
      macro_average.precision_sum += individual_result.stats.precision
      macro_average.recall_sum += individual_result.stats.recall

    return micro_average, macro_average

  def merge_accumulators(self, accumulators):
    micros, macros = zip(*accumulators)
    micro_total = results_pb2.Stats()
    for micro in micros:
      micro_total.true_positives += micro.true_positives
      micro_total.false_positives += micro.false_positives
      micro_total.false_negatives += micro.false_negatives
    macro_total = _MacroStats()
    for macro in macros:
      macro_total.count += macro.count
      macro_total.precision_sum += macro.precision_sum
      macro_total.recall_sum += macro.recall_sum
      macro_total.error_message += macro.error_message
    return micro_total, macro_total

  def extract_output(self, averages):
    micro_average, macro_average = averages
    calculate_stats(micro_average)
    return micro_average, macro_average.calculate_stats()


def write_aggregate_results(stats, results_dir, project, credentials):
  """Write the aggregate results to results_dir."""
  storage_client = storage.Client(project, credentials)
  micro, macro = stats

  results = results_pb2.Results()
  results.strict_entity_matching_results.micro_average_results.CopyFrom(micro)
  results.strict_entity_matching_results.macro_average_results.CopyFrom(macro)

  logging.info('Aggregate results:\n%s', results)

  filename = gcsutil.GcsFileName.from_path(
      os.path.join(results_dir, 'aggregate_results.txt'))
  logging.info('Writing aggregate results to %s', filename.string())
  bucket = storage_client.lookup_bucket(filename.bucket)
  blob = bucket.blob(filename.blob)
  blob.upload_from_string(str(results))


def run_pipeline(mae_input_pattern, mae_golden_dir, results_dir,
                 credentials, project, pipeline_args):
  """Evaluate the input files against the goldens."""
  filenames = []
  storage_client = storage.Client(project, credentials)
  for f in gcsutil.find_files(mae_input_pattern, storage_client):
    if os.path.dirname(f.string()) != os.path.dirname(mae_input_pattern):
      # Ignore subdirectories.
      continue
    filenames.append(f)

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))

  _ = (p |
       beam.Create(filenames) |
       beam.Map(compare, mae_golden_dir, project, credentials) |
       beam.CombineGlobally(CombineResultsFn()) |
       beam.Map(write_aggregate_results, results_dir, project, credentials))

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
  parser.add_argument('--project', type=str, required=True,
                      help='GCP project to run as.')
