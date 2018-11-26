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

from datetime import datetime
import logging
import math
import posixpath
import xml.etree.ElementTree as XmlTree

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from common import gcsutil
from eval import eval_lib
from eval import results_pb2
from google.cloud import storage

from google.protobuf import text_format


def _get_utcnow():
  return datetime.utcnow()


def get_findings_from_text(raw_text, types_to_ignore):
  """Convert MAE xml to eval_lib.Finding objects."""
  tree = XmlTree.fromstring(raw_text)
  note_text = tree.find('TEXT').text
  findings = set()
  if tree.find('TAGS') is not None:
    for tag_elem in tree.find('TAGS'):
      if tag_elem.tag in types_to_ignore:
        continue
      findings.add(eval_lib.Finding.from_tag(
          tag_elem.tag, tag_elem.get('spans'), note_text))
  return findings, note_text


def _get_findings_from_file(filename, storage_client, types_to_ignore):
  """Parse findings from the given MAE XML file."""
  bucket = storage_client.lookup_bucket(filename.bucket)
  if not bucket:
    raise Exception('Failed to get bucket "{}".'.format(filename.bucket))
  blob = bucket.get_blob(filename.blob)
  if not blob:
    raise Exception('Failed to get blob "{}" in bucket "{}".'.format(
        filename.blob, filename.bucket))
  contents = blob.download_as_string()
  return get_findings_from_text(contents, types_to_ignore)


def compare_findings(findings, golden_findings, record_id, note_text,
                     golden_note_text):
  """Compare findings against goldens."""
  logging.info('Running comparison for record "%s"', record_id)

  if note_text != golden_note_text:
    # If the only difference is a single trailing character, ignore it.
    if ((len(note_text) == len(golden_note_text) + 1 and
         note_text.startswith(golden_note_text)) or
        (len(golden_note_text) == len(note_text) + 1 and
         golden_note_text.startswith(note_text))):
      pass
    else:
      raise Exception(
          'Note text is different from golden for record "{}".'.format(
              record_id))

  strict_entity_results = eval_lib.strict_entity_compare(
      findings, golden_findings, record_id)
  binary_token_results = eval_lib.binary_token_compare(
      findings, golden_findings, record_id)
  return strict_entity_results, binary_token_results


def compare_bq_row(row, types_to_ignore):
  """Compare the findings in the given BigQuery row.

  Args:
    row: BQ row: Map containing (findings_record_id, findings_xml, golden_xml).
    types_to_ignore: List of strings representing types that should be excluded
      from the analysis.
  Returns:
    (IndividualResult, IndividualResult), where the first is for strict entity
    matching and the second is for binary token matching.
  Raises:
    Exception: If golden_xml doesn't exist.
  """
  findings, note_text = get_findings_from_text(row['findings_xml'],
                                               types_to_ignore)
  if 'golden_xml' not in row or row['golden_xml'] is None:
    raise Exception(
        'No golden found for record %s.' % row['findings_record_id'])
  golden_findings, golden_note_text = get_findings_from_text(row['golden_xml'],
                                                             types_to_ignore)
  record_id = row['findings_record_id']

  return compare_findings(findings, golden_findings, record_id, note_text,
                          golden_note_text)


def compare(filename, golden_dir, types_to_ignore):
  """Load data from the file and the golden file and compare.

  Args:
    filename: Name of the file to compare.
    golden_dir: Directory with golden findings to compare against. Must contain
      a file with the same basename as filename.
    types_to_ignore: List of strings representing types that should be excluded
      from the analysis.
  Returns:
    (IndividualResult, IndividualResult), where the first is for strict entity
    matching and the second is for binary token matching.
  """
  storage_client = storage.Client()
  golden_file = gcsutil.GcsFileName.from_path(
      posixpath.join(golden_dir, posixpath.basename(filename.blob)))

  findings, note_text = _get_findings_from_file(
      filename, storage_client, types_to_ignore)
  golden_findings, golden_note_text = _get_findings_from_file(
      golden_file, storage_client, types_to_ignore)
  record_id = posixpath.basename(filename.blob)
  if record_id.endswith('.xml'):
    record_id = record_id[:-4]

  return compare_findings(findings, golden_findings, record_id, note_text,
                          golden_note_text)


class OverallResults(object):
  """Class to hold and accumulate the summarized results to output."""

  def __init__(self):
    self.strict_entity_matching = eval_lib.AccumulatedResults()
    self.binary_token_matching = eval_lib.AccumulatedResults()
    self.is_empty = True

  def __add__(self, other):
    new = OverallResults()
    new.strict_entity_matching = (
        self.strict_entity_matching + other.strict_entity_matching)
    new.binary_token_matching = (
        self.binary_token_matching + other.binary_token_matching)
    new.is_empty = False
    return new

  def to_results_proto(self):
    """Convert to results_pb2.Results."""
    results = results_pb2.Results()
    eval_lib.calculate_stats(self.strict_entity_matching.micro)
    results.strict_entity_matching_results.micro_average_results.CopyFrom(
        self.strict_entity_matching.micro)
    results.strict_entity_matching_results.macro_average_results.CopyFrom(
        self.strict_entity_matching.macro.calculate_stats())
    r = results.strict_entity_matching_results.per_type_micro_average_results
    r.extend(self.strict_entity_matching.per_type_protos())

    eval_lib.calculate_stats(self.binary_token_matching.typeless_micro)
    results.binary_token_matching_results.micro_average_results.CopyFrom(
        self.binary_token_matching.typeless_micro)
    results.binary_token_matching_results.macro_average_results.CopyFrom(
        self.binary_token_matching.typeless_macro.calculate_stats())
    results.binary_token_matching_results.per_type_micro_average_results.extend(
        self.binary_token_matching.per_type_protos())

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
    if overall_results is None or overall_results.is_empty:
      return None

    # Dataflow's pickling gets confused if it has to deal with raw protos, so we
    # serialize them explicitly.
    results = overall_results.to_results_proto()
    logging.info('Aggregate results:\n%s', results)
    return results.SerializeToString()


def write_aggregate_results_to_gcs(results_bytes, results_dir):
  """Write the aggregate results to results_dir."""
  storage_client = storage.Client()
  results = results_pb2.Results()
  results.ParseFromString(results_bytes)

  filename = gcsutil.GcsFileName.from_path(
      posixpath.join(results_dir, 'aggregate_results.txt'))
  logging.info('Writing aggregate results to %s', filename.string())
  bucket = storage_client.lookup_bucket(filename.bucket)
  blob = bucket.blob(filename.blob)
  blob.upload_from_string(str(results))


def _create_row(stats, now, extra_columns=tuple()):
  """Create a BigQuery row from the given stats."""
  row = {'true_positives': stats.true_positives,
         'false_positives': stats.false_positives,
         'false_negatives': stats.false_negatives}
  if not math.isnan(stats.precision):
    row['precision'] = stats.precision
  if not math.isnan(stats.recall):
    row['recall'] = stats.recall
  if not math.isnan(stats.f_score):
    row['f_score'] = stats.f_score

  row['timestamp'] = now

  for column_name, val in extra_columns:
    row[column_name] = val

  return row


def format_individual_result_for_bq(result, now):
  _, binary_token_result = result
  return _create_row(binary_token_result.typeless, now,
                     [('record_id', binary_token_result.record_id)])


def format_aggregate_results_for_bq(aggregate_results_bytes, now):
  """Format results as a BigQuery row (dict from column name to value)."""
  ret = []
  aggregate_results = results_pb2.Results()
  aggregate_results.ParseFromString(aggregate_results_bytes)
  binary_token_results = aggregate_results.binary_token_matching_results
  ret.append(_create_row(binary_token_results.micro_average_results, now,
                         [('info_type', 'ALL')]))
  for result in binary_token_results.per_type_micro_average_results:
    ret.append(_create_row(result.stats, now,
                           [('info_type', result.info_type_category)]))
  return ret


def format_aggregate_text_for_bq(text_aggregate_results, timestamp):
  """Format results as a BigQuery row from a text input."""
  ret = []
  aggregate_results = results_pb2.Results()
  text_format.Merge(text_aggregate_results, aggregate_results)
  binary_token_results = aggregate_results.binary_token_matching_results
  ret.append(_create_row(binary_token_results.micro_average_results, timestamp,
                         [('info_type', 'ALL')]))
  for result in binary_token_results.per_type_micro_average_results:
    ret.append(_create_row(result.stats, timestamp,
                           [('info_type', result.info_type_category)]))
  return ret


def format_debug_info(entity_and_binary_result_pair, now):
  _, binary_token_result = entity_and_binary_result_pair
  for debug_info in binary_token_result.debug_info:
    debug_info['timestamp'] = now
  return binary_token_result.debug_info


def get_binary_token_result(entity_and_binary_result_pair):
  _, binary_token_result = entity_and_binary_result_pair
  pb = results_pb2.IndividualResult()
  pb.record_id = binary_token_result.record_id
  pb.stats.CopyFrom(binary_token_result.typeless)
  return text_format.MessageToString(pb)


BASE_SCHEMA = (
    'recall:FLOAT,precision:FLOAT,f_score:FLOAT,'
    'true_positives:INTEGER,false_positives:INTEGER,false_negatives:INTEGER,'
    'timestamp:TIMESTAMP')


def run_pipeline(mae_input_pattern, mae_golden_dir, results_dir,
                 mae_input_query, mae_golden_table,
                 write_per_note_stats_to_gcs, results_table,
                 per_note_results_table, debug_output_table, types_to_ignore,
                 timestamp, pipeline_args):
  """Evaluate the input files against the goldens."""
  if ((mae_input_pattern is None) == (mae_input_query is None) or
      (mae_golden_dir is None) == (mae_golden_table is None) or
      (mae_input_query is None) != (mae_golden_table is None) or
      (mae_input_pattern is None) != (mae_golden_dir is None)):
    return ['Must set exactly one of: '
            '(--mae_input_pattern AND --mae_golden_dir) '
            'OR (--mae_input_query AND --mae_golden_table).']

  if write_per_note_stats_to_gcs and not results_dir:
    return ['Must set --results_dir when --write_per_note_stats_to_gcs is set.']

  logging.info('Starting evaluation.')

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))

  if mae_input_pattern:
    filenames = []
    storage_client = storage.Client()
    for f in gcsutil.find_files(mae_input_pattern, storage_client):
      if posixpath.dirname(f.string()) != posixpath.dirname(mae_input_pattern):
        # Ignore subdirectories.
        continue
      filenames.append(f)

  per_note_results = None
  if mae_input_query and mae_golden_table:
    query_template = ('SELECT findings.record_id, findings.xml, golden.xml '
                      'FROM ({}) AS findings '
                      'LEFT JOIN [{}] AS golden '
                      'ON findings.record_id=golden.record_id')
    query = query_template.format(mae_input_query, mae_golden_table)
    per_note_results = (p |
                        beam.io.Read(beam.io.BigQuerySource(query=query)) |
                        beam.Map(compare_bq_row, types_to_ignore))
  else:
    per_note_results = (p |
                        beam.Create(filenames) |
                        beam.Map(compare, mae_golden_dir, types_to_ignore))
  if not timestamp:
    timestamp = str(_get_utcnow())
  if debug_output_table:
    _ = (per_note_results |
         beam.FlatMap(format_debug_info, timestamp) |
         'write_debug_info' >> beam.io.Write(beam.io.BigQuerySink(
             debug_output_table,
             schema=('record_id:STRING,classification:STRING,info_type:STRING,'
                     'text:STRING,context:STRING,start:INTEGER,end:INTEGER,'
                     'timestamp:TIMESTAMP'),
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))

  if per_note_results_table:
    _ = (per_note_results |
         beam.Map(format_individual_result_for_bq, timestamp) |
         'write_per_note' >> beam.io.Write(beam.io.BigQuerySink(
             per_note_results_table, schema=('record_id:STRING,' + BASE_SCHEMA),
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
  aggregate_results = (per_note_results |
                       beam.CombineGlobally(CombineResultsFn()))
  if results_dir:
    _ = (aggregate_results |
         beam.Map(write_aggregate_results_to_gcs, results_dir))
  if results_table:
    _ = (aggregate_results |
         beam.FlatMap(format_aggregate_results_for_bq, timestamp) |
         'write_aggregate' >> beam.io.Write(beam.io.BigQuerySink(
             results_table, schema=('info_type:STRING,' + BASE_SCHEMA),
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))

  if write_per_note_stats_to_gcs:
    _ = (per_note_results |
         beam.Map(get_binary_token_result) |
         beam.io.WriteToText(posixpath.join(results_dir, 'per-note-results')))

  result = p.run().wait_until_finish()

  logging.info('Eval result: %s', result)
  return []


def add_all_args(parser):
  """Add command-line arguments to the parser."""
  parser.add_argument(
      '--mae_input_pattern', type=str, required=False,
      help='GCS directory with MAE files to compare against goldens.')
  parser.add_argument(
      '--mae_input_query', type=str, required=False,
      help='BQ query with MAE XML to compare against goldens.')
  parser.add_argument(
      '--mae_golden_dir', type=str, required=False,
      help='GCS directory with "golden" MAE files to use as a baseline.')
  parser.add_argument(
      '--mae_golden_table', type=str, required=False,
      help='BQ table with "golden" MAE XML to use as a baseline.')
  parser.add_argument('--results_dir', type=str,
                      help='GCS directory to write results to.')
  parser.add_argument('--write_per_note_stats_to_gcs', type=bool, default=False,
                      help=('Also write per-note binary token matching '
                            'results to GCS.'))
  parser.add_argument('--results_table', type=str,
                      help=('Bigquery table to write overall (micro-averaged) '
                            'binary token matching results to.'))
  parser.add_argument('--per_note_results_table', type=str,
                      help=('Bigquery table to write per-note binary token '
                            'matching results to.'))
  parser.add_argument('--types_to_ignore', type=lambda s: s.split(','),
                      help=('Comma-separated list of types that should be '
                            'excluded from the analysis.'))
  parser.add_argument('--debug_output_table', type=str,
                      help=('Table for storing debug info (including PHI!) for '
                            'binary token matching results.'))
