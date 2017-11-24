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

"""Run Google Data Loss Prevention API DeID.

All input/output files should be on Google Cloud Storage.

Requires Apache Beam client and Google Python API Client:
pip install --upgrade apache_beam
pip install --upgrade google-api-python-client
"""

from __future__ import absolute_import

import copy
from functools import partial
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apiclient import discovery
from google.cloud import storage
from dlp import mae
import google.auth


def get_deid_text(deid_response):
  return {
      'patient_id': deid_response['patient_id'],
      'record_number': deid_response['record_number'],
      'note': deid_response['raw_response']['items'][0]['value']
  }


def deid(credentials, deid_config, inspect_config, dlp_api_name, row):
  """Put the data through the DLP API DeID."""
  dlp = discovery.build(dlp_api_name, 'v2beta1', credentials=credentials)
  content = dlp.content()

  req_body = {
      'deidentifyConfig': deid_config,
      'inspectConfig': inspect_config,
      'items': [
          {
              'type': 'text/plain',
              'value': row['note']
          }
      ]
  }
  response = content.deidentify(body=req_body).execute()
  if 'error' in response:
    raise Exception('Deidentify() failed for patient {} record {}: {}'.format(
        row['patient_id'], row['record_number'], response['error']))

  return {
      'patient_id': row['patient_id'],
      'record_number': int(row['record_number']),
      'raw_response': response
  }


def inspect(credentials, inspect_config, dlp_api_name, row):
  """Put the data through the DLP API DeID inspect method."""
  dlp = discovery.build(dlp_api_name, 'v2beta1', credentials=credentials)
  content = dlp.content()

  req_body = {
      'inspectConfig': inspect_config,
      'items': [
          {
              'type': 'text/plain',
              'value': row['note']
          }
      ]
  }
  response = content.inspect(body=req_body).execute()
  if 'error' in response:
    raise Exception('Inspect() failed for patient {} record {}: {}'.format(
        row['patient_id'], row['record_number'], response['error']))

  return {
      'patient_id': row['patient_id'],
      'record_number': int(row['record_number']),
      'original_note': row['note'],
      'result': response['results'][0]
  }


def format_findings(inspect_result):
  return {
      'patient_id': inspect_result['patient_id'],
      'record_number': inspect_result['record_number'],
      'findings': str(inspect_result['result'])
  }


def start(finding):
  if 'start' not in finding['location']['byteRange']:
    return 0
  return int(finding['location']['byteRange']['start'])


def end(finding):
  return int(finding['location']['byteRange']['end'])


def sort_findings(result):
  """Sort the findings from inspect() and ensure none of them overlap."""
  if 'findings' not in result:
    return []
  # Deepcopy the findings so we can modify them for the purposes of writing
  # annotations without impacting other methods using this data.
  sorted_findings = copy.deepcopy(sorted(result['findings'], key=start))
  i = 1
  while i < len(sorted_findings):
    if (end(sorted_findings[i-1]) == end(sorted_findings[i]) and
        start(sorted_findings[i-1]) == start(sorted_findings[i])):
      # We have two findings on the same string. Combine the findings by adding
      # the second one's infoType to the first's, and delete the second finding.
      sorted_findings[i-1]['infoType']['name'] += (
          ',' + sorted_findings[i]['infoType']['name'])
      del sorted_findings[i]
      continue

    if end(sorted_findings[i-1]) > start(sorted_findings[i]):
      raise Exception(
          'Can\'t process overlapping findings:\n\n%s\n\nAND\n\n%s\n\n' %
          (sorted_findings[i-1], sorted_findings[i]))
    i += 1
  return sorted_findings


def add_annotations(inspect_result):
  """Annotate the original note with the findings from inspect()."""
  annotated = inspect_result['original_note']
  for finding in reversed(sort_findings(inspect_result['result'])):
    found_text = annotated[start(finding):end(finding)]
    # Annotate with XML.
    annotated = (
        annotated[:start(finding)] +
        '<finding info_types="{0}">{1}</finding>'.format(
            finding['infoType']['name'], found_text) +
        annotated[end(finding):])

  return {
      'patient_id': inspect_result['patient_id'],
      'record_number': inspect_result['record_number'],
      'note': annotated,
  }


def split_gcs_name(gcs_path):
  bucket = gcs_path.split('/')[2]
  blob = gcs_path[len('gs://') + len(bucket) + 1 : ]
  return bucket, blob


def write_mae(project, credentials, mae_dir, mae_result):
  """Write the MAE results to GCS."""
  storage_client = storage.Client(project, credentials)
  filename = '{0}-{1}.xml'.format(
      mae_result.patient_id, mae_result.record_number)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(os.path.join(blob_dir, filename))
  blob.upload_from_string(mae_result.mae_xml)


def write_dtd(storage_client, mae_dir, mae_tag_categories, task_name):
  """Write the DTD config file."""
  dtd_contents = mae.generate_dtd(mae_tag_categories, task_name)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(os.path.join(blob_dir, 'classification.dtd'))
  blob.upload_from_string(dtd_contents)


def run_pipeline(input_query, input_table, deid_table, findings_table,
                 annotated_notes_table, mae_dir, deid_config_file, task_name,
                 project, credentials, dlp_api_name, pipeline_args):
  """Read the records from BigQuery, DeID them, and write them to BigQuery."""
  if (input_query is None) == (input_table is None):
    return 'Exactly one of input_query and input_table must be set.'
  deid_config = {}
  mae_tag_categories = {}
  if deid_config_file:
    with open(deid_config_file) as f:
      cfg = json.load(f)
      deid_config = cfg['deidConfig']
      if 'infoTypeCategories' in cfg:
        mae_tag_categories = cfg['infoTypeCategories']

  info_types = set()
  for transform in deid_config['infoTypeTransformations']['transformations']:
    info_types |= set([t['name'] for t in transform['infoTypes']])
  inspect_config = {'infoTypes': [{'name': t} for t in info_types]}

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  reads = None
  if input_table:
    reads = p | 'read' >> beam.io.Read(beam.io.BigQuerySource(input_table))
  else:
    bq = beam.io.BigQuerySource(query=input_query)
    reads = (p |
             'read' >> beam.io.Read(bq))

  inspect_data = None
  if findings_table or annotated_notes_table or mae_dir:
    inspect_data = (
        reads |
        'inspect' >> beam.Map(partial(
            inspect, credentials, inspect_config, dlp_api_name)))
  if findings_table:
    # Call inspect and write the result to BigQuery.
    _ = (inspect_data
         | 'format_findings' >> beam.Map(format_findings)
         | 'write_findings' >> beam.io.Write(beam.io.BigQuerySink(
             findings_table,
             schema='patient_id:STRING,record_number:INTEGER,findings:STRING',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
  if annotated_notes_table:
    # Annotate the PII found by inspect and write the result to BigQuery.
    _ = (inspect_data
         | 'add_annotations' >> beam.Map(add_annotations)
         | 'write_annotated' >> beam.io.Write(beam.io.BigQuerySink(
             annotated_notes_table,
             schema='patient_id:STRING, record_number:INTEGER, note:STRING',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
  if mae_dir:
    if not mae_dir.startswith('gs://'):
      return ['--mae_dir must be a GCS path starting with "gs://".']
    if not credentials:
      credentials, _ = google.auth.default()
    client = storage.Client(project, credentials)
    write_dtd(client, mae_dir, mae_tag_categories, task_name)
    _ = (inspect_data
         | 'generate_mae' >> beam.Map(
             partial(mae.generate_mae, task_name, mae_tag_categories))
         | 'write_mae' >> beam.Map(
             partial(write_mae, project, credentials, mae_dir)))
  if deid_table:
    if not deid_config_file:
      return ['Must set --deid_config_file when --deid_table is set.']
    # Call deidentify and write the result to BigQuery.
    _ = (reads
         | 'deid' >> beam.Map(
             partial(deid, credentials, deid_config, inspect_config,
             dlp_api_name))
         | 'get_deid_text' >> beam.Map(get_deid_text)
         | 'write_deid_text' >> beam.io.Write(beam.io.BigQuerySink(
             deid_table,
             schema='patient_id:STRING, record_number:INTEGER, note:STRING',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
  result = p.run().wait_until_finish()

  logging.info('DLP DeID result: %s', result)
  return []


def add_all_args(parser):
  """Add command-line arguments to the parser."""
  parser.add_argument(
      '--input_query', type=str, required=False,
      help=('BigQuery query to provide input data. Must yield rows with 3 '
            'fields: (patient_id, record_number, note).'))
  parser.add_argument(
      '--input_table', type=str, required=False,
      help=('BigQuery table to provide input data. Must have rows with 3 '
            'fields: (patient_id, record_number, note).'))
  parser.add_argument('--deid_table', type=str, required=False,
                      help='BigQuery table to store DeID\'d data.')
  parser.add_argument('--findings_table', type=str, required=False,
                      help='BigQuery table to store DeID summary data.')
  parser.add_argument('--annotated_notes_table', type=str, required=False,
                      help=('BigQuery table to store text with annotations of '
                            'findings from DLP API\'s inspect().'))
  parser.add_argument('--mae_dir', type=str, required=False,
                      help=('GCS directory to store inspect() results in MAE '
                            'format.'))
  parser.add_argument('--mae_task_name', type=str, required=False,
                      help='Task name to use in generated MAE files.',
                      default='InspectPhiTask')
  parser.add_argument('--deid_config_file', type=str, required=False,
                      help='Path to a json file holding a DeidentifyConfig.')
  parser.add_argument('--project', type=str, required=True,
                      help='GCP project to run as.')
  parser.add_argument('--dlp_api_name', type=str, required=False,
                      help='Name to use in the DLP API url.',
                      default='dlp')
