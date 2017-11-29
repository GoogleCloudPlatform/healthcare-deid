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

import collections
import copy
from functools import partial
import json
import logging
import os
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apiclient import discovery
from common import mae


def get_deid_text(deid_response):
  return {
      'patient_id': deid_response['patient_id'],
      'record_number': deid_response['record_number'],
      'note': deid_response['raw_response']['items'][0]['value']
  }


def _per_row_inspect_config(inspect_config, per_row_types, row):
  """Return a copy of inspect_config with the given per-row types added."""
  if not per_row_types:
    return inspect_config

  inspect_config = copy.deepcopy(inspect_config)
  if 'customInfoTypes' not in inspect_config:
    inspect_config['customInfoTypes'] = []
  for per_row_type in per_row_types:
    col = per_row_type['columnName']
    if col not in row:
      raise Exception('customInfoType column "{}" not found.'.format(col))
    inspect_config['customInfoTypes'].append({
        'infoType': {'name': per_row_type['infoTypeName']},
        'dictionary': {'wordList': {'words': [row[col]]}}
    })
  return inspect_config


def deid(credentials, deid_config, inspect_config, per_row_types, dlp_api, row):
  """Put the data through the DLP API DeID."""
  dlp = discovery.build(dlp_api, 'v2beta1', credentials=credentials)
  content = dlp.content()

  inspect_config = _per_row_inspect_config(inspect_config, per_row_types, row)
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


def inspect(credentials, inspect_config, per_row_types, dlp_api, row):
  """Put the data through the DLP API DeID inspect method."""
  dlp = discovery.build(dlp_api, 'v2beta1', credentials=credentials)
  content = dlp.content()

  inspect_config = _per_row_inspect_config(inspect_config, per_row_types, row)
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


def write_mae(mae_result, storage_client_fn, project, credentials, mae_dir):
  """Write the MAE results to GCS."""
  storage_client = storage_client_fn(project, credentials)
  filename = '{0}-{1}.xml'.format(
      mae_result.patient_id, mae_result.record_number)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(os.path.join(blob_dir, filename))
  blob.upload_from_string(mae_result.mae_xml)


def write_dtd(storage_client_fn, project, credentials, mae_dir,
              mae_tag_categories, task_name):
  """Write the DTD config file."""
  storage_client = storage_client_fn(project, credentials)
  dtd_contents = mae.generate_dtd(mae_tag_categories, task_name)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(os.path.join(blob_dir, 'classification.dtd'))
  blob.upload_from_string(dtd_contents)


def generate_configs(config_text, input_query=None, input_table=None,
                     bq_client=None, bq_config_fn=None):
  """Generate DLP API configs based on the input config file."""
  mae_tag_categories = {}
  per_row_types = []
  cfg = json.loads(config_text, object_pairs_hook=collections.OrderedDict)
  deid_config = cfg['deidConfig'] if 'deidConfig' in cfg else {}
  if 'infoTypeCategories' in cfg:
    mae_tag_categories = cfg['infoTypeCategories']
  if 'perRowTypes' in cfg:
    per_row_types = cfg['perRowTypes']

  # Generate an inspectConfig based on all the infoTypes listed in the deid
  # config's transformations.
  info_types = set()
  if deid_config:
    for transform in deid_config['infoTypeTransformations']['transformations']:
      info_types |= set([t['name'] for t in transform['infoTypes']])
  inspect_config = {'infoTypes': [{'name': t} for t in info_types]}

  per_dataset_types = []
  if 'perDatasetTypes' in cfg and bq_client:
    per_dataset_types = cfg['perDatasetTypes']
    custom_info_types = _load_per_dataset_types(
        per_dataset_types, input_query, input_table, bq_client, bq_config_fn)
    inspect_config['customInfoTypes'] = custom_info_types

  return (inspect_config, deid_config, mae_tag_categories, per_row_types,
          per_dataset_types)


def _load_per_dataset_types(per_dataset_cfg, input_query, input_table,
                            bq_client, bq_config_fn):
  """Load data that applies to the whole dataset as custom info types."""
  custom_info_types = []

  saved_query_objects = []
  old_api = hasattr(bq_client, 'run_async_query')
  # Generate the query based on the config options.
  for type_config in per_dataset_cfg:
    query = ''
    if 'bqQuery' in type_config:
      query = type_config['bqQuery']
    elif 'bqTable' in type_config or input_table:
      table = input_table
      if 'bqTable' in type_config:
        table = type_config['bqTable']
      columns = [t['columnName'] for t in type_config['infoTypes']]
      query = 'SELECT %s FROM [%s]' % (
          ','.join(columns), table.replace(':', '.'))
    else:
      query = input_query

    query_job = None
    if old_api:
      query_job = bq_client.run_async_query(str(uuid.uuid4()), query)
      query_job.begin()
    else:
      job_config = bq_config_fn()
      job_config.use_legacy_sql = True
      query_job = bq_client.query(query, job_config=job_config)
    saved_query_objects.append((query_job, type_config))

  for query_job, type_config in saved_query_objects:
    if old_api:
      query_job.result()  # Wait for the job to complete.
      query_job.destination.reload()
      results_table = query_job.destination.fetch_data()
    else:
      results_table = query_job.result()  # Wait for the job to complete.

    # Read the results.
    field_indexes = {}
    if old_api:
      for info_type in type_config['infoTypes']:
        field_indexes[info_type['columnName']] = -1
      i = 0
      for entry in results_table.schema:
        if entry.name in field_indexes:
          field_indexes[entry.name] = i
        i += 1

    type_to_words = collections.defaultdict(set)
    has_results = False
    for row in results_table:
      has_results = True
      if not old_api and not hasattr(row, 'get'):
        # Workaround for google-cloud-bigquery==0.28.0, which is the latest
        # version as of 2017-11-20.
        field_indexes = row._xxx_field_to_index  # pylint: disable=protected-access
      for info_type in type_config['infoTypes']:
        column_name = info_type['columnName']
        value = None
        if old_api or not hasattr(row, 'get'):
          value = row[field_indexes[column_name]]
        else:
          value = row.get(column_name)
        type_to_words[info_type['infoTypeName']].add(value)

    if not has_results:
      raise Exception('No results for query: "{0}"'.format(query_job.query))

    # Generate custom info types based on the results.
    for info_type_name, words in type_to_words.iteritems():
      custom_info_types.append({
          'infoType': {'name': info_type_name},
          'dictionary': {'wordList': {'words': list(words)}}
      })

  return custom_info_types


def run_pipeline(input_query, input_table, deid_table, findings_table,
                 annotated_notes_table, mae_dir, deid_config_file, task_name,
                 credentials, project, storage_client_fn, bq_client,
                 bq_config_fn, dlp_api_name, pipeline_args):
  """Read the records from BigQuery, DeID them, and write them to BigQuery."""
  if (input_query is None) == (input_table is None):
    return 'Exactly one of input_query and input_table must be set.'
  config_text = ''
  if deid_config_file:
    with open(deid_config_file) as f:
      config_text = f.read()

  (inspect_config, deid_config, mae_tag_categories, per_row_types,
   per_dataset_types) = generate_configs(
       config_text, input_query, input_table, bq_client, bq_config_fn)

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
        'inspect' >> beam.Map(
            partial(inspect, credentials, inspect_config, per_row_types,
                    dlp_api_name)))
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
    write_dtd(storage_client_fn, project, credentials, mae_dir,
              mae_tag_categories, task_name)
    _ = (inspect_data
         | 'generate_mae' >> beam.Map(
             mae.generate_mae, task_name, mae_tag_categories)
         | 'write_mae' >> beam.Map(
             write_mae, storage_client_fn, project, credentials, mae_dir))
  if deid_table:
    if per_row_types or per_dataset_types:
      all_custom_info_types = per_row_types[:]
      for per_dataset_type in per_dataset_types:
        all_custom_info_types += per_dataset_type['infoTypes']
      # Add a basic transform for the custom infoTypes. Note that this must be
      # done after creating the inspectConfig above, since the custom infoTypes
      # can't be listed in the inspectConfig.
      transform = {
          'infoTypes': [
              {'name': t['infoTypeName']} for t in all_custom_info_types],
          'primitiveTransformation': {'replaceWithInfoTypeConfig': {}}
      }
      deid_config['infoTypeTransformations']['transformations'].append(
          transform)
    if not deid_config_file:
      return ['Must set --deid_config_file when --deid_table is set.']
    # Call deidentify and write the result to BigQuery.
    _ = (reads
         | 'deid' >> beam.Map(
             partial(deid,
                     credentials, deid_config, inspect_config, per_row_types,
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
