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
import itertools
import json
import logging
import os
import time
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apiclient import discovery
from apiclient import errors
from common import mae
import httplib2


def _get_index(column_name, headers):
  """Return the position in the headers list where column_name appears."""
  i = 0
  for header in headers:
    if header['name'] == column_name:
      return i
    i += 1
  return -1


def _request_with_retry(fn, num_retries=5):
  """Makes a service request; and retries if needed."""
  for attempt in xrange(num_retries):
    try:
      return fn()
    except errors.HttpError as error:
      if attempt == (num_retries - 1):
        # Give up after num_retries
        logging.error('last attempt failed. giving up.')
        raise
      elif (error.resp.status == 429 or
            (error.resp.status == 403 and
             error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded'])):
        # 429 - Too Many Requests
        # 403 - Client Rate limit exceeded. Wait and retry.
        # 403 can also mean app authentication issue, so explicitly check
        # for rate limit error
        # https://developers.google.com/drive/web/handle-errors
        sleep_seconds = 5+2**attempt
        logging.warn(
            'attempt %d failed with 403 or 429 error. retrying in %d sec...',
            attempt + 1, sleep_seconds)
        time.sleep(sleep_seconds)
      elif error.resp.status in [500, 503]:
        sleep_seconds = 10+2**attempt
        # 500, 503 - Service error. Wait and retry.
        logging.warn('attempt %d failed with 5xx error. retrying in %d sec...',
                     attempt + 1, sleep_seconds)
        time.sleep(sleep_seconds)
      else:
        # Don't retry for client errors.
        logging.error('attempt %d failed. giving up.', attempt + 1)
        raise
    except httplib2.HttpLib2Error:
      # Don't retry for connection errors.
      logging.error('attempt %d failed. giving up.', attempt + 1)
      raise


def get_deid_text(deid_response, pass_through_columns, target_columns):
  """Get the de-id'd text from the deid() API call response."""
  # Sample response for a request with a table as input:
  # {'items': [
  #    {'table': {
  #      'headers': [{'name': 'note'}, {'name': 'first_name'}],
  #      'rows': [
  #        {'values': [{'stringValue': 'text'}, {'stringValue': 'Pat'}]}
  #      ]
  #    }}
  #  ...
  # ] }
  response = {}
  for col in pass_through_columns:
    response[col['name']] = deid_response[col['name']]

  if len(target_columns) == 1:
    response[target_columns[0]['name']] = (
        deid_response['raw_response']['item']['value'])
  else:
    table = deid_response['raw_response']['item']['table']
    for col in target_columns:
      i = _get_index(col['name'], table['headers'])
      val = ''
      if i >= 0 and table['rows']:
        val = table['rows'][0]['values'][i][col['type']]
      response[col['name']] = val

  return response


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


# Creates the 'item' field for a deid or inspect request.
# In the simple case, returns a single value, e.g.:
#   'item': { 'type': 'text/plain', 'value': 'given text' }
# If multiple columns are specified, creates a table with a single row, e.g.:
#   'item': {'table': {
#     'headers': [{'name': 'note'}, {'name': 'secondary note'}]
#     'rows': [ {
#       'values': [{'stringValue': 'text of the note'},
#                  {'stringValue': 'text of the secondary note'}]
#     }]
#   }}
def _create_item(target_columns, row):
  if len(target_columns) == 1:
    return {'type': 'text/plain', 'value': row[target_columns[0]['name']]}
  else:
    table = {'headers': [], 'rows': [{'values': []}]}
    for col in target_columns:
      table['headers'].append({'name': col['name']})
      table['rows'][0]['values'].append({col['type']: row[col['name']]})
    return {'table': table}


def deid(row, credentials, project, deid_config, inspect_config,
         pass_through_columns, target_columns, per_row_types, dlp_api_name):
  """Put the data through the DLP API DeID method.

  Args:
    row: A BigQuery row with data to send to the DLP API.
    credentials: oauth2client.Credentials for authentication with the DLP API.
    project: The project to send the request for.
    deid_config: DeidentifyConfig map, as defined in the DLP API:
      https://goo.gl/e8DBmm#DeidentifyTemplate.DeidentifyConfig
    inspect_config: inspectConfig map, as defined in the DLP API:
      https://cloud.google.com/dlp/docs/reference/rest/v2beta2/InspectConfig
    pass_through_columns: List of strings; columns that should not be sent to
      the DLP API, but should still be included in the final output.
    target_columns: List of strings; columns that should be sent to the DLP API,
      and have the DLP API data included in the final output.
    per_row_types: List of objects representing columns that should be read and
      sent to the DLP API as custom infoTypes.
    dlp_api_name: Name of the DLP API to use (generally 'dlp', but may vary for
      testing purposes).
  Raises:
    Exception: If the request fails.
  Returns:
    A dict containing:
     - 'raw_response': The result from the DLP API call.
     - An entry for each pass-through column.
  """
  dlp = discovery.build(dlp_api_name, 'v2beta2', credentials=credentials)
  projects = dlp.projects()
  content = projects.content()

  inspect_config = _per_row_inspect_config(inspect_config, per_row_types, row)
  req_body = {
      'deidentifyConfig': deid_config,
      'inspectConfig': inspect_config,
      'item': _create_item(target_columns, row)
  }
  parent = 'projects/{0}'.format(project)
  response = _request_with_retry(
      content.deidentify(body=req_body, parent=parent).execute)
  if 'error' in response:
    patient_id = row['patient_id'] if 'patient_id' in row else '?'
    record_number = row['record_number'] if 'record_number' in row else '?'
    raise Exception('Deidentify() failed for patient {} record {}: {}'.format(
        patient_id, record_number, response['error']))

  ret = {'raw_response': response}
  for col in pass_through_columns:
    ret[col['name']] = row[col['name']]
  return ret


def inspect(row, credentials, project, inspect_config, pass_through_columns,
            target_columns, per_row_types, dlp_api_name):
  """Put the data through the DLP API inspect method.

  Args:
    row: A BigQuery row with data to send to the DLP API.
    credentials: oauth2client.Credentials for authentication with the DLP API.
    project: The project to send the request for.
    inspect_config: inspectConfig map, as defined in the DLP API:
      https://cloud.google.com/dlp/docs/reference/rest/v2beta2/InspectConfig
    pass_through_columns: List of strings; columns that should not be sent to
      the DLP API, but should still be included in the final output.
    target_columns: List of strings; columns that should be sent to the DLP API,
      and have the DLP API data included in the final output.
    per_row_types: List of objects representing columns that should be read and
      sent to the DLP API as custom infoTypes.
    dlp_api_name: Name of the DLP API to use (generally 'dlp', but may vary for
      testing purposes).
  Raises:
    Exception: If the request fails.
  Returns:
    A dict containing:
     - 'result': The result from the DLP API call.
     - 'original_note': The original note, to be used in generating MAE output.
     - An entry for each pass-through column.
  """
  dlp = discovery.build(dlp_api_name, 'v2beta2', credentials=credentials)
  projects = dlp.projects()
  content = projects.content()

  inspect_config = _per_row_inspect_config(inspect_config, per_row_types, row)

  req_body = {
      'inspectConfig': inspect_config,
      'item': _create_item(target_columns, row)
  }

  parent = 'projects/{0}'.format(project)
  response = _request_with_retry(
      content.inspect(body=req_body, parent=parent).execute)
  truncated = 'findingsTruncated'
  if truncated in response['result'] and response['result'][truncated]:
    raise Exception('Inspect() failed; too many findings:\n%s' % response)
  if 'error' in response:
    raise Exception('Inspect() failed for patient {} record {}: {}'.format(
        row['patient_id'], row['record_number'], response['error']))

  ret = {'result': response['result']}
  # Pass the original note along for use in MAE output.
  if len(target_columns) == 1:
    ret['original_note'] = row[target_columns[0]['name']]
  for col in pass_through_columns:
    ret[col['name']] = row[col['name']]
  return ret


def format_findings(inspect_result, pass_through_columns):
  ret = {'findings': str(inspect_result['result'])}
  for col in pass_through_columns:
    ret[col['name']] = inspect_result[col['name']]
  return ret


def split_gcs_name(gcs_path):
  bucket = gcs_path.split('/')[2]
  blob = gcs_path[len('gs://') + len(bucket) + 1 : ]
  return bucket, blob


def write_mae(mae_result, storage_client_fn, project, mae_dir):
  """Write the MAE results to GCS."""
  storage_client = storage_client_fn(project)
  filename = '{0}-{1}.xml'.format(
      mae_result.patient_id, mae_result.record_number)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(os.path.join(blob_dir, filename))
  blob.upload_from_string(mae_result.mae_xml)


def write_dtd(storage_client_fn, project, mae_dir, mae_tag_categories,
              task_name):
  """Write the DTD config file."""
  storage_client = storage_client_fn(project)
  dtd_contents = mae.generate_dtd(mae_tag_categories, task_name)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(os.path.join(blob_dir, 'classification.dtd'))
  blob.upload_from_string(dtd_contents)


def _is_custom_type(type_name, per_row_types, per_dataset_types):
  for custom_type in per_row_types:
    if custom_type['infoTypeName'] == type_name:
      return True
  for per_dataset_type in per_dataset_types:
    if 'infoTypes' in per_dataset_type:
      for custom_type in per_dataset_type['infoTypes']:
        if custom_type['infoTypeName'] == type_name:
          return True
  return False


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

  inspect_config = {}
  per_dataset_types = []
  if 'perDatasetTypes' in cfg:
    per_dataset_types = cfg['perDatasetTypes']
    if bq_client:
      inspect_config['customInfoTypes'] = _load_per_dataset_types(
          per_dataset_types, input_query, input_table, bq_client, bq_config_fn)

  # Generate an inspectConfig based on all the infoTypes listed in the deid
  # config's transformations.
  info_types = set()
  if deid_config:
    for transform in deid_config['infoTypeTransformations']['transformations']:
      for t in transform['infoTypes']:
        # Don't include custom infoTypes in the inspect config or the DLP API
        # will complain.
        if _is_custom_type(t['name'], per_row_types, per_dataset_types):
          continue
        info_types.add(t['name'])
  inspect_config['infoTypes'] = [{'name': t} for t in info_types]

  pass_through_columns = [{'name': 'patient_id', 'type': 'stringValue'},
                          {'name': 'record_number', 'type': 'integerValue'}]
  target_columns = [{'name': 'note', 'type': 'stringValue'}]
  if 'columns' in cfg:
    if 'passThrough' in cfg['columns']:
      pass_through_columns = cfg['columns']['passThrough']
    if 'inspect' in cfg['columns']:
      target_columns = cfg['columns']['inspect']

  return (inspect_config, deid_config, mae_tag_categories, per_row_types,
          pass_through_columns, target_columns)


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
        # version as of 2017-12-08.
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


def _generate_schema(pass_through_columns, target_columns):
  """Generate a BigQuery schema with the configured columns."""
  m = {'stringValue': 'STRING', 'integerValue': 'INTEGER',
       'floatValue': 'FLOAT', 'booleanValue': 'BOOLEAN'}
  segments = []
  for col in itertools.chain(pass_through_columns, target_columns):
    segments.append('{0}:{1}'.format(col['name'], m[col['type']]))
  return ', '.join(segments)


def run_pipeline(input_query, input_table, deid_table, findings_table,
                 mae_dir, deid_config_file, task_name, credentials, project,
                 storage_client_fn, bq_client, bq_config_fn, dlp_api_name,
                 pipeline_args):
  """Read the records from BigQuery, DeID them, and write them to BigQuery."""
  if (input_query is None) == (input_table is None):
    return 'Exactly one of input_query and input_table must be set.'
  config_text = ''
  if deid_config_file:
    with open(deid_config_file) as f:
      config_text = f.read()
  (inspect_config, deid_config, mae_tag_categories, per_row_types,
   pass_through_columns, target_columns) = generate_configs(
       config_text, input_query, input_table, bq_client, bq_config_fn)

  if len(target_columns) > 1 and mae_dir:
    raise Exception(
        'Cannot use --mae_dir when multiple columns are specified for '
        '"inspect" in the config file.')

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  reads = None
  if input_table:
    reads = p | 'read' >> beam.io.Read(beam.io.BigQuerySource(input_table))
  else:
    bq = beam.io.BigQuerySource(query=input_query)
    reads = (p |
             'read' >> beam.io.Read(bq))

  inspect_data = None
  if findings_table or mae_dir:
    inspect_data = (
        reads | 'inspect' >> beam.Map(
            inspect, credentials, project, inspect_config, pass_through_columns,
            target_columns, per_row_types, dlp_api_name))
  if findings_table:
    # Write the inspect result to BigQuery. We don't process the result, even
    # if it's for multiple columns.
    schema = _generate_schema(pass_through_columns, []) + ',findings:STRING'
    _ = (inspect_data
         | 'format_findings' >> beam.Map(format_findings, pass_through_columns)
         | 'write_findings' >> beam.io.Write(beam.io.BigQuerySink(
             findings_table, schema=schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
  if mae_dir:
    if not mae_dir.startswith('gs://'):
      return ['--mae_dir must be a GCS path starting with "gs://".']
    write_dtd(storage_client_fn, project, mae_dir, mae_tag_categories,
              task_name)
    _ = (inspect_data
         | 'generate_mae' >> beam.Map(
             mae.generate_mae, task_name, mae_tag_categories)
         | 'write_mae' >> beam.Map(
             write_mae, storage_client_fn, project, mae_dir))
  if deid_table:
    if not deid_config_file:
      return ['Must set --deid_config_file when --deid_table is set.']
    # Call deidentify and write the result to BigQuery.
    schema = _generate_schema(pass_through_columns, target_columns)
    _ = (reads
         | 'deid' >> beam.Map(
             deid,
             credentials, project, deid_config, inspect_config,
             pass_through_columns, target_columns, per_row_types, dlp_api_name)
         | 'get_deid_text' >> beam.Map(get_deid_text,
                                       pass_through_columns, target_columns)
         | 'write_deid_text' >> beam.io.Write(beam.io.BigQuerySink(
             deid_table, schema=schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
  result = p.run().wait_until_finish()

  logging.info('DLP DeID result: %s', result)
  return []


def add_all_args(parser):
  """Add command-line arguments to the parser."""
  parser.add_argument(
      '--input_query', type=str, required=False,
      help=('BigQuery query to provide input data. Must yield rows with 3 '
            'fields: (patient_id, record_number, note), unless columns are '
            'configured differently in the config file.'))
  parser.add_argument(
      '--input_table', type=str, required=False,
      help=('BigQuery table to provide input data. Must have rows with 3 '
            'fields: (patient_id, record_number, note), unless columns are '
            'configured differently in the config file.'))
  parser.add_argument('--deid_table', type=str, required=False,
                      help='BigQuery table to store DeID\'d data.')
  parser.add_argument('--findings_table', type=str, required=False,
                      help='BigQuery table to store DeID summary data.')
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
