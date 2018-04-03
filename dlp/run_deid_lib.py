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
import posixpath
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
      elif (error.resp.status == 400 and error.resp.reason == 'Bad Request' and
            'Invalid info_type' in str(error)):
        raise Exception(
            str(error) + '\nEnsure you are using the correct deid_config_file.')
      elif (error.resp.status == 403 and error.resp.reason == 'Forbidden' and
            'serviceusage.services.use' in str(error)):
        raise Exception(
            str(error) + '\nEnsure the service account specified in '
            'GOOGLE_APPLICATION_CREDENTIALS has the '
            'serviceusage.services.use permission.')
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
  # {'item': {'table': {
  #      'headers': [{'name': 'note'}, {'name': 'first_name'}],
  #      'rows': [
  #        {'values': [{'stringValue': 'text'}, {'stringValue': 'Pat'}]}
  #      ]
  #    }}}
  response = {}
  for col in pass_through_columns:
    response[col['name']] = deid_response[col['name']]

  table = deid_response['item']['table']
  for col in target_columns:
    i = _get_index(col['name'], table['headers'])
    val = ''
    if i >= 0 and table['rows']:
      val = table['rows'][0]['values'][i][col['type']]
    response[col['name']] = val

  return response


def _per_row_inspect_config(inspect_config, per_row_types, rows):
  """Return a copy of inspect_config with the given per-row types added."""
  if not per_row_types:
    return inspect_config

  inspect_config = copy.deepcopy(inspect_config)
  if 'customInfoTypes' not in inspect_config:
    inspect_config['customInfoTypes'] = []

  for per_row_type in per_row_types:
    column_name = per_row_type['columnName']
    words = set()
    for row in rows:
      if column_name not in row:
        raise Exception(
            'customInfoType column "{}" not found.'.format(column_name))
      words.add(row[column_name])
    inspect_config['customInfoTypes'].append({
        'infoType': {'name': per_row_type['infoTypeName']},
        'dictionary': {'wordList': {'words': list(words)}}
    })
  return inspect_config


# Creates the 'item' field for a deid or inspect request, e.g.:
#   'item': {'table': {
#     'headers': [{'name': 'note'}, {'name': 'secondary note'}]
#     'rows': [ {
#       {'values': [{'stringValue': 'text of the note'},
#                   {'stringValue': 'text of the secondary note'}]},
#       {'values': [{'stringValue': 'row2 note text'},
#                   {'stringValue': 'row2 secondary note'}]}
#     } ]
#   }}
def _create_item(target_columns, rows):
  """Creates the 'item' field for a deid or inspect request."""
  table = {'headers': [], 'rows': []}
  for _ in rows:
    table['rows'].append({'values': []})
  for col in target_columns:
    table['headers'].append({'name': col['name']})
    for i in range(len(rows)):
      if col['name'] not in rows[i]:
        raise Exception('Expected column "{}" not found in row: "{}"'.format(
            col['name'], rows[i]))
      table['rows'][i]['values'].append({col['type']: rows[i][col['name']]})
  return {'table': table}


def _rebatch_deid(rows, credentials, project, deid_config, inspect_config,
                  pass_through_columns, target_columns, per_row_types,
                  dlp_api_name):
  """Call deid() twice with half the list each time and merge the result."""
  half_size = len(rows) / 2
  ret_a = deid(rows[:half_size], credentials, project, deid_config,
               inspect_config, pass_through_columns, target_columns,
               per_row_types, dlp_api_name)
  ret_b = deid(rows[half_size:], credentials, project, deid_config,
               inspect_config, pass_through_columns, target_columns,
               per_row_types, dlp_api_name)
  return ret_a + ret_b


def deid(rows, credentials, project, deid_config, inspect_config,
         pass_through_columns, target_columns, per_row_types, dlp_api_name):
  """Put the data through the DLP API DeID method.

  Args:
    rows: A list of BigQuery rows with data to send to the DLP API.
    credentials: oauth2client.Credentials for authentication with the DLP API.
    project: The project to send the request for.
    deid_config: DeidentifyConfig map, as defined in the DLP API:
      https://goo.gl/WrvsDB#DeidentifyTemplate.DeidentifyConfig
    inspect_config: inspectConfig map, as defined in the DLP API:
      https://cloud.google.com/dlp/docs/reference/rest/v2/InspectConfig
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
    A list of dicts (one per row) containing:
     - 'item': The 'item' element of the result from the DLP API call.
     - An entry for each pass-through column.
  """
  dlp = discovery.build(dlp_api_name, 'v2', credentials=credentials)
  projects = dlp.projects()
  content = projects.content()

  inspect_config = _per_row_inspect_config(inspect_config, per_row_types, rows)
  req_body = {
      'deidentifyConfig': deid_config,
      'inspectConfig': inspect_config,
      # Include pass-through columns as target columns here so they can be used
      # as the context field for a transformation. We will re-write the response
      # so they contain the original data.
      'item': _create_item(pass_through_columns + target_columns, rows)
  }
  parent = 'projects/{0}'.format(project)
  try:
    response = _request_with_retry(
        content.deidentify(body=req_body, parent=parent).execute)
  except errors.HttpError as error:
    error_json = json.loads(error.content)
    if (error.resp.status != 400 or
        'Retry with a smaller request.' not in error_json['error']['message'] or
        len(rows) == 1):
      raise error
    logging.warning('Batch deid() request too large (%s rows). '
                    'Retrying as two smaller batches.', len(rows))
    return _rebatch_deid(rows, credentials, project, deid_config,
                         inspect_config, pass_through_columns, target_columns,
                         per_row_types, dlp_api_name)
  if 'error' in response:
    raise Exception('Deidentify() failed: {}'.format(response['error']))

  retvals = []
  for i in range(len(rows)):
    response_row = response['item']['table']['rows'][i]
    item = {'table': {'headers': response['item']['table']['headers'],
                      'rows': [response_row]}}
    ret = {'item': item}
    for col in pass_through_columns:
      ret[col['name']] = rows[i][col['name']]
    retvals.append(ret)

  return retvals


def _rebatch_inspect(
    rows, credentials, project, inspect_config, pass_through_columns,
    target_columns, per_row_types, dlp_api_name):
  """Call inspect() twice with half the list each time and merge the result."""
  half_size = len(rows) / 2
  ret_a = inspect(rows[:half_size], credentials, project, inspect_config,
                  pass_through_columns, target_columns, per_row_types,
                  dlp_api_name)
  ret_b = inspect(rows[half_size:], credentials, project, inspect_config,
                  pass_through_columns, target_columns, per_row_types,
                  dlp_api_name)
  # Merge ret_b into ret_a and adjust the row indexes up accordingly.
  for retval in ret_b:
    if 'findings' in retval['result']:
      for finding in retval['result']['findings']:
        index = 0
        # More complicated types, like an image within a pdf, may have multiple
        # contentLocations, but our simple table will only have one.
        content_location = finding['location']['contentLocations'][0]
        table_location = content_location['recordLocation']['tableLocation']
        if 'rowIndex' in table_location:
          index = int(table_location['rowIndex'])
        table_location['rowIndex'] = index + half_size
    ret_a.append(retval)
  return ret_a


def inspect(rows, credentials, project, inspect_config, pass_through_columns,
            target_columns, per_row_types, dlp_api_name):
  """Put the data through the DLP API inspect method.

  Args:
    rows: A list of BigQuery rows with data to send to the DLP API.
    credentials: oauth2client.Credentials for authentication with the DLP API.
    project: The project to send the request for.
    inspect_config: inspectConfig map, as defined in the DLP API:
      https://cloud.google.com/dlp/docs/reference/rest/v2/InspectConfig
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
    A list of dicts (one per row) containing:
     - 'result': The result from the DLP API call.
     - 'original_note': The original note, to be used in generating MAE output.
     - An entry for each pass-through column.
  """
  dlp = discovery.build(dlp_api_name, 'v2', credentials=credentials)
  projects = dlp.projects()
  content = projects.content()

  inspect_config = _per_row_inspect_config(inspect_config, per_row_types, rows)

  req_body = {
      'inspectConfig': inspect_config,
      'item': _create_item(target_columns, rows)
  }

  parent = 'projects/{0}'.format(project)
  response = _request_with_retry(
      content.inspect(body=req_body, parent=parent).execute)
  truncated = 'findingsTruncated'
  if truncated in response['result'] and response['result'][truncated]:
    if len(rows) == 1:
      raise Exception('Inspect() failed; too many findings (> %s).' %
                      len(response['result']['findings']))
    logging.warning('Batch inspect() request too large (%s rows). '
                    'Retrying as two smaller batches.', len(rows))
    return _rebatch_inspect(
        rows, credentials, project, inspect_config, pass_through_columns,
        target_columns, per_row_types, dlp_api_name)

  if 'error' in response:
    raise Exception('Inspect() failed: {}'.format(response['error']))

  retvals = []
  for row in rows:
    ret = {'result': {'findings': []}}
    # Pass the original note along for use in MAE output.
    if len(target_columns) == 1:
      ret['original_note'] = row[target_columns[0]['name']]
    for col in pass_through_columns:
      if col['name'] not in row:
        raise Exception(
            'Expected column "{}" not found in row: "{}". Adjust your input '
            'table or the "columns" section of your config file.'.format(
                col['name'], row))
      ret[col['name']] = row[col['name']]
    retvals.append(ret)
  if 'findings' in response['result']:
    for finding in response['result']['findings']:
        # More complicated types, like an image within a pdf, may have multiple
        # contentLocations, but our simple table will only have one.
      content_location = finding['location']['contentLocations'][0]
      table_location = content_location['recordLocation']['tableLocation']
      if not table_location:
        retvals[0]['result']['findings'].append(finding)
      else:
        index = int(table_location['rowIndex'])
        retvals[index]['result']['findings'].append(finding)

  return retvals


def format_findings(inspect_result, pass_through_columns):
  ret = {'findings': str(inspect_result['result'])}
  for col in pass_through_columns:
    ret[col['name']] = inspect_result[col['name']]
  return ret


def split_gcs_name(gcs_path):
  bucket = gcs_path.split('/')[2]
  blob = gcs_path[len('gs://') + len(bucket) + 1 : ]
  return bucket, blob


def mae_to_bq_row(mae_result):
  return {'record_id': mae_result.record_id, 'xml': mae_result.mae_xml}


def write_mae(mae_result, storage_client_fn, project, mae_dir):
  """Write the MAE results to GCS."""
  storage_client = storage_client_fn(project)
  filename = '{}.xml'.format(mae_result.record_id)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(posixpath.join(blob_dir, filename))
  blob.upload_from_string(mae_result.mae_xml)


def write_dtd(storage_client_fn, project, mae_dir, mae_tag_categories,
              task_name):
  """Write the DTD config file."""
  storage_client = storage_client_fn(project)
  dtd_contents = mae.generate_dtd(mae_tag_categories, task_name)
  bucket_name, blob_dir = split_gcs_name(mae_dir)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(posixpath.join(blob_dir, 'classification.dtd'))
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


def _find_transformation(all_transformations, target_info_type):
  for transformation in all_transformations:
    for info_type in transformation['infoTypes']:
      if info_type['name'] == target_info_type:
        return transformation

  raise Exception('No transformation specified for infoType %s' %
                  target_info_type)


def _get_transforms_for_types(all_transformations, info_types):
  """Get the transformations that apply to the given types."""
  included_types = set()
  transforms = []
  for info_type in info_types:
    if info_type in included_types:
      continue
    transformation = copy.deepcopy(
        _find_transformation(all_transformations, info_type))
    # Remove all non-specified infoTypes from the transformation.
    transformation['infoTypes'] = [
        it for it in transformation['infoTypes'] if it['name'] in info_types]
    transforms.append(transformation)
    for info_type in transformation['infoTypes']:
      included_types.add(info_type['name'])

  return transforms


def _generate_deid_config(all_transformations, target_columns):
  """Generate the deidentifyConfig for the deidentify API calls.

  The generated config contains a RecordTransformations.FieldTransformation
  (https://goo.gl/WrvsDB#DeidentifyTemplate.FieldTransformation) for each column
  in target_columns, where the transformation is the list of all the
  transformations in all_transformations which match the infoTypes specified for
  that column, or all the transformations if no infoTypes are specified.

  Args:
    all_transformations: The "transformations" list from the config file.
    target_columns: The "columns.inspect" list from the config file.

  Returns:
    A DeidentifyConfig.
  """
  if not all_transformations:
    return {}

  field_transformations = []
  fields_using_all_info_types = []
  for col in target_columns:
    if 'infoTypesToDeId' not in col:
      fields_using_all_info_types.append(col['name'])
      continue

    info_type_transforms = []
    info_type_transforms = _get_transforms_for_types(
        all_transformations, col['infoTypesToDeId'])

    field_transformations.append(
        {'fields': [{'name': col['name']}],
         'infoTypeTransformations': {'transformations': info_type_transforms}})

  # All inspect columns that don't specify types are included together here and
  # will use all the transformations listed in the config.
  if fields_using_all_info_types:
    field_transformations.append(
        {'fields': [{'name': f} for f in fields_using_all_info_types],
         'infoTypeTransformations': {'transformations': all_transformations}})

  return {'recordTransformations':
          {'fieldTransformations': field_transformations}}


def generate_configs(config_text, input_query=None, input_table=None,
                     bq_client=None, bq_config_fn=None):
  """Generate DLP API configs based on the input config file."""
  mae_tag_categories = {}
  per_row_types = []
  mae_key_columns = []
  cfg = json.loads(config_text, object_pairs_hook=collections.OrderedDict)
  if 'tagCategories' in cfg:
    mae_tag_categories = cfg['tagCategories']
  if 'keyColumns' in cfg:
    mae_key_columns = cfg['keyColumns']

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
  transformations = cfg['transformations'] if 'transformations' in cfg else []
  info_types = set()
  for transformation in transformations:
    for t in transformation['infoTypes']:
      # Don't include custom infoTypes in the inspect config or the DLP API
      # will complain.
      if _is_custom_type(t['name'], per_row_types, per_dataset_types):
        continue
      info_types.add(t['name'])
  inspect_config['infoTypes'] = [{'name': t} for t in info_types]

  if 'columns' not in cfg:
    raise Exception('Required section "columns" not specified in config.')
  if 'inspect' not in cfg['columns']:
    raise Exception('Required section "columns.inspect" not specified in '
                    'config.')
  target_columns = cfg['columns']['inspect']

  pass_through_columns = []
  if 'passThrough' in cfg['columns']:
    pass_through_columns = cfg['columns']['passThrough']

  deid_config = _generate_deid_config(transformations, target_columns)

  return (inspect_config, deid_config, mae_tag_categories, mae_key_columns,
          per_row_types, pass_through_columns, target_columns)


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


# These functions take a BigQuery row from either before (old) or after (new)
# google-cloud-bigquery v0.28 and convert it to a simple map from field name to
# value. This allows us to minimize special handling for supporting both
# versions, and is also necessary because the new Row object causes infinite
# recursion when Dataflow attempts to encode it.
def _convert_new_row(row):
  new_row = {}
  for field_name, value in row.items():
    new_row[field_name] = value
  return new_row


def _convert_old_row(row, field_indexes):
  new_row = {}
  for field_name, index in sorted(field_indexes.items(), key=lambda x: x[1]):
    new_row[field_name] = row[index]
  return new_row


def _generate_schema(pass_through_columns, target_columns):
  """Generate a BigQuery schema with the configured columns."""
  m = {'stringValue': 'STRING', 'integerValue': 'INTEGER',
       'floatValue': 'FLOAT', 'booleanValue': 'BOOLEAN'}
  segments = []
  for col in itertools.chain(pass_through_columns, target_columns):
    segments.append('{0}:{1}'.format(col['name'], m[col['type']]))
  return ', '.join(segments)


def _get_reads(p, input_table, input_query, bq_client, bq_config_fn,
               batch_size):
  """Read data from BigQuery.

  Args:
    p: A beam.Pipeline object.
    input_table: Table to get BigQuery data from. Only one of this and
      input_query may be set.
    input_query: Query to get BigQuery data from. Only one of this and
      input_table may be set.
    bq_client: A bigquery.Client object.
    bq_config_fn: The bigquery.job.QueryJobConfig function.
    batch_size: How many rows to send to the DLP API in each request. If this is
      1, we can use Beam's built-in BigQuerySource. Otherwise, we need to read
      directly from BigQuery and batch the rows together.
  Returns:
    A PCollection of rows from the given BigQuery input table or query.
  """
  if batch_size == 1:
    bq = None
    if input_table:
      bq = beam.io.BigQuerySource(input_table)
    else:
      bq = beam.io.BigQuerySource(query=input_query)
    # Wrap each read in a list so it's identical to a batched read of size 1.
    return (p | 'read' >> beam.io.Read(bq)
            | 'wrap' >> beam.Map(lambda read: [read]))

  old_api = hasattr(bq_client, 'run_async_query')
  query = input_query or 'SELECT * FROM [%s]' % input_table.replace(':', '.')
  results_table = None
  field_indexes = {}
  if old_api:
    query_job = bq_client.run_async_query(str(uuid.uuid4()), query)
    query_job.begin()
    query_job.result()  # Wait for the job to complete.
    query_job.destination.reload()
    results_table = query_job.destination.fetch_data()
    i = 0
    for entry in results_table.schema:
      field_indexes[entry.name] = i
      i += 1
  else:
    job_config = bq_config_fn()
    job_config.use_legacy_sql = True
    query_job = bq_client.query(query, job_config=job_config)
    results_table = query_job.result()

  buf = []
  batched_rows = []
  for row in results_table:
    if old_api:
      row = _convert_old_row(row, field_indexes)
    else:
      row = _convert_new_row(row)
    buf.append(row)
    if len(buf) >= batch_size:
      batched_rows.append(buf)
      buf = []
  if buf:
    batched_rows.append(buf)

  return p | beam.Create(batched_rows)


def run_pipeline(input_query, input_table, deid_table, findings_table,
                 mae_dir, mae_table, deid_config_file, task_name, credentials,
                 project, storage_client_fn, bq_client, bq_config_fn,
                 dlp_api_name, batch_size, pipeline_args):
  """Read the records from BigQuery, DeID them, and write them to BigQuery."""
  if (input_query is None) == (input_table is None):
    return 'Exactly one of input_query and input_table must be set.'
  config_text = ''
  if deid_config_file:
    with open(deid_config_file) as f:
      config_text = f.read()
  (inspect_config, deid_config, mae_tag_categories, mae_key_columns,
   per_row_types, pass_through_columns, target_columns) = generate_configs(
       config_text, input_query, input_table, bq_client, bq_config_fn)

  if len(target_columns) > 1 and (mae_dir or mae_table):
    raise Exception(
        'Cannot use --mae_dir or --mae_table when multiple columns are '
        'specified for "inspect" in the config file.')
  if mae_dir:
    for col in mae_key_columns:
      if not [ptc for ptc in pass_through_columns if ptc['name'] == col]:
        raise Exception(
            'Config file error: keyColumns has {}, which is not present in '
            'columns.passThrough". All key columns must be passed through '
            'un-transformed to allow for evals.'.format(col))

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  reads = _get_reads(p, input_table, input_query, bq_client, bq_config_fn,
                     batch_size)

  inspect_data = None
  if findings_table or mae_dir or mae_table:
    inspect_data = (reads | 'inspect' >> beam.FlatMap(
        inspect, credentials, project, inspect_config,
        pass_through_columns, target_columns, per_row_types, dlp_api_name))
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
  mae_data = None
  if mae_dir or mae_table:
    if not mae_key_columns:
      raise Exception('"mae.keyColumns" not specified in the config. Please '
                      'specify a list of columns that will be used as the '
                      'primary key for identifying MAE results.')
    mae_data = (inspect_data | 'generate_mae' >> beam.Map(
        mae.generate_mae, task_name, mae_tag_categories, mae_key_columns))
  if mae_dir:
    _ = (mae_data | 'write_mae_to_gcs' >> beam.Map(
        write_mae, storage_client_fn, project, mae_dir))
  if mae_table:
    _ = (mae_data | 'mae_to_bq_row' >> beam.Map(mae_to_bq_row) |
         'write_mae_to_bq' >> beam.io.Write(beam.io.BigQuerySink(
             mae_table, schema=('record_id:STRING,xml:STRING'),
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))

  if deid_table:
    if not deid_config_file:
      return ['Must set --deid_config_file when --deid_table is set.']
    # Call deidentify and write the result to BigQuery.
    schema = _generate_schema(pass_through_columns, target_columns)
    _ = (reads
         | 'deid' >> beam.FlatMap(
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
      help=('BigQuery query to provide input data. Must yield rows with all '
            'fields specified in the "columns" section of the config file.'))
  parser.add_argument(
      '--input_table', type=str, required=False,
      help=('BigQuery table to provide input data. Must have rows with all '
            'fields specified in the "columns" section of the config file.'))
  parser.add_argument('--deid_table', type=str, required=False,
                      help='BigQuery table to store DeID\'d data.')
  parser.add_argument('--findings_table', type=str, required=False,
                      help='BigQuery table to store DeID summary data.')
  parser.add_argument('--mae_dir', type=str, required=False,
                      help=('GCS directory to store inspect() results in MAE '
                            'format.'))
  parser.add_argument('--mae_table', type=str, required=False,
                      help='BQ table to store inspect() results in MAE format.')
  parser.add_argument('--mae_task_name', type=str, required=False,
                      help='Task name to use in generated MAE files.',
                      default='InspectPhiTask')
  parser.add_argument('--deid_config_file', type=str, required=False,
                      help='Path to a json file holding the config to use.')
  parser.add_argument('--project', type=str, required=True,
                      help='GCP project to run as.')
  parser.add_argument('--dlp_api_name', type=str, required=False,
                      help='Name to use in the DLP API url.',
                      default='dlp')
  parser.add_argument('--batch_size', type=int, required=False,
                      help='How many rows to send in each DLP API call.',
                      default=1)
