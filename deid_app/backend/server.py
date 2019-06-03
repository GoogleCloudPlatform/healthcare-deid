# Copyright 2018 Google Inc. All rights reserved.
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

"""Server to run Google Data Loss Prevention API DeID.

For now, no authentication is implemented, to be run on localhost.

Requires Apache Beam client, Flask, Google Python API Client:
pip install --upgrade apache_beam
pip install --upgrade flask
pip install --upgrade google-api-python-client
"""

from __future__ import absolute_import

from datetime import datetime
import json
import logging
import posixpath

from apiclient import discovery
import enum
import flask
from common import gcsutil
from common import unicodecsv
from deid_app.backend import config
from deid_app.backend import model
from dlp import run_deid_lib
from eval import run_pipeline_lib as eval_lib
import jsonschema

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import exceptions


logging.getLogger().setLevel(logging.INFO)

app = flask.Flask(__name__)
app.config.from_object(config.Config)
bq_client = bigquery.Client(app.config['PROJECT_ID'])

with app.app_context():
  model.init_app(app)

PATIENT_ID = 'patient_id'
RECORD_NUM = 'record_number'
NOTE = 'note'
FINDINGS = 'findings'
EXPECTED_CSV_SCHEMA = [RECORD_NUM, PATIENT_ID, NOTE]
EXPECTED_FINDINGS_SCHEMA = [RECORD_NUM, PATIENT_ID, FINDINGS,
                            run_deid_lib.DLP_FINDINGS_TIMESTAMP]

EXPECTED_OUTPUT_SCHEMA = (EXPECTED_CSV_SCHEMA +
                          [run_deid_lib.DLP_DEID_TIMESTAMP])
CSV_FIELD_TYPE = {
    RECORD_NUM: 'INT64',
    PATIENT_ID: 'STRING',
    NOTE: 'STRING',
}

deid_schema = {
    'type':
        'object',
    'properties': {
        'name': {
            'type': 'string'
        },
        'inputMethod': {
            'type': 'string'
        },
        'inputInfo': {
            'type': 'string'
        },
        'outputMethod': {
            'type': 'string'
        },
        'outputInfo': {
            'type': 'string'
        },
        'findingsTable': {
            'type': 'string'
        },
        'maeTable': {
            'type': 'string'
        },
        'maeDir': {
            'type': 'string'
        },
        'batchSize': {
            'type': 'number'
        },
    },
    'required': [
        'name',
        'inputMethod',
        'inputInfo',
        'outputMethod',
        'outputInfo',
    ],
}

eval_pipeline_shema = {
    'type': 'object',
    'properties': {
        'name': {'type': 'string'},
        'input': {
            'type': 'object',
            'properties': {
                'gcs': {
                    'type': 'object',
                    'properties': {
                        'pattern': {'type': 'string'},
                        'golden': {'type': 'string'},
                    },
                    'required': [
                        'pattern',
                        'golden',
                    ],
                },
                'bigquery': {
                    'type': 'object',
                    'properties': {
                        'query': {'type': 'string'},
                        'golden': {'type': 'string'},
                    },
                    'required': [
                        'query',
                        'golden',
                    ],
                },
            },
            'oneOf': [
                {'required': ['gcs']},
                {'required': ['bigquery']},
            ],
        },
        'output': {
            'type': 'object',
            'properties': {
                'gcs': {
                    'type': 'object',
                    'properties': {
                        'dir': {'type': 'string'},
                        'debug': {'type': 'boolean'},
                    },
                    'required': [
                        'dir',
                        'debug',
                    ],
                },
                'bigquery': {
                    'type': 'object',
                    'properties': {
                        'stats': {'type': 'string'},
                        'debug': {'type': 'string'},
                        'perNote': {'type': 'string'},
                    },
                    'required': [
                        'stats',
                        'debug',
                    ],
                },
            },
            'anyOf': [
                {'required': ['gcs']},
                {'required': ['bigquery']},
            ],
        },
        'ignoreTypes': {
            'type': 'array',
            'items': {'type': 'string'},
        },
    },
    'required': [
        'name',
        'input',
        'output',
    ],
}

dlp_image_demo_schema = {
    'type': 'object',
    'properties': {
        'type': {
            'type': 'string',
            'enum': ['image/jpeg',
                     'image/bmp',
                     'image/png',
                     'image/svg',
                     'text/plain',
                    ]
        },
        'data': {'type': 'string'},
    },
    'required': [
        'data',
        'type'
    ],
}

bq_table_schema = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'mode': {
                'type': 'string',
                'enum': ['NULLABLE',
                         'REQUIRED',
                         'REPEATED',
                        ]
            },
            'name': {'type': 'string'},
            'type': {'type': 'string'},
        },
        'required': [
            'name',
            'type',
        ],
    },
}

dlp_image_redaction_configs = [{
    'redactionColor': {
        'blue': 0.1,
        'green': 0.1,
        'red': 0.8
    },
    'redactAllText': 'true'
}]


def get_bq_dataset(dataset_id):
  """Returns a dataset instance from BigQuery."""
  dataset_ref = bq_client.dataset(dataset_id)
  try:
    dataset = bq_client.get_dataset(dataset_ref)
  except exceptions.NotFound as e:
    raise e

  return dataset


def append_project(table_name):
  """formats a table name to 'project:dataset.table'."""
  return '{}:{}'.format(app.config['PROJECT_ID'], table_name)


def get_bq_table(dataset_id, table_id):
  """Return a table instance from BigQuery."""
  dataset_ref = bq_client.dataset(dataset_id)
  table_ref = dataset_ref.table(table_id)
  try:
    return bq_client.get_table(table_ref)
  except exceptions.NotFound as e:
    raise e


def get_bq_rows(query):
  """Returns a BigQuery query as a list of rows."""
  query_job = bq_client.query(query)

  res = query_job.result()  # blocks until query is done.
  return [dict(list(row.items())) for row in res]


def verify_bq_table(dataset_id, table_id, expected_schema):
  """Verifies that a table exists and has an expected schema.

  Args:
    dataset_id: The name of the BigQuery dataset.
    table_id: The name of the BigQuery table.
    expected_schema: A list of the expected names of columns.
  Raises:
    exceptions.NotFound: If the table does not exist in BigQuery.
  Returns:
    A boolean of the verification status.
  """
  table = get_bq_table(dataset_id, table_id)
  table_headers = [col.name for col in table.schema]
  return set(table_headers) == set(expected_schema)


def verify_gcs_path(path):
  """Verifies that a GCS path exists.

  Args:
    path: A string that represents the target path.
  Returns:
    A boolean of the verification status.
  """
  storage_client = storage.Client()
  path_info = gcsutil.GcsFileName.from_path(path)
  try:
    bucket = storage_client.get_bucket(path_info.bucket)
  except exceptions.NotFound:
    return False
  return storage.Blob(bucket=bucket,
                      name=path_info.blob).exists(storage_client)


@app.route('/')
@app.route('/index')
@app.route('/api')
def index():
  return flask.jsonify(data='Deid backend server', status=200), 200


@app.route('/api/project')
def get_project():
  return flask.jsonify(project=app.config['PROJECT_ID']), 200


@app.route('/api/datasets')
def get_datasets():
  datasets = list(bq_client.list_datasets())
  dataset_ids = [dataset.dataset_id for dataset in datasets]
  return flask.jsonify(datasets=dataset_ids), 200


@app.route('/api/datasets/<dataset_id>', methods=['POST', 'DELETE'])
def manage_dataset(dataset_id):
  """Create and delete datasets from BigQuery."""
  dataset_ref = bq_client.dataset(dataset_id)
  method = flask.request.method

  if method == 'POST':
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'
    payload = flask.request.json
    if payload:
      dataset.location = payload.get('location') or dataset.location
      dataset.description = payload.get('description') or ''

    try:
      dataset = bq_client.create_dataset(dataset)
    except exceptions.Conflict as e:
      error_msg = 'There already exists a dataset with this name'
      return flask.jsonify(error=e.code, text=error_msg), e.code

    return flask.jsonify(result='success'), 200
  elif method == 'DELETE':
    try:
      bq_client.delete_dataset(dataset_ref, delete_contents=True)
    except exceptions.NotFound as e:
      error_msg = 'Dataset Does not exist'
      return flask.jsonify(error=e.code, text=error_msg), e.code

    return flask.jsonify(result='success'), 200


@app.route('/api/datasets/<dataset_id>/tables')
def get_tables(dataset_id):
  """Get table names for a provided dataset."""
  try:
    get_bq_dataset(dataset_id)
  except exceptions.NotFound as e:
    return flask.jsonify(error=e.code, text=e.message), e.code

  dataset_ref = bq_client.dataset(dataset_id)
  tables = list(bq_client.list_tables(dataset_ref))
  table_ids = [table.table_id for table in tables]
  return flask.jsonify(dataset=dataset_id, tables=table_ids), 200


@app.route('/api/datasets/<dataset_id>/tables/<table_id>',
           methods=['POST'])
def manage_tables(dataset_id, table_id):
  """Create tables in datasets in BigQuery."""
  try:
    get_bq_dataset(dataset_id)
  except exceptions.NotFound as e:
    return flask.jsonify(error=e.code, text=e.message), e.code

  table_ref = bq_client.dataset(dataset_id).table(table_id)
  try:
    jsonschema.validate(flask.request.json, bq_table_schema)
  except jsonschema.ValidationError:
    error_msg = 'unable to validate provided payload.'
    return flask.jsonify(error=400, text=error_msg), 400

  schema = [bigquery.SchemaField(field['name'], field['type'],
                                 field.get('mode') or 'NULLABLE')
            for field in flask.request.json]
  table = bigquery.Table(table_ref, schema=schema)
  try:
    table = bq_client.create_table(table)
  except exceptions.GoogleAPIError as e:
    return flask.jsonify(error=e.message), 400

  return flask.jsonify(result='success'), 200


@app.route('/api/deidentify/<job_id>/metadata')
def get_job_metadata(job_id):
  """Gets the list of patient_id, record_num for a given job."""
  job = model.DeidJobTable.query.get(job_id)
  if not job:
    error_msg = 'Job does not exist'
    return flask.jsonify(text=error_msg, error=404), 404

  try:
    orig_data = get_bq_rows(job.original_query)
  except exceptions.NotFound as e:
    return flask.jsonify(text=e.message, error=e.code), e.code

  # The metadata is only the patient_id and record_number
  metadata = [{
      'patientId': row[PATIENT_ID],
      'recordNumber': row[RECORD_NUM],
  } for row in orig_data]

  return flask.jsonify(notesMetadata=metadata), 200


class NoteAnnotation(enum.Enum):
  HIGHLIGHTED = 0
  UNHIGHLIGHTED = 1


class NoteHighlight(object):
  """Represents a chunk of a note that was deidentified and its metadata.

  A note is split into a list of NoteHighlight objects. Each NoteHighlight can
  indicate that the note is highlighted. In that case, the NoteHighlight should
  contain a replacement and color information for the chunk that should be
  highlighted.

  Attributes:
    annotation: A string representation of a NoteAnnotation that indicates
      whether this range is highlighted or not.
    quote: A string that represents the original chunk of the note.
    replacement: a string that indicates the value to replace a highlighted
      chunk with.
    begin: An integer of the index of this chunk compared to the rest of the
      note.
    length: An integer with the length of the chunk.
    color: A string that represents the color to be associated with a
      highlighted chunk.
  """

  def __init__(self, annotation, quote, replacement, begin, length, color):
    """Initializes a NoteHighlight object with all attributed."""
    self.annotation = annotation
    self.quote = quote
    self.replacement = replacement
    self.begin = begin
    self.length = length
    self.color = color


@app.route('/api/deidentify/<job_id>/note/<record_number>')
def get_note_highlights(job_id, record_number):
  """returns a list of ranges to highlight."""
  job = model.DeidJobTable.query.get(job_id)
  if not job:
    error_msg = 'Job does not exist'
    return flask.jsonify(text=error_msg, error=404), 404

  orig_query = job.original_query + ' where {}={}'.format(
      RECORD_NUM, record_number)
  findings_query = 'select findings from {} where {}=\'{}\' and {}={}'.format(
      job.findings_table, run_deid_lib.DLP_FINDINGS_TIMESTAMP, job.timestamp,
      RECORD_NUM, record_number)

  try:
    orig_row = get_bq_rows(orig_query)
    findings_data = get_bq_rows(findings_query)
  except exceptions.NotFound as e:
    return flask.jsonify(text=e.message, error=e.code), e.code

  if len(findings_data) != 1 or len(orig_row) != 1:
    error_msg = 'Selected record number does not exist or is not unique'
    return flask.jsonify(text=error_msg, error=400), 400

  findings = json.loads(findings_data[0]['findings'])['findings']
  note = orig_row[0][NOTE]
  res = []

  findings.sort(key=lambda x: int(x['location']['codepointRange']['start']))

  # Assumption:
  #   The location attribute always has a codepointRange field that indicates
  #     the offset of the identified string in unicode format.
  #   The original text is always replaced with its detected info type.
  offset = 0
  for finding in findings:
    location = finding['location']['codepointRange']
    start, end = int(location['start']), int(location['end'])

    # This check handles overlapping findings. For now, this ensures that the
    # code doesn't crash in that case.
    if start < offset:
      continue
    color = 'Bisque'
    # For every detected text, there is 2 chunks that can be created: the one
    # preceding the detected text (unhighlighted) and the highlighted one (the
    # detected text).

    # The unhighlighted chunk
    first_quote = note[offset:start]
    first_replacement = first_quote
    first_annotation = NoteAnnotation.UNHIGHLIGHTED
    first_length = start - offset - 1
    first_chunk = NoteHighlight(first_annotation.name, first_quote,
                                first_replacement, offset, first_length, color)
    res.append(first_chunk.__dict__)  # dict is json serializable.

    # The highlighted chunk
    second_quote = note[start:end]
    second_replacement = finding['infoType']['name']
    second_annotation = NoteAnnotation.HIGHLIGHTED
    second_length = end - start
    second_chunk = NoteHighlight(second_annotation.name, second_quote,
                                 second_replacement, start, second_length,
                                 color)
    res.append(second_chunk.__dict__)

    offset = end

  # If the last info type isn't at the end of the note, then there is some
  # leftover unhighlighted chunk.
  final_chunk = NoteHighlight(NoteAnnotation.UNHIGHLIGHTED.name, note[offset:],
                              '', offset, len(note) - offset, '')
  res.append(final_chunk.__dict__)

  return flask.jsonify(data=res), 200


@app.route('/api/deidentify', methods=['GET', 'POST'])
def deidentify():
  """run dlp pipeline."""
  if flask.request.method == 'GET':
    jobs, offset = model.get_list(model.DeidJobTable)
    result = [{
        'id': job['id'],
        'name': job['name'],
        'originalQuery': job['original_query'],
        'deidTable': job['deid_table'],
        'status': job['status'],
        'logTrace': job['log_trace'],
        'timestamp': job['timestamp'],
    } for job in jobs]
    return flask.jsonify(jobs=result, offset=offset), 200


  try:
    jsonschema.validate(flask.request.json, deid_schema)
  except jsonschema.ValidationError:
    error_msg = 'unable to validate provided payload.'
    return flask.jsonify(error=400, text=error_msg), 400

  job_data = {
      'name': flask.request.json['name'],
      'timestamp': datetime.utcnow(),
  }
  (input_query, input_table, deid_table, findings_table, mae_dir, mae_table,
   mae_task_name, batch_size, dtd_dir, input_csv, output_csv) = (
       None, None, None, None, None,
       None, None, None, None, None, None)

  request = flask.request
  # determine input
  input_method, input_info = (request.json['inputMethod'],
                              request.json['inputInfo'])
  if input_method == 'input_table':
    input_table = input_info
    try:
      dataset, table = input_table.split('.')
      if not verify_bq_table(dataset, table, EXPECTED_CSV_SCHEMA):
        error_msg = ('input table schema does not match the expected one. '
                     'Expecting: {}'.format(', '.join(EXPECTED_CSV_SCHEMA)))
        return flask.jsonify(error=400, text=error_msg), 400
    except exceptions.NotFound:
      return flask.jsonify(error=400, text='unable to locate input data'), 400
    job_data['original_query'] = 'SELECT * FROM {}'.format(input_table)
  elif input_method == 'input_query':
    input_query = input_info
    job_data['original_query'] = input_query
    try:
      get_bq_rows(input_query)
    except exceptions.BadRequest:
      error_msg = 'invalid input query'
      return flask.jsonify(error=400, text=error_msg), 400
  elif input_method == 'input_csv':
    input_csv = input_info
  else:
    error_msg = 'wrong input method provided'
    return flask.jsonify(error=400, text=error_msg), 400

  # Determine output
  output_method, output_info = (request.json['outputMethod'],
                                request.json['outputInfo'])
  job_data['deid_table'] = output_info
  if output_method == 'deid_table':
    deid_table = output_info
    dataset, table = deid_table.split('.')
    try:
      if not verify_bq_table(dataset, table, EXPECTED_OUTPUT_SCHEMA):
        error_msg = ('output table schema does not match the expected one. '
                     'Expecting: {}'.format(', '.join(EXPECTED_OUTPUT_SCHEMA)))
        return flask.jsonify(error=400, text=error_msg), 400
    except exceptions.NotFound:
      # if table not found, a new one will be created
      pass
  elif output_method == 'output_csv':
    output_csv = output_info
  else:
    error_msg = 'wrong output method provided'
    return flask.jsonify(error=400, text=error_msg), 400

  deid_config_json = run_deid_lib.parse_config_file(
      app.config['DEID_CONFIG_FILE'])

  findings_table = request.json.get('findingsTable')
  job_data['findings_table'] = findings_table
  try:
    dataset, table = findings_table.split('.')
    if not verify_bq_table(dataset, table, EXPECTED_FINDINGS_SCHEMA):
      error_msg = ('findings table schema does not match the expected one. '
                   'Expecting: {}'.format(', '.join(EXPECTED_FINDINGS_SCHEMA)))
      return flask.jsonify(error=400, text=error_msg), 400
  except exceptions.NotFound:
    # if table not found, a new one will be created
    pass

  mae_table = request.json.get('maeTable')
  mae_dir = request.json.get('maeDir')
  batch_size = request.json.get('batchSize') or 1

  pipeline_args = ['--project', app.config['PROJECT_ID']]

  deid_job = model.create(model.DeidJobTable, job_data)
  errors = run_deid_lib.run_pipeline(
      input_query, input_table, deid_table, findings_table, mae_dir, mae_table,
      deid_config_json, mae_task_name, app.config['PROJECT_ID'], storage.Client,
      bq_client, bigquery.job.QueryJobConfig, app.config['DLP_API_NAME'],
      batch_size, dtd_dir, input_csv, output_csv, deid_job.timestamp,
      pipeline_args)

  if errors:
    deid_job.update(status=400, log_trace=errors)
    return flask.jsonify(error=400, text=errors), 400

  deid_job.update(status=200)
  return flask.jsonify(result='success'), 200


@app.route('/api/eval', methods=['GET', 'POST'])
def evaluate():
  """Run evaluation pipeline."""
  if flask.request.method == 'GET':
    jobs, offset = model.get_list(model.EvalJobTable)
    return flask.jsonify(jobs=jobs, offset=offset), 200

  # Process POST requests.
  try:
    jsonschema.validate(flask.request.json, eval_pipeline_shema)
  except jsonschema.ValidationError:
    error_msg = 'unable to validate provided payload.'
    return flask.jsonify(error=400, text=error_msg), 400

  (mae_input_pattern, mae_golden_dir, results_dir, mae_input_query,
   mae_golden_table, write_per_note_stats_to_gcs, results_table,
   per_note_results_table, debug_output_table, types_to_ignore) = (
       None, None, None, None, None, None, None, None, None, None)

  job_data = {
      'name': flask.request.json['name'],
      'timestamp': datetime.utcnow(),
  }

  # Get input info
  input_json = flask.request.json['input']
  gcs_input, bq_input = input_json.get('gcs'), input_json.get('bigquery')
  if gcs_input:

    mae_input_pattern = job_data['findings'] = gcs_input['pattern'] + '*.xml'
    mae_golden_dir = job_data['goldens'] = gcs_input['golden']
  if bq_input:
    job_data['findings'] = bq_input['query']
    mae_input_query = append_project(job_data['findings'])
    job_data['goldens'] = bq_input['golden']
    mae_golden_table = append_project(job_data['goldens'])
    try:
      findings_dataset, findings_table = job_data['findings'].split('.')
      get_bq_table(findings_dataset, findings_table)
      golden_dataset, golden_table = job_data['golden'].split('.')
      get_bq_table(golden_dataset, golden_table)
    except exceptions.NotFound:
      error_msg = 'unable to locate input BigQuery tables'
      return flask.jsonify(error=400, text=error_msg), 400

  # Get output info
  output_json = flask.request.json['output']
  gcs_output, bq_output = output_json.get('gcs'), output_json.get('bigquery')
  if gcs_output:
    results_dir = job_data['stats'] = gcs_output['dir']
    write_per_note_stats_to_gcs = gcs_output['debug']
    if write_per_note_stats_to_gcs:
      job_data['debug'] = gcs_output['dir']
  if bq_output:
    job_data['stats'] = bq_output['stats']
    results_table = append_project(job_data['stats'])
    job_data['debug'] = bq_output['debug']
    debug_output_table = append_project(job_data['debug'])
    if bq_output.get('perNote'):
      per_note_results_table = append_project(bq_output.get('perNote'))

  # Get types to ignore
  types_to_ignore = flask.request.json.get('ignoreTypes') or []

  # Get pipeline args
  pipeline_args = []

  eval_job = model.create(model.EvalJobTable, job_data)
  errors = eval_lib.run_pipeline(mae_input_pattern, mae_golden_dir, results_dir,
                                 mae_input_query, mae_golden_table,
                                 write_per_note_stats_to_gcs, results_table,
                                 per_note_results_table, debug_output_table,
                                 types_to_ignore, eval_job.timestamp,
                                 pipeline_args)

  if errors:
    eval_job.update(status=400, log_trace=errors)
    return flask.jsonify(error=400, text=errors), 400

  eval_job.update(status=200)
  return flask.jsonify(result='success'), 200


@app.route('/api/eval/stats/<job_id>', methods=['GET'])
def get_eval_stats(job_id):
  """Returns the evaluation statistics of an EvalJob."""
  job = model.EvalJobTable.query.get(job_id)
  if not job:
    error_msg = 'evaluation job does not exist'
    return flask.jsonify(text=error_msg, error=404), 404

  if job.status != 200:
    error_msg = 'selected job did not finish successfully'
    return flask.jsonify(text=error_msg, error=400), 400
  stats = job.stats
  if stats.startswith('gs://'):
    st_client = storage.Client()
    filename = gcsutil.GcsFileName.from_path(
        posixpath.join(stats, 'aggregate_results.txt'))
    bucket = st_client.lookup_bucket(filename.bucket)
    if not bucket:
      error_msg = 'stats bucket was not found'
      return flask.jsonify(error=404, text=error_msg), 404
    blob = bucket.blob(filename.blob)
    contents = blob.download_as_string()
    stats_rows = eval_lib.format_aggregate_text_for_bq(contents,
                                                       str(job.timestamp))
  else:
    query = 'SELECT * FROM {} where timestamp = \'{}\''.format(job.stats,
                                                               job.timestamp)
    try:
      stats_rows = get_bq_rows(query)
    except exceptions.NotFound as e:
      return flask.jsonify(error=e.code, text=e.message), e.code

  # Change the key format from snake_case into camelCase and remove any keys
  # with None values
  result = [
      dict(  # pylint: disable=g-complex-comprehension
          (k, v) for (k, v) in {
              'infoType': stat.get('info_type'),
              'recall': stat.get('recall'),
              'precision': stat.get('precision'),
              'fScore': stat.get('f_score'),
              'truePositives': stat.get('true_positives'),
              'falsePositives': stat.get('false_positives'),
              'falseNegatives': stat.get('false_negatives'),
              'timestamp': stat.get('timestamp'),
          }.items() if v is not None) for stat in stats_rows
  ]
  return flask.jsonify(stats=result), 200


@app.route('/api/deidentify/upload/table', methods=['POST'])
def upload_dlp_csv():
  """Uploads a csv table to BigQuery dataset.

  The table is expected to have the config schema:
    [RECORD_NUM, PATIENT_ID, NOTE].
  Returns:
    A flask response indicating the result of the operation.
  """
  csv_file = flask.request.files.get('csv')
  if not csv_file:
    return flask.jsonify(error=400, text='no file provided'), 400

  form = flask.request.form
  dataset_id, table_id = form.get('dataset'), form.get('table')
  if not dataset_id or not table_id:
    return flask.jsonify(error=400, text='table or dataset not provided'), 400

  csv_iter = unicodecsv.UnicodeReader(csv_file)
  try:
    headers = csv_iter.next()
  except StopIteration:
    return flask.jsonify(error=400, text='file is empty'), 400
  if set(headers) != set(EXPECTED_CSV_SCHEMA):
    return flask.jsonify(
        error=400, text='expected table schema is: {}'.format(
            ', '.join(EXPECTED_CSV_SCHEMA))), 400

  try:
    if not verify_bq_table(dataset_id, table_id, EXPECTED_CSV_SCHEMA):
      error_msg = ('selected table schema does not match the expected one. '
                   'Expecting: {}'.format(', '.join(EXPECTED_CSV_SCHEMA)))
      return flask.jsonify(error=400, text=error_msg), 400
  except exceptions.NotFound:
    # Table not found, create it
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = [bigquery.schema.SchemaField(
        name=col, field_type=CSV_FIELD_TYPE[col])
              for col in headers]
    table = bq_client.create_table(bigquery.table.Table(table_ref, schema))

  rows = [
      {header: entry for header, entry in zip(headers, row)}
      for row in csv_iter]
  if not rows:
    return flask.jsonify(error=400, text='no rows provided'), 400

  bq_client.insert_rows_json(table, rows)
  return flask.jsonify(res='success'), 200


@app.route('/api/demo/image', methods=['POST'])
def deid_image():
  """redact all text from provided image."""
  request = flask.request
  try:
    jsonschema.validate(request.json, dlp_image_demo_schema)
  except jsonschema.ValidationError:
    error_msg = 'unable to validate provided parameter'
    return flask.jsonify(error=400, text=error_msg), 400

  dlp = discovery.build(app.config['DLP_API_NAME'], 'v2',
                        cache_discovery=False)

  def get_image_type(req_type):
    """change image type format to match what's expected from dlp."""
    if req_type == 'image/jpeg':
      return 'IMAGE_JPEG'
    elif req_type == 'image/bmp':
      return 'IMAGE_BMP'
    elif req_type == 'image/png':
      return 'IMAGE_PNG'
    elif req_type == 'image/svg':
      return 'IMAGE/SVG'
    else:
      return None

  byte_item = {
      'type': get_image_type(request.json['type']),
      'data': request.json['data'],
  }

  body = {
      'byteItem': byte_item,
      'imageRedactionConfigs': dlp_image_redaction_configs,
  }

  projects = dlp.projects()
  image = projects.image()
  parent = 'projects/{0}'.format(app.config['PROJECT_ID'])
  response = image.redact(body=body, parent=parent).execute()

  return flask.jsonify(redactedByteStream=response['redactedImage'], status=200)

if __name__ == '__main__':
  app.run(threaded=True)
