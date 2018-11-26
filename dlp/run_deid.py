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

import argparse
from datetime import datetime
import logging
import os
import sys

from dlp import run_deid_lib
import google.auth
from google.cloud import bigquery
from google.cloud import storage


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Run Data Loss Prevention (DLP) DeID on Google Cloud.')
  run_deid_lib.add_all_args(parser)
  args, pipeline_args = parser.parse_known_args(sys.argv[1:])

  var = 'GOOGLE_APPLICATION_CREDENTIALS'
  if var not in os.environ or not os.environ[var]:
    raise Exception('You must specify service account credentials in the '
                    'GOOGLE_APPLICATION_CREDENTIALS environment variable.')
  _, default_project = google.auth.default()

  # Parse --project and re-add it to the pipeline args, swapping it out for the
  # default if it's not set.
  project = args.project
  if not project:
    project = default_project
  pipeline_args += ['--project', project]

  bq_client = bigquery.Client(project)
  bq_config_fn = None
  if hasattr(bigquery.job, 'QueryJobConfig'):
    bq_config_fn = bigquery.job.QueryJobConfig

  if not args.deid_config_file:
    raise Exception('Must provide DeID Config.')
  deid_config_json = run_deid_lib.parse_config_file(args.deid_config_file)
  timestamp = datetime.utcnow()

  errors = run_deid_lib.run_pipeline(
      args.input_query, args.input_table, args.deid_table, args.findings_table,
      args.mae_dir, args.mae_table, deid_config_json, args.mae_task_name,
      project, storage.Client, bq_client, bq_config_fn, args.dlp_api_name,
      args.batch_size, args.dtd_dir, args.input_csv, args.output_csv, timestamp,
      pipeline_args)

  if errors:
    logging.error(errors)
    return 1

  logging.info('Ran DLP API DeID.')

if __name__ == '__main__':
  main()
