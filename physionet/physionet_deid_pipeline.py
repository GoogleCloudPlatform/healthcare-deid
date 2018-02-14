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

"""Take data from BigQuery, run PhysioNet DeID, and write to BigQuery."""

from __future__ import absolute_import

import argparse
import logging
import posixpath
import sys

from physionet import bigquery_to_gcs_lib
from physionet import gcs_to_bigquery_lib
from physionet import physionet_to_mae_lib
from physionet import run_deid_lib
from google.cloud import storage


def add_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--gcs_working_directory', type=str, required=True,
                      help='GCS directory to write intermediate files to.')


def main():
  logging.getLogger().setLevel(logging.INFO)

  # Pre-parse the args to check if we're running the gcs-to-bigquery pipeline,
  # the physionet-to-mae pipeline, or both, so we can add the correct args.
  pre_parser = argparse.ArgumentParser(
      description=('Run Physionet DeID on Google Cloud Platform.'),
      conflict_handler='resolve')
  pre_parser.add_argument('--output_table', type=str, required=False,
                          help='BigQuery table to store output data.')
  pre_parser.add_argument('--mae_output_dir', type=str, required=False,
                          help='GCS directory to store output data.')
  args, _ = pre_parser.parse_known_args(sys.argv[1:])
  if not args.output_table and not args.mae_output_dir:
    raise Exception(
        'Must specify at least one of --output_table or --mae_output_dir')
  run_mae = args.mae_output_dir is not None
  run_gcs_to_bq = args.output_table is not None

  parser = argparse.ArgumentParser(
      description=('Run Physionet DeID on Google Cloud Platform.'))
  add_args(parser)
  if args.output_table:
    gcs_to_bigquery_lib.add_args(parser)
  if run_mae:
    # run_deid_lib also adds the --project flag, so don't add it here.
    physionet_to_mae_lib.add_args(parser, include_project=False)
  run_deid_lib.add_args(parser)
  bigquery_to_gcs_lib.add_args(parser)
  # --project is used both as a local arg (in run_deid) and a pipeline arg (in
  # the bq<->gcs pipelines), so parse it, then add it to pipeline_args as well.
  args, pipeline_args = parser.parse_known_args(sys.argv[1:])
  pipeline_args += ['--project', args.project]

  if run_mae and not args.include_original_in_pn_output:
    raise Exception('--include_original_in_pn_output must be true when '
                    '--mae_output_dir is set.')

  input_file = posixpath.join(args.gcs_working_directory, 'input', 'file')
  bigquery_to_gcs_lib.run_pipeline(args.input_query, input_file, pipeline_args)

  logging.info('Copied data from BigQuery to %s', input_file)

  output_dir = posixpath.join(args.gcs_working_directory, 'output')

  storage_client = storage.Client(args.project)

  run_deid_lib.run_pipeline(
      input_file + '-?????-of-?????', output_dir, args.config_file,
      args.project, posixpath.join(args.gcs_working_directory, 'logs'),
      args.dict_directory, args.lists_directory, args.max_num_threads,
      args.include_original_in_pn_output, storage_client=storage_client)

  logging.info('Ran PhysioNet DeID and put output in %s', output_dir)

  if run_gcs_to_bq:
    gcs_to_bigquery_lib.run_pipeline(
        posixpath.join(output_dir, 'file-?????-of-?????'),
        args.output_table, pipeline_args)
    logging.info('Wrote output to %s.', args.output_table)

  if run_mae:
    physionet_to_mae_lib.run_pipeline(
        posixpath.join(output_dir, 'file-?????-of-?????'), args.mae_output_dir,
        args.mae_task_name, args.project, pipeline_args)
    logging.info('Wrote output to %s.', args.mae_output_dir)

  logging.info('Run complete.')

if __name__ == '__main__':
  main()
