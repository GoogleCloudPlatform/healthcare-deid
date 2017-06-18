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
import os
import sys

from healthcare_deid.physionet import bigquery_to_gcs_lib
from healthcare_deid.physionet import gcs_to_bigquery_lib
from healthcare_deid.physionet import run_deid_lib


def add_args(parser):
  """Add command-line arguments to the program."""
  parser.add_argument('--gcs_working_directory', type=str, required=True,
                      help='GCS directory to write intermediate files to.')


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description=('Run Physionet DeID on Google Cloud Platform.'))
  add_args(parser)
  gcs_to_bigquery_lib.add_args(parser)
  run_deid_lib.add_args(parser)
  bigquery_to_gcs_lib.add_args(parser)
  # --project is used both as a local arg (in run_deid) and a pipeline arg (in
  # the bq<->gcs pipelines), so parse it, then add it to pipeline_args as well.
  args, pipeline_args = parser.parse_known_args(sys.argv[1:])
  pipeline_args += ['--project', args.project]

  input_file = os.path.join(args.gcs_working_directory, 'input', 'file')
  bigquery_to_gcs_lib.run_pipeline(args.input_query, input_file, pipeline_args)

  logging.info('Copied data from BigQuery to %s', input_file)

  output_dir = os.path.join(args.gcs_working_directory, 'output')
  run_deid_lib.run_pipeline(input_file + '-?????-of-?????', output_dir,
                            args.config_file, args.project,
                            os.path.join(args.gcs_working_directory, 'logs'),
                            dict_directory=args.dict_directory,
                            lists_directory=args.lists_directory,
                            max_num_threads=args.max_num_threads)

  logging.info('Ran PhysioNet DeID and put output in %s', output_dir)

  gcs_to_bigquery_lib.run_pipeline(
      os.path.join(output_dir, 'file-?????-of-?????'),
      args.output_table, pipeline_args)

  logging.info('Run complete. Wrote output to %s.', args.output_table)

if __name__ == '__main__':
  main()
