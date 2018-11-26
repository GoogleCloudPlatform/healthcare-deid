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

"""Evaluate DeID findings against a 'golden' baseline.

All input/output files should be on Google Cloud Storage.

Requires Apache Beam client and Google Python API Client:
pip install --upgrade apache_beam
pip install --upgrade google-api-python-client
"""

from __future__ import absolute_import

import argparse
import logging
import sys

from eval import run_pipeline_lib


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Evaluate DeID findings on Google Cloud.')
  run_pipeline_lib.add_all_args(parser)
  args, pipeline_args = parser.parse_known_args(sys.argv[1:])

  errors = run_pipeline_lib.run_pipeline(
      args.mae_input_pattern, args.mae_golden_dir, args.results_dir,
      args.mae_input_query, args.mae_golden_table,
      args.write_per_note_stats_to_gcs, args.results_table,
      args.per_note_results_table, args.debug_output_table,
      args.types_to_ignore or [], None, pipeline_args)

  if errors:
    logging.error(errors)
    return 1

  logging.info('Ran eval.')

if __name__ == '__main__':
  main()
