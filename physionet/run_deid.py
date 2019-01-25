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

"""Stand-alone executable version of run_deid."""

from __future__ import absolute_import

import argparse
import logging
import sys

from physionet import run_deid_lib
from google.cloud import storage


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description=('Run Physionet DeID on Google Cloud Platform.'))
  run_deid_lib.add_all_args(parser)
  args = parser.parse_args(sys.argv[1:])

  storage_client = storage.Client(args.project)

  errors = run_deid_lib.run_pipeline(
      args.input_pattern, args.output_directory, args.config_file, args.project,
      args.log_directory, args.dict_directory, args.lists_directory,
      args.max_num_threads, args.include_original_in_pn_output,
      args.service_account, storage_client)

  if errors:
    logging.error(errors)
    return 1

  logging.info('Ran PhysioNet DeID and put output in %s', args.output_directory)

if __name__ == '__main__':
  main()
