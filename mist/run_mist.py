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

"""Stand-alone executable version of run_mist_lib."""

from __future__ import absolute_import

import argparse
import logging
import sys

from google.cloud import storage
from mist import run_mist_lib
import google.auth


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description=('Run MIST on Google Cloud Platform.'))
  run_mist_lib.add_all_args(parser)
  args = parser.parse_args(sys.argv[1:])

  credentials, _ = google.auth.default()
  storage_client = storage.Client(args.project, credentials=credentials)

  errors = run_mist_lib.run_pipeline(
      args.input_pattern, args.output_directory, args.model_filename,
      args.project, args.log_directory, args.max_num_threads,
      args.service_account, storage_client, credentials)

  if errors:
    logging.error(errors)
    return 1

  logging.info('Ran MIST and put output in %s', args.output_directory)

if __name__ == '__main__':
  main()
