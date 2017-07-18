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

"""Functions to turn a file pattern into PhysioNet records from the files."""

from __future__ import absolute_import

import logging

from apache_beam.io import filesystems
from apache_beam.metrics import Metrics


def map_file_to_records(file_path):
  """Separate full file contents into individual records."""
  record_counter = Metrics.counter('main', 'chc-physionet-records')
  reader = filesystems.FileSystems.open(file_path)
  buf = ''
  for line in reader:
    buf += line
    if '||||END_OF_RECORD' in line:
      record_counter.inc()
      yield buf
      buf = ''


def match_files(input_path):
  """Find the list of absolute file paths that match the input path spec."""
  file_counter = Metrics.counter('main', 'chc-matched-files')
  logging.info('matching path: %s', input_path)
  for match_result in filesystems.FileSystems.match([input_path]):
    for metadata in match_result.metadata_list:
      logging.info('matched path: %s', metadata.path)
      file_counter.inc()
      yield metadata.path
