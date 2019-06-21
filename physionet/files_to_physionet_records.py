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
import re

from apache_beam.io import filesystems
from apache_beam.metrics import Metrics


def parse_physionet_record(text):
  """Parse the PhysioNet text and get patient_id, record_number, and note."""
  if not isinstance(text, str):
    text = text.decode('utf-8')
  # The format is described at:
  # http://physionet.org/physiotools/deid/doc/DeidUserManual.pdf
  sep = r'\|\|\|\|'
  optional_date = r'(\d\d/\d\d/\d\d\d\d' + sep + r')?'
  pattern = (r'(\n)*START_OF_RECORD=(?P<patient_id>\d+?)' + sep +
             r'(?P<record_number>\d+?)' + sep + optional_date +
             r'\n(?P<text>.*)\n' + sep + r'END_OF_RECORD')
  match = re.match(pattern, text, re.MULTILINE | re.DOTALL)
  if not match:
    logging.error('Failed to parse record: "%s"', text)
    return

  output = {
      'patient_id': int(match.group('patient_id')),
      'record_number': int(match.group('record_number')),
      'note': match.group('text')
  }
  return output


def _result(patient_id, record_number, note):
  inspect_result = {}
  inspect_result['patient_id'] = patient_id
  inspect_result['record_number'] = record_number
  inspect_result['original_note'] = note
  inspect_result['result'] = {}
  inspect_result['result']['findings'] = []
  return inspect_result


def _finding(start, end):
  finding = {}
  finding['infoType'] = {}
  finding['infoType']['name'] = ''
  finding['location'] = {}
  finding['location']['codepointRange'] = {}
  finding['location']['codepointRange']['start'] = start
  finding['location']['codepointRange']['end'] = end
  return finding


def map_phi_to_findings(file_path):
  """Separate full file contents into individual records."""
  if not isinstance(file_path, str):
    file_path = file_path.decode('utf-8')
  # Get the original text for each note so we can include it with the findings.
  notes = {}
  for record in map_file_to_records(file_path + '.text'):
    parsed = parse_physionet_record(record)
    record_id = '{}-{}'.format(parsed['patient_id'], parsed['record_number'])
    notes[record_id] = parsed['note']

  # Get the findings from the .phi file.
  result = None
  start_pattern = r'Patient (?P<patient>\d+)\tNote (?P<record>\d+)'
  finding_pattern = r'\d+\t(?P<start>\d+)\t(?P<end>\d+)'
  for line in filesystems.FileSystems.open(file_path + '.phi'):
    if not isinstance(line, str):
      line = line.decode('utf-8')
    line = line.strip()
    if not line:
      continue
    match = re.match(start_pattern, line)
    if match:
      if result:
        yield result
      patient_id = match.group('patient')
      record_number = match.group('record')
      record_id = '{}-{}'.format(patient_id, record_number)
      result = _result(patient_id, record_number, notes[record_id])
      continue

    match = re.match(finding_pattern, line)
    if not match:
      raise Exception(
          'Invalid line in file "{}": "{}"'.format(file_path + '.phi', line))
    start = int(match.group('start'))
    end = int(match.group('end'))
    result['result']['findings'].append(_finding(start, end))

  yield result


def map_file_to_records(file_path):
  """Separate full file contents into individual records."""
  record_counter = Metrics.counter('main', 'chc-physionet-records')
  reader = filesystems.FileSystems.open(file_path)
  buf = ''
  for line in reader:
    if not isinstance(line, str):
      line = line.decode('utf-8')
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
