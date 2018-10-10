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

"""Functions for generating MAE-compatible data."""

from bisect import bisect
import collections
import re


MaeTuple = collections.namedtuple('MaeTuple', ['record_id', 'mae_xml'])

FILE_START = u"""<?xml version="1.0" encoding="UTF-8" ?>
<{0}>
<TEXT><![CDATA[{1}]]></TEXT>
<TAGS>"""
TAG = '\n<{0} id="{0}{1}" spans="{2}~{3}" />'
FILE_END = '\n</TAGS></{0}>\n'


# Remove control characters which aren't valid in XML 1.0, since XmlTree can't
# handle them. https://www.w3.org/TR/xml/#charsets
INVALID_XML_CHARS = []
for ch in range(32):
  if ch in [9, 10, 13]:  # Tab, Line Feed, and Carriage Return are valid.
    continue
  INVALID_XML_CHARS.append(chr(ch))
INVALID_XML_CHARS = ''.join(INVALID_XML_CHARS)


def _start(finding, invalid_chars_indexes):
  if 'start' not in finding['location']['codepointRange']:
    return 0
  start = int(finding['location']['codepointRange']['start'])
  return start - bisect(invalid_chars_indexes, start)


def _end(finding, invalid_chars_indexes):
  end = int(finding['location']['codepointRange']['end'])
  return end - bisect(invalid_chars_indexes, end)


def generate_dtd(mae_tag_categories, task_name):
  # DTD format at http://github.com/keighrim/mae-annotation/wiki/defining-dtd
  dtd_contents = ['<!ENTITY name "{0}">'.format(task_name)]
  for category in mae_tag_categories:
    name = category['name']
    dtd_contents.append(
        '\n\n<!ELEMENT {0} ( #PCDATA ) >\n'
        '<!ATTLIST {0} id ID prefix="{0}" #REQUIRED >\n'.format(name))
  return ''.join(dtd_contents)


def _get_infotype_to_tag_map(mae_tag_categories):
  infotype_to_tag_map = collections.defaultdict(
      lambda: 'UNKNOWN_CLASSIFICATION_TYPE')
  for category in mae_tag_categories:
    for info_type in category['infoTypes']:
      infotype_to_tag_map[info_type] = category['name']
  return infotype_to_tag_map


def remove_invalid_characters(text):
  return re.sub('[' + INVALID_XML_CHARS + ']', '', text)


def generate_mae(inspect_result, task_name, mae_tag_categories, key_columns):
  """Convert inspect() findings to MAE format.

  Args:
    inspect_result: Dict containing 'original_note', 'result' (findings from DLP
      inspect() call), and one entry for each of key_columns.
    task_name: String to use as the MAE task name.
    mae_tag_categories: Dict containing 'name' (string to use as a MAE tag) and
      'infoTypes' (list of infoTypes that should use this tag).
    key_columns: The values from these columns will be concatenated together
      with dashes to form the record_id.

  Returns:
    A MaeTuple.
  """
  raw_note = inspect_result['original_note']
  invalid_chars_indexes = []
  for i, char in enumerate(raw_note):
    if char in INVALID_XML_CHARS:
      invalid_chars_indexes.append(i)
  safe_note = remove_invalid_characters(raw_note)

  infotype_to_tag_map = _get_infotype_to_tag_map(mae_tag_categories)
  mae_xml = [FILE_START.format(task_name, safe_note)]
  counts = collections.defaultdict(int)
  findings = []
  if 'findings' in inspect_result['result']:
    findings = inspect_result['result']['findings']
  for finding in findings:
    tag_name = infotype_to_tag_map[finding['infoType']['name']]
    count = counts[tag_name]
    counts[tag_name] += 1
    mae_xml.append(TAG.format(
        tag_name, count, _start(finding, invalid_chars_indexes),
        _end(finding, invalid_chars_indexes)))

  mae_xml.append(FILE_END.format(task_name))

  record_id_values = []
  for col in key_columns:
    record_id_values.append(str(inspect_result[col]))

  return MaeTuple('-'.join(record_id_values), ''.join(mae_xml))
