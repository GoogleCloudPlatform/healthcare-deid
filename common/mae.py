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

import collections


MaeTuple = collections.namedtuple('MaeTuple',
                                  ['patient_id', 'record_number', 'mae_xml'])


def _start(finding):
  if 'start' not in finding['location']['byteRange']:
    return 0
  return int(finding['location']['byteRange']['start'])


def _end(finding):
  return int(finding['location']['byteRange']['end'])


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


def generate_mae(inspect_result, task_name, mae_tag_categories):
  """Write out inspect() findings in MAE format alongside the text in GCS."""
  infotype_to_tag_map = _get_infotype_to_tag_map(mae_tag_categories)
  mae_xml = [u"""<?xml version="1.0" encoding="UTF-8" ?>
<{0}>
<TEXT><![CDATA[{1}]]></TEXT>
<TAGS>""".format(task_name, inspect_result['original_note'])]
  counts = collections.defaultdict(int)
  findings = []
  if 'findings' in inspect_result['result']:
    findings = inspect_result['result']['findings']
  for finding in findings:
    tag_name = infotype_to_tag_map[finding['infoType']['name']]
    count = counts[tag_name]
    counts[tag_name] += 1
    mae_xml.append('\n<{0} id="{0}{1}" spans="{2}~{3}" />'.format(
        tag_name, count, _start(finding), _end(finding)))

  mae_xml.append('\n</TAGS></{0}>\n'.format(task_name))
  return MaeTuple(inspect_result['patient_id'], inspect_result['record_number'],
                  ''.join(mae_xml))
