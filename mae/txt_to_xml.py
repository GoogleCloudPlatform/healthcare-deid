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
r"""Convert text files to MAE XML files.

MAE automatically converts text files to XML upon opening, but this process
mangles unicode characters on Windows. This tool provides a workaround: first
generate MAE XML files using this tool, then open them in MAE.

Usage:
python txt_to_xml.py --input_pattern="input/*.txt" --output_dir="output" \
  --task_name="InspectPhiTask"
"""

from __future__ import absolute_import

import argparse
import codecs
import glob
import logging
import os
import sys


TEMPLATE = u"""<?xml version="1.0" encoding="UTF-8" ?>
<{0}>
<TEXT><![CDATA[{1}]]></TEXT>
</{0}>"""


def run(file_pattern, output_dir, task_name):
  """Re-write the files with invalid XML characters removed."""
  logging.info('Matched files: %s', glob.glob(file_pattern))
  for filename in glob.glob(file_pattern):
    logging.info('Loading file: "%s"', filename)
    with codecs.open(filename, encoding='utf-8') as f:
      contents = f.read()
      outname = os.path.join(output_dir, os.path.basename(filename)) + '.xml'
      with codecs.open(outname, 'w', encoding='utf-8') as o:
        o.write(TEMPLATE.format(task_name, contents))


def main(argv):
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Convert text files to MAE XML files.')
  parser.add_argument('--input_pattern', type=str, required=True)
  parser.add_argument('--output_dir', type=str, required=True)
  parser.add_argument('--task_name', type=str, required=True)
  args, _ = parser.parse_known_args(argv[1:])
  run(args.input_pattern, args.output_dir, args.task_name)
  logging.info('Complete. Output is in "%s".', args.output_dir)


if __name__ == '__main__':
  main(sys.argv)
