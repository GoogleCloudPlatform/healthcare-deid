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
r"""Remove characters that aren't valid in XML 1.0 so MAE can read them.

Usage:
PYTHONPATH="."
python mae/remove_invalid_characters.py --input_pattern="input/*.xml" \
    --output_dir="output"
"""

from __future__ import absolute_import

import argparse
import glob
import logging
import os
import sys

from common import mae


def run(file_pattern, output_dir):
  """Re-write the files with invalid XML characters removed."""
  logging.info('Matched files: %s', glob.glob(file_pattern))
  for filename in glob.glob(file_pattern):
    logging.info('Loading file: "%s"', filename)
    with open(filename) as f:
      contents = f.read()
      with open(os.path.join(output_dir, os.path.basename(filename)), 'w') as o:
        o.write(mae.remove_invalid_characters(contents))


def main(argv):
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Remove characters that aren\'t valid in XML 1.0.')
  parser.add_argument('--input_pattern', type=str, required=True)
  parser.add_argument('--output_dir', type=str, required=True)
  args, _ = parser.parse_known_args(argv[1:])
  run(args.input_pattern, args.output_dir)
  logging.info('Complete. Output is in "%s".', args.output_dir)


if __name__ == '__main__':
  main(sys.argv)
