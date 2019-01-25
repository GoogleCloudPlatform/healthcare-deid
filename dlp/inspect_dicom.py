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

"""Run Google Vision API.

Requires Google Python API Client:
pip install --upgrade google-api-python-client
"""

from __future__ import absolute_import

import argparse
import base64
import logging
import os
import subprocess
import sys
from apiclient import discovery
from apiclient import errors
from dlp import run_deid_lib
import google.auth


def get_dicom_bytes(dicom_file):
  """Convert DICOM file to bytes."""
  dicom_byte = subprocess.check_output(
      ['dcmj2pnm', dicom_file, '--write-png'])
  return base64.b64encode(dicom_byte)


def get_word_count(vision, image_bytes):
  """Use Vision API to detect the count of words in the image."""
  vision_req_body = {
      'requests': [{
          'image': {
              'content': image_bytes,
          },
          'features': [{
              'type': 'TEXT_DETECTION'
          }]
      }]
  }
  try:
    result = run_deid_lib.request_with_retry(
        vision.annotate(body=vision_req_body).execute)
  except errors.HttpError as error:
    raise error
  if 'error' in result:
    raise Exception('Annotate() failed: {}'.format(result['error']))

  if result and result['responses'] and result['responses'][0]:
    return result['responses'][0]['fullTextAnnotation']['text'].count('\n')
  else:
    return 0


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Run Vision API to annotate image.')
  parser.add_argument('--image_file', type=str, required=False,
                      help=('Path to an image file.'))
  parser.add_argument('--dicom_file', type=str, required=False,
                      help=('Path to a dicom file.'))
  parser.add_argument('--dicom_dir', type=str, required=False,
                      help=('Path to a directory holding dicom files.'))
  parser.add_argument('--csv_output', type=str, required=False,
                      help=('Path to a file for output.'))

  args, _ = parser.parse_known_args(sys.argv[1:])

  var = 'GOOGLE_APPLICATION_CREDENTIALS'
  if var not in os.environ or not os.environ[var]:
    raise Exception('You must specify service account credentials in the '
                    'GOOGLE_APPLICATION_CREDENTIALS environment variable.')
  credentials, _ = google.auth.default()

  vision = discovery.build(
      'vision', 'v1', credentials=credentials, cache_discovery=False)
  images = vision.images()

  if not args.dicom_dir:
    image_bytes = ''
    if args.image_file:
      with open(args.image_file, 'rb') as image_file:
        image_bytes = base64.b64encode(image_file.read()).rstrip('\n')
    if args.dicom_file:
      image_bytes = get_dicom_bytes(args.dicom_file)
    logging.info('We detected %d words.', get_word_count(images, image_bytes))
    return

  output_file = '/tmp/tmp.csv'
  if args.csv_output:
    output_file = args.csv_output
  output = open(output_file, 'w')
  output.write('file_name,text_chars\n')

  for filename in os.listdir(args.dicom_dir):
    if filename.endswith('.dcm'):
      dicom_bytes = get_dicom_bytes(os.path.join(args.dicom_dir, filename))
      count = get_word_count(images, dicom_bytes)
      if args.csv_output:
        output.write('%s,%d\n' % (filename, count))
      else:
        logging.info('We detected %d words in file %s.', count, filename)
  output.close()

if __name__ == '__main__':
  main()
