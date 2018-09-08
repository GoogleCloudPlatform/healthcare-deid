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

"""Run Google Data Loss Prevention API Image DeID.

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
import time
from apiclient import discovery
from apiclient import errors
from dlp import run_deid_lib
import google.auth


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Run Data Loss Prevention (DLP) Imagine DeID.')
  parser.add_argument('--image_dir', type=str, required=False,
                      help=('Path to an image dir.'))
  parser.add_argument(
      '--dicom_file', type=str, required=False, help=('Path to a dicom file.'))
  parser.add_argument(
      '--redact_dir', type=str, required=False, help=('Path to source dir'))
  parser.add_argument(
      '--count_area', type=bool, required=False, default=False,
      help=('Whether to count pixel'))

  args, _ = parser.parse_known_args(sys.argv[1:])

  var = 'GOOGLE_APPLICATION_CREDENTIALS'
  if var not in os.environ or not os.environ[var]:
    raise Exception('You must specify service account credentials in the '
                    'GOOGLE_APPLICATION_CREDENTIALS environment variable.')
  credentials, project = google.auth.default()

  image_bytes = ''
  dlp = discovery.build(
      'dlp', 'v2', credentials=credentials, cache_discovery=False)

  if args.image_dir:
    file_count = 0
    word_count = 0
    for filename in os.listdir(args.image_dir):
      source_filename = os.path.join(args.image_dir, filename)
      dest_filename = os.path.join(args.redact_dir, filename)
      with open(source_filename, 'rb') as image_file:
        image_bytes = base64.b64encode(image_file.read()).rstrip('\n')
      text, area = get_deid_result(
          dest_filename, image_bytes, dlp, project, args)
      logging.info('DLP detected %s words in %s',
                   len(text.split()), source_filename)
      file_count += 1
      word_count += len(text.split())
      time.sleep(0.5)

    logging.info('DLP detected %s words in %s files', word_count, file_count)

  elif args.dicom_file:
    dicom_byte = subprocess.check_output(
        ['dcmj2pnm', args.dicom_file, '--write-png'])
    image_bytes = base64.b64encode(dicom_byte)
    filename = '/tmp/tmp1.jpg'
    text, area = get_deid_result(filename, image_bytes, dlp, project, args)
    logging.info(
        'DLP detected %s words, output image: %s',
        len(text.split()), filename)
    if area > 0:
      logging.info('and covering at least %s pixels', area)


def get_deid_result(filename, image_bytes, dlp, project, args):
  """Call DeID library and returns the detected text, and pixel size."""
  projects = dlp.projects()
  image = projects.image()
  content = projects.content()

  # Without findings, redact will not work.

  req_body = {
      'inspectConfig': {
          'includeQuote':
              True,
          'infoTypes': [
              {
                  'name': 'AGE'
              },
              {
                  'name': 'DATE'
              },
              {
                  'name': 'CREDIT_CARD_NUMBER'
              },
              {
                  'name': 'EMAIL_ADDRESS'
              },
              {
                  'name': 'LOCATION'
              },
              {
                  'name': 'PERSON_NAME'
              },
              {
                  'name': 'PHONE_NUMBER'
              },
          ],
      },
      'imageRedactionConfigs': [{
          'redactAllText': True,
      }],
      'byteItem': {
          'type': 'IMAGE_JPEG',
          'data': image_bytes,
      },
  }
  content_req_body = {
      'inspectConfig': {
          'infoTypes': [
              {
                  'name': 'AGE'
              },
              {
                  'name': 'DATE'
              },
              {
                  'name': 'CREDIT_CARD_NUMBER'
              },
              {
                  'name': 'EMAIL_ADDRESS'
              },
              {
                  'name': 'LOCATION'
              },
              {
                  'name': 'PERSON_NAME'
              },
              {
                  'name': 'PHONE_NUMBER'
              },
          ],
          'minLikelihood':
              'POSSIBLE'
      },
      'item': {
          'byteItem': {
              'type': 'IMAGE_JPEG',
              'data': image_bytes,
          }
      },
  }
  parent = 'projects/{0}'.format(project)
  try:
    response = run_deid_lib.request_with_retry(
        image.redact(body=req_body, parent=parent).execute)
  except errors.HttpError as error:
    raise error
  if 'error' in response:
    raise Exception('Redact() failed: {}'.format(response['error']))

  try:
    text = response['extractedText']
    imgdata = base64.b64decode(response['redactedImage'])
    with open(filename, 'wb') as f:
      f.write(imgdata)
  except KeyError:
    with open(filename, 'wb') as f:
      f.write(image_bytes)
    return '', 0

  # inspect call only returns info of findings. Texts that do not match any
  # info type are not included.
  area = 0

  if args.count_area:
    try:
      inspect = run_deid_lib.request_with_retry(
          content.inspect(body=content_req_body, parent=parent).execute)
    except errors.HttpError as error:
      raise error
    if 'error' in inspect:
      raise Exception('Inspect() failed: {}'.format(inspect['error']))

    if inspect['result']:
      findings = inspect['result']['findings']
      for finding in findings:
        locations = finding['location']['contentLocations']
        for location in locations:
          boxes = location['imageLocation']['boundingBoxes']
          for box in boxes:
            area += int(box['width']) * int(box['height'])
    else:
      logging.info('No inspect findings.')

  return text, area


if __name__ == '__main__':
  main()
