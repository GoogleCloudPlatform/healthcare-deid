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

r"""Script to upload xml files (local or GCS) to BigQuery.

Expected filename format: <record_id>.xml.

Requires Apache Beam client and Google Cloud Storage client:
pip install --upgrade apache_beam google-cloud-storage

Sample usage:
python upload_files_to_bq.py --file_pattern="dir/*.xml" \
    --table_name project:dataset.table
"""

from __future__ import absolute_import

import argparse
import glob
import logging
import os
import posixpath
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from common import gcsutil
from google.cloud import storage


def run(file_pattern, table_name, pipeline_args):
  """Upload the files to the BigQuery table."""
  rows = []
  if file_pattern.startswith('gs://'):
    storage_client = storage.Client()
    for filename in gcsutil.find_files(file_pattern, storage_client):
      logging.info('Loading file: "%s"', filename)
      bucket = storage_client.lookup_bucket(filename.bucket)
      if not bucket:
        raise Exception('Failed to get bucket "{}".'.format(filename.bucket))
      blob = bucket.get_blob(filename.blob)
      if not blob:
        raise Exception('Failed to get blob "{}" in bucket "{}".'.format(
            filename.blob, filename.bucket))
      contents = blob.download_as_string()
      record_id = posixpath.basename(filename.blob)[:-4]
      rows.append({'record_id': record_id, 'xml': contents})
  else:
    logging.info('Matched files: %s', glob.glob(file_pattern))
    for filename in glob.glob(file_pattern):
      logging.info('Loading file: "%s"', filename)
      record_id = os.path.basename(filename)[:-4]
      contents = ''
      with open(filename) as f:
        contents = f.read()
      rows.append({'record_id': record_id, 'xml': contents})

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))

  _ = (p |
       beam.Create(rows) |
       beam.io.Write(beam.io.BigQuerySink(
           table_name,
           schema=('record_id:STRING,xml:STRING'),
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))

  result = p.run().wait_until_finish()

  logging.info('Result: %s', result)
  return []


def main(argv):
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Upload MAE XML files to BigQuery.')
  parser.add_argument('--file_pattern', type=str, required=True)
  parser.add_argument('--table_name', type=str, required=True)
  args, pipeline_args = parser.parse_known_args(argv[1:])
  run(args.file_pattern, args.table_name, pipeline_args)


if __name__ == '__main__':
  main(sys.argv)
