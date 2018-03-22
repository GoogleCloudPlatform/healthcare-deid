r"""Script to upload local files to BigQuery.

Expected filename format: <record_id>.xml.

Requires Apache Beam client:
pip install --upgrade apache_beam

Sample usage:
python upload_files_to_bq.py --file_pattern="dir/*.xml" \
    --table_name project:dataset.table
"""

from __future__ import absolute_import

import argparse
import glob
import logging
import os
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run(file_pattern, table_name, pipeline_args):
  """Upload the files to the BigQuery table."""
  rows = []
  logging.error(glob.glob(file_pattern))
  for filename in glob.glob(file_pattern):
    logging.error('Loading file: "%s"', filename)
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

  logging.info('Eval result: %s', result)
  return []


def main(argv):
  parser = argparse.ArgumentParser(
      description='Upload MAE XML files to BigQuery.')
  parser.add_argument('--file_pattern', type=str, required=True)
  parser.add_argument('--table_name', type=str, required=True)
  args, pipeline_args = parser.parse_known_args(argv[1:])
  run(args.file_pattern, args.table_name, pipeline_args)


if __name__ == '__main__':
  main(sys.argv)
