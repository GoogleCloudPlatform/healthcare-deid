"""Script to convert BigQuery data to MAE-compatible XML files on local disk."""

from __future__ import absolute_import

import argparse
import codecs
import logging
import os
import sys

from google.cloud import bigquery

TEMPLATE = u"""<?xml version="1.0" encoding="UTF-8" ?>
<{0}>
<TEXT><![CDATA[{1}]]></TEXT>
</{0}>"""


def run(input_query, output_dir, task_name, id_columns, target_column):
  """Get the BigQuery data and write it to local files."""
  if output_dir.startswith('gs://'):
    raise Exception('Writing the output to a GCS bucket is not supported; '
                    'please write to a local directory. You can then upload '
                    'your files using "gsutil cp".')
  bq_client = bigquery.Client()
  job_config = bigquery.job.QueryJobConfig()
  job_config.use_legacy_sql = True
  query_job = bq_client.query(input_query, job_config=job_config)
  results_table = query_job.result()

  for row in results_table:
    id_str = '-'.join([str(row.get(col)) for col in id_columns])
    filename = os.path.join(output_dir, id_str + '.xml')
    with codecs.open(filename, 'w', encoding='utf-8') as f:
      f.write(TEMPLATE.format(task_name, row.get(target_column)))

  logging.info('Output written to "%s"', output_dir)


def main(argv):
  logging.getLogger().setLevel(logging.INFO)

  var = 'GOOGLE_APPLICATION_CREDENTIALS'
  if var not in os.environ or not os.environ[var]:
    raise Exception('You must specify service account credentials in the '
                    'GOOGLE_APPLICATION_CREDENTIALS environment variable.')

  parser = argparse.ArgumentParser(
      description='Download BigQuery data and format it as MAE XML.')
  parser.add_argument('--input_query', type=str, required=True)
  parser.add_argument('--local_output_dir', type=str, required=True)
  parser.add_argument('--task_name', type=str, default='InspectPhiTask')
  parser.add_argument('--id_columns', type=str,
                      default='patient_id,record_number')
  parser.add_argument('--target_column', type=str,
                      default='note')
  args = parser.parse_args(argv[1:])

  run(args.input_query, args.local_output_dir, args.task_name,
      args.id_columns.split(','), args.target_column)


if __name__ == '__main__':
  main(sys.argv)
