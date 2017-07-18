# MIST on GCP

This package contains tools to run [MIST](http://mist-deid.sourceforge.net) on
Google Cloud. The example commands use the [bazel build system](http://bazel.build/versions/master/docs/install.html), but can also be
run directly (i.e.`python mist/xxx.py`) if $PYTHONPATH includes this
package.

## How to run

### BigQuery -> GCS

bigquery_to_gcs.py runs a query on BigQuery and writes the data to the given
path in Google Cloud Storage.

```shell
bazel run mist:bigquery_to_gcs -- \
--input_query "select patient_id, note from [${PROJECT?}:${DATASET?}.deid]" \
--output_path gs://${BUCKET?}/deid/mist/bq-output \
--project ${PROJECT?}
```

### Run MIST

run_deid.py runs Physionet De-ID via the Pipelines API on a sharded file in GCS.
The PhysioNet code and configuration has been packaged into a Docker container
at gcr.io/genomics-api-test/physionet. Running it requires having the Google
Python API client and Google Cloud Storage client installed:

```shell
pip install --upgrade apache_beam
pip install --upgrade google-api-python-client
pip install --upgrade google-cloud-storage
```

Example usage (run with `--help` for more about the arguments):

```shell
bazel run mist:run_mist -- \
  --model_filename gs://${BUCKET}/deid/mist/model \
  --project ${PROJECT?} \
  --log_directory gs://${BUCKET?}/deid/mist/logs \
  --input_pattern gs://${BUCKET?}/deid/mist/input/notes* \
  --output_directory gs://${BUCKET?}/deid/mist/output
```

### GCS -> BigQuery

gcs_to_bigquery.py grabs data from a given Google Cloud Storage path and puts
it into BigQuery.

Example usage:

```shell
bazel run mist:gcs_to_bigquery -- \
--input_pattern gs://${BUCKET?}/deid/mist/bq-output* \
--output_table "${PROJECT?}:${DATASET?}.deid_output"
```
