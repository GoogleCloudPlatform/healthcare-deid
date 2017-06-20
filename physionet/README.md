# PhysioNet DeID on GCP

This package contains tools to run the PhysioNet DeID tool on Google Cloud. The
example commands use the [bazel build
system](http://bazel.build/versions/master/docs/install.html), but can also be
run directly (i.e.`python physionet/xxx.py`) if $PYTHONPATH includes this
package.

## End-to-End BigQuery -> PhysioNet -> BigQuery Pipeline

physionet_deid_pipeline.py stitches together 3 of our tools to perform
end-to-end DeID by reading from BigQuery, running PhysioNet, and writing back
out to BigQuery. Running it requires having the Google Python API client, the
Google Cloud Storage client, and the Python Apache Beam client installed:

```none
pip install --upgrade apache_beam
pip install --upgrade google-api-python-client
pip install --upgrade google-cloud-storage
```

Example usage:

```none
blaze run third_party/py/healthcare_deid/physionet:physionet_deid_pipeline -- \
  --input_query "select * from [${PROJECT?}:${DATASET?}.deid]" \
  --project ${PROJECT?} \
  --gcs_working_directory gs://${BUCKET?}/deid/workspace \
  --config_file gs://${BUCKET?}/deid/simple.config \
  --output_table "${PROJECT?}:${DATASET?}.deid_output"
```

## Running individual pieces of the pipeline

The three steps (BQ->GCS, DeID, GCS->BQ) can be run individually as well:

### BigQuery to GCS Physionet

bigquery_to_gcs takes a BigQuery query as input, transforms the data to
Physionet De-ID format, and writes it to GCS.

Example usage:

```none
bazel run physionet:bigquery_to_gcs -- \
  --input_query="select * from [${PROJECT?}:${DATASET?}.deid]" \
  --output_file gs://${BUCKET?}/deid/output/intermediate \
  --project ${PROJECT?}
```

### Run DeID

run_deid.py runs Physionet De-ID via the Pipelines API on a sharded file in GCS.
The PhysioNet code and configuration has been packaged into a Docker container
at gcr.io/genomics-api-test/physionet. Running it requires having the Google
Python API client and Google Cloud Storage client installed:

Example usage (run with `--help` for more about the arguments):

```none
bazel run physionet:run_deid -- \
  --config_file gs://${BUCKET?}/physionet/simple.config \
  --project ${PROJECT?} \
  --log_directory gs://${BUCKET?}/physionet/logs \
  --input_pattern gs://${BUCKET?}/physionet/input/notes-*-of-????? \
  --output_directory gs://${BUCKET?}/physionet/output \
  --max_num_threads 10
```

### GCS PhysioNet to BigQuery

You can use gcs_to_bigquery to take DeID'd PhysioNet data from GCS and put it
into a BigQuery table.

Example usage:

```none
bazel run physionet:gcs_to_bigquery -- \
  --output_table ${PROJECT?}:${DATASET?}.deid_output \
  --input_pattern gs://${BUCKET?}/deid/input/*
```
