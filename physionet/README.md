# PhysioNet DeID on GCP

This package contains tools to run the PhysioNet DeID tool on Google Cloud. The
example commands use the [bazel build
system](http://bazel.build/versions/master/docs/install.html), but can also be
run directly (i.e.`python physionet/xxx.py`) if $PYTHONPATH includes this
package.

## Building the docker container

Before running PhysioNet, you'll need to build a docker container that your
project can use.

```shell
cd physionet/docker/

gcloud builds submit . --config=cloudbuild.yaml \
  --substitutions="_PHYSIONET_VERSION=1.1" --project=${PROJECT?}
```

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
bazel run physionet:physionet_deid_pipeline -- \
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
It uses a docker image from gcr.io/YOUR_PROJECT/physionet which can be built
using docker/cloudbuild.yaml. Running it requires having the Google Python API
client and Google Cloud Storage client installed:

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

### GCS PhysioNet to MAE

You can use physionet_to_mae to convert the PhysioNet DeID results to MAE
format. run_deid needs to be run with --include_original_in_pn_output=true to
generate output that will work as input for physionet_to_mae.

Example usage:

```shell
bazel run physionet:physionet_to_mae -- \
  --mae_output_dir gs://${BUCKET}/deid/mae/ \
  --input_pattern gs://${BUCKET}/deid/input/file-?????-of-?????
```
