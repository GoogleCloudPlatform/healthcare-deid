# Evaluation Pipeline

This package contains a tool to run compare the findings of the DLP pipeline (
dlp/README.md) to 'golden' human-annotated findings. The
example commands use the [bazel build
system](http://bazel.build/versions/master/docs/install.html), but can also be
run directly (i.e.`python dlp/xxx.py`) if $PYTHONPATH includes this package.

## Setup

If you are planning to use this tool on real PHI, ensure your project and
permissions follow [HIPPA
guidelines](https://cloud.google.com/security/compliance/hipaa/).

### Authentication

* Create a [service
account](https://cloud.google.com/storage/docs/authentication#service_accounts)
  for your project.
* Ensure the service account has access to the GCS buckets you will be using.
* Generate service account credentials:

```shell
gcloud iam service-accounts keys create --iam-account ${SERVICE_ACCOUNT?} ~/${SERVICE_ACCOUNT?}.json
```

* Specify the credentials in the GOOGLE_APPLICATION_CREDENTIALS environment
  variable:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=~/${SERVICE_ACCOUNT?}.json
```

### Dependencies

Running the pipeline requires having the Google Python API client, Google Cloud
Storage client, and Python Apache Beam client installed. Note that as of
2017-10-27, there is an incompatibility with the latest version of the
`six` library, which requires a downgrade to 1.10.0 to fix.

```shell
sudo pip install --upgrade apache_beam[gcp] google-api-python-client google-cloud-storage six==1.10.0
```

The code for the pipeline itself is available for download from GitHub:

```shell
git clone https://github.com/GoogleCloudPlatform/healthcare-deid.git &&
cd healthcare-deid
```

## Running the pipeline

The pipeline takes as input a pattern indicating which findings to compare, and
a directory containing "golden" findings to comare against. It is expected that
the findings are in MAE format (see dlp/README.md) and that each file matching
the pattern will have a counterpart with the same name in --mae_golden_dir.

Note that for strict entity matching to work, the .dtd files used to create the
inputs and the goldens must use the same type names. The ordering and prefixes,
however, do not need to match for the .dtd files to be compatible.

Output is written to a GCS file called `aggregate_results.txt` in --results_dir.

Example usage:

```shell
bazel build eval:run_pipeline && \
bazel-bin/eval/run_pipeline \
  --mae_input_pattern gs://${BUCKET?}/dir/*.xml \
  --mae_golden_dir gs://${BUCKET?}/goldens/ \
  --results_dir gs://${BUCKET?}/eval/ \
  --project ${PROJECT?}
```

To run in parallel on Google Cloud Dataflow, add:

```shell
--runner DataflowRunner \
--temp_location gs://{$BUCKET?}/tmp \
--staging_location gs://{$BUCKET?}/staging \
--setup_file ./setup.py
```

## Calculations and Output

The metrics used are true positives, false positives, false negatives, and the
derived values of precision, recall, and f-score (harmonic mean of precision and
recall).

### Higher-order metrics (micro- and macro- averages)

Micro-averages are calculated by summing the individual true positives, false
positives, and false negatives, then calculating overall precision and recall.

Macro-averages are calculated by taking the mean of the precision and recall
scores for all the individual results.

### Strict Entity Matching

For strict entity matching, a finding must have an exact match of both category
and range to be a true positive; otherwise it is a false positive. Any finding
in the golden that does not have an exact match (category and range) is a false
negative.

### Output Format

The output format is the Results protocol buffer defined in eval/results.proto.
It contains the metrics discussed above, and may have error_message filled in if
there were any problems in processing. Specifically, if macro-averaging cannot
use a result (e.g. precision is NaN because there was no PHI in the note), it
will skip over those results, but note their resource ID in error_message.

Here's an example of a result message:

```none
strict_entity_matching_results {
  micro_average_results {
    true_positives: 3
    false_positives: 2
    false_negatives: 2
    precision: 0.6
    recall: 0.6
    f_score: 0.6
  }
  macro_average_results {
    precision: 0.5
    recall: 0.5
    f_score: 0.5
    error_message: "Ignored results for 1-4 "
  }
}
```
