# Evaluation Pipeline

This package contains a tool to run compare the findings of the [DLP pipeline](../dlp/README.md) to 'golden' human-annotated findings. The
example commands use the [bazel build
system](http://bazel.build/versions/master/docs/install.html), but can also be
run directly (i.e.`python dlp/xxx.py`) if $PYTHONPATH includes this package.

## Setup

If you are planning to use this tool on real PHI, ensure your project and
permissions follow [HIPAA
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

This tool uses the Apache Beam SDK for Python, which requires Python version
2.7.x. Check that you have version 2.7.x by running:

```shell
python --version
```

The code for the pipeline is available for download from GitHub:

```shell
git clone https://github.com/GoogleCloudPlatform/healthcare-deid.git && \
cd healthcare-deid
```

Running the pipeline requires having the Google Python API client, Google Cloud
Storage client, and Python Apache Beam client installed. Note that as of
2017-10-27, there is an incompatibility with the latest version of the
`six` library, which requires a downgrade to 1.10.0 to fix.

```shell
virtualenv -p python2 env
source env/bin/activate
pip install --upgrade apache_beam[gcp] google-api-python-client google-cloud-bigquery google-auth-httplib2 google-cloud-storage six==1.10.0
```

## Annotating Goldens

Use the [bq_to_xml tool](../mae/README.md#downloading-notes-from-bigquery) to download your data from BigQuery and write it to
MAE-compatible XML files. You can then annotate the notes using the [MAE
Annotation Tool](https://github.com/keighrim/mae-annotation/releases), and upload
the results to BigQuery using [upload_files_to_bq](../mae/README.md#uploading-xml-files-to-bigquery).

## Running the pipeline

### BigQuery Input
If --mae_input_query and --mae_golden_table are specified, the input xml comes
from the specified BigQuery query, which is left-joined on record_id with data
from mae_golden_table. The input query and golden table must both have
`record_id` and `xml` fields.

### GCS Input
If --mae_input_pattern and --mae_golden_dir are specified, the pipeline takes as
input a pattern indicating which findings to compare, and a directory containing
"golden" findings to comare against. It is expected that the findings are in MAE
format (see dlp/README.md) and that each file matching the pattern will have a
counterpart with the same name in --mae_golden_dir.

### XML format for strict entity matching
Note that for strict entity matching to work, the .dtd files used to create the
inputs and the goldens must use the same type names. The ordering and prefixes,
however, do not need to match for the .dtd files to be compatible.

### Output
Overall output is:

 - Printed to stdout.
 - Written to a GCS file called `aggregate_results.txt` in --results_dir, if
   specified.
 - Written to --results_table in BigQuery, if specified.

Detailed output is:

 - Written to a GCS file `per-note-results` in --results_dir, if
   --write_per_note_stats_to_gcs is specified.
 - Written to --per_note_results_table, and/or
   --debug_output_table in BigQuery, if either of those are specified.

Example usage:

```shell
bazel build eval:run_pipeline && \
bazel-bin/eval/run_pipeline \
  --mae_input_pattern gs://${BUCKET?}/dir/*.xml \
  --mae_golden_dir gs://${BUCKET?}/goldens/ \
  --results_dir gs://${BUCKET?}/eval/
```

Or with BigQuery instead of GCS:

```shell
bazel build eval:run_pipeline && \
bazel-bin/eval/run_pipeline \
  --mae_input_query "SELECT * FROM [${PROJECT?}:${DATASET?}.table]" \
  --mae_golden_table ${PROJECT?}:${DATASET?}.goldens \
  --results_table ${PROJECT?}:${DATASET?}.results
```

To run in parallel on Google Cloud Dataflow, add:

```shell
--runner DataflowRunner \
--project ${PROJECT?} \
--temp_location gs://${BUCKET?}/tmp \
--staging_location gs://${BUCKET?}/staging \
--setup_file ./setup.py
```

By default, running on Dataflow will use autoscaling. To disable autoscaling
and manually specify the number of workers, use:

```shell
--autoscaling_algorithm=NONE --num_workers=XXX
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

### Binary Token Matching

For Binary Token Matching we break up all the findings into whitespace-separated
tokens, and we evaluate as if those were all individual findings that are not
labeled with a type. To accomadate situations where tokenization may be done
differently by different systems (around punctuation, generally), we count a
match if two labels have any overlap. e.g. if the goldens have "T." "J." "Smith"
and the DLP API got "T" "J" "Smith", this still counts as 3 matches (true
positives).

### Strict Entity Matching

For strict entity matching, a finding must have an exact match of both category
and range to be a true positive; otherwise it is a false positive. Any finding
in the golden that does not have an exact match (category and range) is a false
negative.

### BigQuery Output

If --results_table is specified, the micro-averaged binary token matching
results will be written to BigQuery for each infoType, as well as the overall
aggregated values (infoType 'ALL').

If --per_type_results_table is specified, the overall binary token matching
results for each note will be written to that table in BigQuery.

The schemas for these tables are detailed in run_pipeline_lib.py, but
essentially the columns are:
* All the fields in the Stats protocol buffer defined in eval/results.proto.
* For results_table, the infoType the stats apply to.
* For per_type_results_table, the record_id the stats apply to.

### GCS Output Format

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
