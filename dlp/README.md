# Data Loss Prevention (DLP) API Pipeline

This package contains a tool to run the DLP api on a dataset in BigQuery. The
example commands use the [bazel build
system](http://bazel.build/versions/master/docs/install.html), but can also be
run directly (i.e.`python dlp/xxx.py`) if $PYTHONPATH includes this
package.

## Setup

If you are planning to use this tool on real PHI, ensure your project and
permissions follow HIPPA guidelines.

### Authentication

* Enable the [DLP API](https://console.cloud.google.com/apis/api/dlp.googleapis.com/overview)
* Create a [service account](https://cloud.google.com/storage/docs/authentication#service_accounts)
  for your project with the "BigQuery User" role.
* Ensure the service account has access to the BigQuery tables and GCS buckets
  you will be using.
* Grant the `serviceusage.services.use` permission to your service account. You
   can do this by giving the service account the OWNER or EDITOR role, or by
   [creating a custom role](https://console.cloud.google.com/iam-admin/roles)
   with the permission, and then [granting that custom](https://console.cloud.google.com/iam-admin/iam)
   role to the service account.
* Generate service account credentials:

```shell
gcloud iam service-accounts keys create --iam-account SERVICE_ACCOUNT_EMAIL_ADDRESS ~/key.json
```

* Specify the credentials in the GOOGLE_APPLICATION_CREDENTIALS environment
  variable:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=~/key.json
```

### Dependencies

Running the pipeline requires having the Google Python API client, Google Cloud
Storage client, and Python Apache Beam client installed. Note that as of
2017-10-27, there is an incompatibility with the latest version of the
`six` library, which requires a downgrade to 1.10.0 to fix.

```shell
virtualenv env
pip install --upgrade apache_beam[gcp] google-api-python-client google-cloud-bigquery google-auth-httplib2 google-cloud-storage six==1.10.0
```

The code for the pipeline itself is available for download from GitHub:

```shell
git clone https://github.com/GoogleCloudPlatform/healthcare-deid.git &&
cd healthcare-deid
```

## Running the pipeline

The pipeline takes as input a table with 3 columns: patient_id, record_number,
and note. You may also provide a query that generates matching row
(--input_query), but note that this causes the data to be stored in a temporary
table which does not have data locality guarantees.

The output is written to 2 separate tables:

* deid_table holds the deidentified notes from the deidentify call to the DLP
  API.
* findings_table holds the findings of PII identified from calling inspect on
  the DLP API.

Example usage:

```shell
bazel build dlp:run_deid && \
bazel-bin/dlp/run_deid \
  --input_table "${PROJECT?}:${DATASET?}.dlp_input" \
  --project ${PROJECT?} \
  --deid_config_file dlp/sample_deid_config.json \
  --deid_table ${PROJECT?}:${DATASET?}.deid_output \
  --findings_table ${PROJECT?}:${DATASET?}.dlp_findings \
  --mae_dir gs://bucket-name/mae-output-directory
```

To run in parallel on Google Cloud Dataflow, add:

```shell
--runner DataflowRunner \
--temp_location gs://${BUCKET?}/tmp \
--staging_location gs://${BUCKET?}/staging \
--setup_file ./setup.py
```

By default, running on Dataflow will use autoscaling. To disable autoscaling
and manually specify the number of workers, use:

```shell
--autoscaling_algorithm=NONE --num_workers=XXX
```

## Config file

--deid_config_file specifies a json file (example in [sample_deid_config.json](http://github.com/GoogleCloudPlatform/healthcare-deid/tree/master/dlp/sample_deid_config.json))
that contains an object with five fields:

`deidConfig`: A [DeidentifyConfig](https://cloud.google.com/dlp/docs/reference/rest/v2beta2/organizations.deidentifyTemplates#DeidentifyTemplate.DeidentifyConfig)
for use with the DLP API's content.deidentify method.

`infoTypeCategories`: A list of tags to use in the MAE output and the infoTypes
from the deidConfig that correspond to those tags. Each entry in the list has a
name, and a list of infoTypes, e.g.:

```none
{
  "name": "FIRST_NAME",
  "infoTypes": ["US_FEMALE_NAME", "US_MALE_NAME"]
}
```

`perRowTypes`: A list of columnName/infoTypeName pairs. For each inspect() and
deid() call, for each row, we take the value from the specified column and add
it as a custom infoType with the given name.

`perDatasetTypes`: Similar to perRowTypes, but does a pre-processing step where
it runs through the entire dataset and gets *all* the values for the given
column(s). These are then passed to every inspect() and deid() request as
custom infoTypes.

`columns`: Specifies two types of columns - those that should be passed through
to the output table as-is (`passThrough`) and those that should be processed by
the DLP API and have the results written to the output table (`inspect`). If
not specified in the config, `passThrough` contains 'patient_id' and
'record_number', and `inspect` contains 'note'. See
sample_multi_column_deid_config.json for an example.

## MAE Format

If you specify the --mae_dir flag, the pipeline will write xml files to a GCS
directory. The files will be named using the structure
"`patientid`-`recordnumber`.xml", with a DTD file in "classification.dtd".
Examples are available under [mae_testdata](http://github.com/GoogleCloudPlatform/healthcare-deid/tree/master/dlp/mae_testdata)
in `sample.xml` and `sample.dtd`.

Note that MAE output cannot be used if you specify `columns` in your config
file.

### DTD Format

The DTD format is described at
http://github.com/keighrim/mae-annotation/wiki/defining-dtd. The DTD file
generated by the pipeline uses the name provided in --mae_task_name, and has
one element for each infoType specified in --deid_config_file. e.g.:

```none
<!ELEMENT FIRST_NAME ( #PCDATA ) >
<!ATTLIST FIRST_NAME id ID prefix="FIRST_NAME" #REQUIRED >
```

### XML Format

The top-level tag is the one specified in --mae_task_name. Then comes the text,
wrapped in TEXT and CDATA, e.g.:

```none
<TEXT><![CDATA[the text of the note goes here]]></TEXT>
```

Then we have the tags indicating discovered PID. The tag is the infoType, and
the id is the infoType plus an integer to uniquely identify it, e.g.:

```
<TAGS>
<US_CENSUS_NAME id="US_CENSUS_NAME0" spans="9~12" />
<US_MALE_NAME id="US_MALE_NAME0" spans="9~12" />
<US_CENSUS_NAME id="US_CENSUS_NAME1" spans="17~25" />
</TAGS>
```
