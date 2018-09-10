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
  * For GCS, grant the "Storage Legacy Bucket Writer" role for any bucket it
    needs to write to, e.g.:

    ```shell
    gsutil acl ch -u ${SERVICE_ACCOUNT_EMAIL:?}:WRITER gs://${BUCKET_NAME:?}
    ```
  * For BigQuery, give the service account "read" access to any datasets it
    needs to read, and "edit" access to any datasets it needs to write to.
* Grant the `serviceusage.services.use` permission to your service account. You
   can do this by giving the service account the OWNER or EDITOR role, or by
   [creating a custom role](https://console.cloud.google.com/iam-admin/roles)
   with the permission, and then [granting that custom](https://console.cloud.google.com/iam-admin/iam)
   role to the service account, or by granting the serviceUsageConsumer role:

   ```shell
   gcloud projects add-iam-policy-binding ${PROJECT:?} \
     --member serviceAccount:${SERVICE_ACCOUNT_EMAIL:?} \
     --role roles/serviceusage.serviceUsageConsumer
   ```
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
virtualenv env
source env/bin/activate
pip install --upgrade apache_beam[gcp] google-api-python-client google-cloud-bigquery google-auth-httplib2 google-cloud-storage six==1.10.0
```

## Running the pipeline

The pipeline supports three types of input:
1. `--input_table`: a reference to a BigQuery table.
2. `--input_query`: a query that generates matching rows. Note that this causes the data to be stored in a temporary table which does not have data locality guarantees.
3. `--input_csv`: a local path to a csv file.

Exactly one input type must be specified. The columns in the input method should
match the "columns" section in the config file.

The output is written to several places, depending on which flags are provided:

* --deid_table holds the deidentified notes from the deidentify call to the DLP
  API.
* --output_csv saves the deidentified notes from the deidentify call to the DLP
  API to a local csv file.
* --findings_table holds the findings of PII identified from calling inspect on
  the DLP API.
* --mae_dir holds the findings in MAE format in GCS.
* --mae_table holds the findings in MAE format in BigQuery.

Example usage:

```shell
bazel build dlp:run_deid && \
bazel-bin/dlp/run_deid \
  --input_table "${PROJECT?}:${DATASET?}.dlp_input" \
  --deid_config_file dlp/sample_deid_config.json \
  --deid_table ${PROJECT?}:${DATASET?}.deid_output \
  --findings_table ${PROJECT?}:${DATASET?}.dlp_findings \
  --mae_table ${PROJECT:?}:${DATASET:?}.mae
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

## Config file

--deid_config_file specifies a json file (example in [sample_deid_config.json](http://github.com/GoogleCloudPlatform/healthcare-deid/tree/master/dlp/sample_deid_config.json))
that contains an object with the following fields:

1. `columns` (**required**) specifies up to three types of columns:

  * `inspect` (**required**): Columns that should be processed by the DLP API
    and have the results written to the output table. Only text identified as
    PHI by the DLP API will be transformed.
  * `passThrough`: Columns that should be passed through to the output table
    as-is.
  * `fieldTransform`: Columns that should have the entire cell's content
    transformed by deidentify() without trying to identify PHI, and have the
    results written to the output table. Each column here must have a
    corresponding entry in `fieldTransformations`. These columns are not
    included in the DLP API inspect() call, and therefore do not show up in
    findings or MAE results.

   If a `inspect` column specifies an `infoTypesToDeId` list, only those
   infoTypes will be used for deidentification for that column.

1. `infoTypeTransformations` is a list of [InfoTypeTransformation](https://cloud.google.com/dlp/docs/reference/rest/v2beta2/organizations.deidentifyTemplates#DeidentifyTemplate.InfoTypeTransformation)
   that will be sent to the DLP API's content.deidentify() method.

1. `fieldTransformations` is a list of [FieldTransformation](https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.deidentifyTemplates#DeidentifyTemplate.FieldTransformation)
   that will be sent to the DLP API's content.deidentify() method.

1. `keyColumns` is a list of columns that form a key uniquely identifying each
    row (used when generating MAE output).

1. `tagCategories` is a list of tags to use in the MAE output and the infoTypes
   from the deidConfig that should be mapped to those tags. Each entry in the
   list has a name, and a list of infoTypes, e.g.:

    ```none
    {
      "name": "FIRST_NAME",
      "infoTypes": ["US_FEMALE_NAME", "US_MALE_NAME"]
    }
    ```

1. `perRowTypes` is a list of columnName/infoTypeName pairs. For each inspect()
   and deid() call, for each row, we take the value from the specified column
   and add it as a custom infoType with the given name.

1. `perDatasetTypes` is similar to perRowTypes, but does a pre-processing step
   where it runs through the entire dataset and gets *all* the values for the
   given column(s). These are then passed to every inspect() and deid() request
   as custom infoTypes.

1. `customInfoTypes` is a list of [CustomInfoType](https://cloud.google.com/dlp/docs/reference/rest/v2/InspectConfig#CustomInfoType)
   objects to pass to the DLP API. You can read more about CustomInfoTypes
   [here](https://cloud.google.com/dlp/docs/creating-custom-infotypes), and
   there is an example of a custom info type for detecting medical record
   numbers available in [custom_mrn_deid_config.json](http://github.com/GoogleCloudPlatform/healthcare-deid/tree/master/dlp/custom_mrn_deid_config.json).

## MAE Format

If you specify the --mae_dir flag, the pipeline will write xml files to a GCS
directory. The files will be named using the structure "`record_id`.xml", where
`record_id` is a hyphen-separated list of fields specified in the config in
"keyColumns". A DTD file is written to "classification.dtd". Examples are
available under [mae_testdata](http://github.com/GoogleCloudPlatform/healthcare-deid/tree/master/dlp/mae_testdata)
in `sample.xml` and `sample.dtd`.

Similarly, if you specify --mae_table a table will be created with two columns:

1. `record_id`: Hyphen-separated list of fields specified in "keyColumns" in
   the config (e.g. "1234-1" if patient_id and record_number are specified).
1. `xml`: The MAE-compatible xml detailing the findings for that note.

Note that MAE output cannot be used if you specify multiple columns in your
config file in `columns.inspect`.

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
