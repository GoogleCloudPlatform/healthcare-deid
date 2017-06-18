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

# Build rules for PhysioNet-DeID-on-GCP tools.

licenses(["notice"])  # Apache License 2.0

py_binary(
    name = "physionet_deid_pipeline",
    srcs = ["physionet_deid_pipeline.py"],
    deps = [
        ":bigquery_to_gcs",
        ":gcs_to_bigquery",
        ":run_deid",
    ],
)

# BigQuery -> GCS rules.
py_library(
    name = "bigquery_to_gcs_lib",
    srcs = ["bigquery_to_gcs_lib.py"],
    deps = [
        "//third_party/py/apache_beam",
    ],
)

py_binary(
    name = "bigquery_to_gcs",
    srcs = ["bigquery_to_gcs.py"],
    deps = [":bigquery_to_gcs_lib"],
)

py_test(
    name = "bigquery_to_gcs_lib_test",
    srcs = ["bigquery_to_gcs_lib_test.py"],
    deps = [":bigquery_to_gcs_lib"],
)

# GCS -> BigQuery rules.
py_library(
    name = "gcs_to_bigquery_lib",
    srcs = ["gcs_to_bigquery_lib.py"],
    deps = [
        "//third_party/py/apache_beam",
    ],
)

py_binary(
    name = "gcs_to_bigquery",
    srcs = ["gcs_to_bigquery.py"],
    deps = [":gcs_to_bigquery_lib"],
)

py_test(
    name = "gcs_to_bigquery_lib_test",
    srcs = ["gcs_to_bigquery_lib_test.py"],
    deps = [":gcs_to_bigquery_lib"],
)

# Rules for running PhysioNet DeID on Docker on GCP.
py_library(
    name = "run_deid_lib",
    srcs = ["run_deid_lib.py"],
    deps = [
        "//third_party/py/apiclient",
        "//third_party/py/gcloud",
    ],
)

py_binary(
    name = "run_deid",
    srcs = ["run_deid.py"],
    deps = [":run_deid_lib"],
)

py_test(
    name = "run_deid_lib_test",
    srcs = ["run_deid_lib_test.py"],
    deps = [
        ":run_deid",
        "//third_party/py/mock",
    ],
)
