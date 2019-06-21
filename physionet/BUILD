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

load("@my_deps//:requirements.bzl", "requirement")

package(
    default_visibility = ["//visibility:public"],
)

# Build rules for PhysioNet-DeID-on-GCP tools.

licenses(["notice"])  # Apache License 2.0

py_binary(
    name = "physionet_deid_pipeline",
    srcs = ["physionet_deid_pipeline.py"],
    python_version = "PY2",
    deps = [
        ":bigquery_to_gcs_lib",
        ":gcs_to_bigquery_lib",
        ":physionet_to_mae_lib",
        ":run_deid_lib",
        requirement("google-cloud-storage"),
    ],
)

# BigQuery -> GCS rules.
py_library(
    name = "bigquery_to_gcs_lib",
    srcs = ["bigquery_to_gcs_lib.py"],
    srcs_version = "PY2AND3",
    deps = [
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
    ],
)

py_binary(
    name = "bigquery_to_gcs",
    srcs = ["bigquery_to_gcs.py"],
    python_version = "PY2",
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
    srcs_version = "PY2AND3",
    deps = [
        ":files_to_physionet_records",
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
    ],
)

py_binary(
    name = "gcs_to_bigquery",
    srcs = ["gcs_to_bigquery.py"],
    python_version = "PY2",
    deps = [":gcs_to_bigquery_lib"],
)

# Rules for running PhysioNet DeID on Docker on GCP.
py_library(
    name = "run_deid_lib",
    srcs = ["run_deid_lib.py"],
    srcs_version = "PY2AND3",
    deps = [
        requirement("google-auth-httplib2"),  # fixdeps: keep
        "//common:run_docker",
    ],
)

py_binary(
    name = "run_deid",
    srcs = ["run_deid.py"],
    python_version = "PY2",
    deps = [
        ":run_deid_lib",
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "run_deid_lib_test",
    srcs = ["run_deid_lib_test.py"],
    srcs_version = "PY2AND3",
    deps = [":run_deid_lib"],
)

# Rules for converting physionet output to MAE format.
py_library(
    name = "physionet_to_mae_lib",
    srcs = ["physionet_to_mae_lib.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":files_to_physionet_records",
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
        requirement("google-cloud-storage"),
        "//common:gcsutil",
        "//common:mae",
    ],
)

py_test(
    name = "physionet_to_mae_lib_test",
    srcs = ["physionet_to_mae_lib_test.py"],
    deps = [
        ":physionet_to_mae_lib",
        "//common:testutil",
    ],
)

py_binary(
    name = "physionet_to_mae",
    srcs = ["physionet_to_mae.py"],
    python_version = "PY3",
    deps = [
        ":physionet_to_mae_lib",
    ],
)

# Helper library.
py_library(
    name = "files_to_physionet_records",
    srcs = ["files_to_physionet_records.py"],
    srcs_version = "PY2AND3",
    deps = [
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
    ],
)

py_test(
    name = "files_to_physionet_records_test",
    srcs = ["files_to_physionet_records_test.py"],
    deps = [":files_to_physionet_records"],
)
