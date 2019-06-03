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

# Build rules for MIST-on-GCP tools.

licenses(["notice"])  # Apache License 2.0

# Rules for running MIST on Docker on GCP.
py_library(
    name = "run_mist_lib",
    srcs = ["run_mist_lib.py"],
    srcs_version = "PY2AND3",
    deps = [
        "//common:run_docker",
    ],
)

py_binary(
    name = "run_mist",
    srcs = ["run_mist.py"],
    python_version = "PY3",
    srcs_version = "PY2AND3",
    deps = [
        ":run_mist_lib",
        requirement("google-cloud-storage"),
    ],
)

# BigQuery -> GCS rules.
py_library(
    name = "bigquery_to_gcs_lib",
    srcs = ["bigquery_to_gcs_lib.py"],
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
    python_version = "PY2",
    deps = [":bigquery_to_gcs_lib"],
)

# GCS -> BigQuery rules.
py_library(
    name = "gcs_to_bigquery_lib",
    srcs = ["gcs_to_bigquery_lib.py"],
    deps = [
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
        "//physionet:files_to_physionet_records",
    ],
)

py_binary(
    name = "gcs_to_bigquery",
    srcs = ["gcs_to_bigquery.py"],
    python_version = "PY2",
    deps = [":gcs_to_bigquery_lib"],
)

py_test(
    name = "gcs_to_bigquery_lib_test",
    srcs = ["gcs_to_bigquery_lib_test.py"],
    python_version = "PY2",
    deps = [":gcs_to_bigquery_lib"],
)
