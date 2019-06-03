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

load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")
load("@my_deps//:requirements.bzl", "requirement")

package(
    default_visibility = ["//visibility:public"],
)

# Build rules for DeID evaluation tools.

licenses(["notice"])  # Apache License 2.0

py_library(
    name = "eval_lib",
    srcs = ["eval_lib.py"],
    srcs_version = "PY2AND3",
    deps = [":results_py_pb2"],
)

py_test(
    name = "eval_lib_test",
    srcs = ["eval_lib_test.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":eval_lib",
        ":results_py_pb2",
    ],
)

py_library(
    name = "run_pipeline_lib",
    srcs = ["run_pipeline_lib.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":eval_lib",
        ":results_py_pb2",
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
        requirement("google-cloud-storage"),
        "//common:gcsutil",
    ],
)

py_test(
    name = "run_pipeline_lib_test",
    srcs = ["run_pipeline_lib_test.py"],
    data = ["//eval/testdata:expected_results"],
    deps = [
        ":results_py_pb2",
        ":run_pipeline_lib",
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
        "//common:beam_testutil",
        "//common:testutil",
    ],
)

py_binary(
    name = "run_pipeline",
    srcs = ["run_pipeline.py"],
    python_version = "PY3",
    deps = [
        ":run_pipeline_lib",
        requirement("google-cloud-storage"),
    ],
)

proto_library(
    name = "results_proto",
    srcs = ["results.proto"],
)

py_proto_library(
    name = "results_py_pb2",
    srcs = ["results.proto"],
)
