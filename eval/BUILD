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
load("@org_pubref_rules_protobuf//python:rules.bzl", "py_proto_library")

package(
    default_visibility = ["//visibility:public"],
)

# Build rules for DeID evaluation tools.

licenses(["notice"])  # Apache License 2.0

py_library(
    name = "run_pipeline_lib",
    srcs = ["run_pipeline_lib.py"],
    deps = [
        ":results_py_pb2",
        "//common:gcsutil",
        requirement("apache_beam"),
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "run_pipeline_lib_test",
    srcs = ["run_pipeline_lib_test.py"],
    data = ["//eval/testdata:expected_results"],
    deps = [
        ":results_py_pb2",
        ":run_pipeline_lib",
        "//common:beam_testutil",
        "//common:testutil",
        requirement("apache_beam"),
    ],
)

py_binary(
    name = "run_pipeline",
    srcs = ["run_pipeline.py"],
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
    protos = ["results.proto"],
)
