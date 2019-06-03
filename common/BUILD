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

licenses(["notice"])  # Apache License 2.0

py_library(
    name = "run_docker",
    srcs = ["run_docker.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":gcsutil",
        requirement("google-auth-httplib2"),
    ],
)

py_test(
    name = "run_docker_test",
    srcs = ["run_docker_test.py"],
    python_version = "PY2",
    srcs_version = "PY2AND3",
    deps = [
        ":gcsutil",
        ":run_docker",
    ],
)

py_library(
    name = "mae",
    srcs = ["mae.py"],
    srcs_version = "PY2AND3",
)

py_test(
    name = "mae_test",
    srcs = ["mae_test.py"],
    python_version = "PY2",
    srcs_version = "PY2AND3",
    deps = [
        ":mae",
    ],
)

py_library(
    name = "gcsutil",
    srcs = ["gcsutil.py"],
    srcs_version = "PY2AND3",
)

py_library(
    name = "testutil",
    srcs = ["testutil.py"],
    srcs_version = "PY2AND3",
)

py_library(
    name = "beam_testutil",
    srcs = ["beam_testutil.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":testutil",
        requirement("apache_beam"),
        requirement("pyarrow"),
        requirement("numpy"),
        requirement("fastavro"),
        requirement("google-api-core"),
        requirement("google-apitools"),
    ],
)

py_library(
    name = "unicodecsv",
    srcs = ["unicodecsv.py"],
    srcs_version = "PY2AND3",
)
