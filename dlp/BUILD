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

# Build rules for DLP API DeID tools.

licenses(["notice"])  # Apache License 2.0

py_library(
    name = "run_deid_lib",
    srcs = ["run_deid_lib.py"],
    deps = [
        "//common:mae",
        requirement("apache_beam"),
    ],
)

py_binary(
    name = "run_deid",
    srcs = ["run_deid.py"],
    deps = [
        ":run_deid_lib",
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "run_deid_lib_test",
    srcs = ["run_deid_lib_test.py"],
    data = [
        "//dlp/mae_testdata:sample.dtd",
        "//dlp/mae_testdata:sample.xml",
        "//dlp/testdata:batch_request.json",
        "//dlp/testdata:config.json",
        "//dlp/testdata:multi_column_config.json",
        "//dlp/testdata:multi_column_request.json",
        "//dlp/testdata:request.json",
    ],
    deps = [
        ":run_deid",
        "//common:beam_testutil",
        "//common:testutil",
        requirement("apache_beam"),
    ],
)
