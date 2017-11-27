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

new_http_archive(
    name = "six_archive",
    build_file = "six.BUILD",
    url = "https://pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
)

bind(
    name = "six",
    actual = "@six_archive//:six",
)

git_repository(
    name = "org_pubref_rules_protobuf",
    remote = "https://github.com/pubref/rules_protobuf.git",
    tag = "v0.8.1",
)

load("@org_pubref_rules_protobuf//python:rules.bzl", "py_proto_repositories")
py_proto_repositories()

git_repository(
    name = "io_bazel_rules_python",
    remote = "https://github.com/bazelbuild/rules_python.git",
    commit = "3e167dcfb17356c68588715ed324c5e9b76f391d",
)

# For PIP support:
load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories")

pip_repositories()

load("@io_bazel_rules_python//python:pip.bzl", "pip_import")
# This rule translates the specified requirements.txt into
# @my_deps//:requirements.bzl, which itself exposes a pip_install method.
pip_import(
   name = "my_deps",
   requirements = "//requirements:requirements.txt",
)

# Load the pip_install symbol for my_deps, and create the dependencies'
# repositories.
load("@my_deps//:requirements.bzl", "pip_install")
pip_install()
