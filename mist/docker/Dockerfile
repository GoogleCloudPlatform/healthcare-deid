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

FROM ubuntu:14.04

RUN apt-get update
RUN apt-get install -y openjdk-7-jdk python python-dev python-virtualenv unzip wget

# Set up the repository to install google-cloud-sdk.
RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN wget -O - https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
RUN apt-get update
RUN apt-get install -y google-cloud-sdk

# Download and install MIST.
RUN wget -O mist.zip https://sourceforge.net/projects/mist-deid/files/latest/download?source=files
RUN unzip mist.zip -d mist
RUN mist/`ls mist`/install.sh

# The path is dependent on the version, so we create a symlink to the
# installation directory and point $MAT_PKG_HOME at it.
RUN ln -s /mist/`ls mist`/src/MAT /mist_home
ENV MAT_PKG_HOME="/mist_home"
