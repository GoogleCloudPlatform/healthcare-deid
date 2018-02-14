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

"""Utilities for unit tests."""

from __future__ import absolute_import

import posixpath

_fake_gcs = {}


class _FakeReader(object):

  def __init__(self, contents):
    self.contents = contents.split('\n')
    for i in range(len(self.contents)-1):
      self.contents[i] += '\n'

  def __iter__(self):
    return self.contents.__iter__()


def fake_open(filename):
  gs_prefix = 'gs://'
  if filename.startswith(gs_prefix):
    filename = filename[len(gs_prefix):]
  return _FakeReader(_fake_gcs[filename])


def set_gcs_file(filename, contents):
  _fake_gcs[filename] = contents


def append_to_gcs_file(filename, contents):
  if filename not in _fake_gcs:
    return set_gcs_file(filename, contents)
  _fake_gcs[filename] += contents


def get_gcs_file(filename):
  return _fake_gcs[filename]


class _FakeBlob(object):

  def __init__(self, bucket_name, file_name):
    self._file_name = posixpath.join(bucket_name, file_name)
    self.name = file_name

  def upload_from_string(self, contents):
    _fake_gcs[self._file_name] = contents

  def download_as_string(self):
    return _fake_gcs[self._file_name]


class _FakeBucket(object):
  """Fake GCS bucket object."""

  def __init__(self, bucket_name):
    self._bucket_name = bucket_name

  def blob(self, file_name):
    return _FakeBlob(self._bucket_name, file_name)

  def list_blobs(self, prefix):
    blobs = []
    for name in _fake_gcs:
      full_prefix = posixpath.join(self._bucket_name, prefix)
      if name.startswith(full_prefix):
        blob_name = name[len(self._bucket_name)+1:]
        blobs.append(_FakeBlob(self._bucket_name, blob_name))
    return blobs

  def get_blob(self, name):
    if posixpath.join(self._bucket_name, name) not in _fake_gcs:
      raise Exception('blob {0} not found in bucket {1}',
                      name, self._bucket_name)
    return _FakeBlob(self._bucket_name, name)


class FakeStorageClient(object):

  def get_bucket(self, bucket_name):
    return _FakeBucket(bucket_name)

  def lookup_bucket(self, bucket_name):
    return _FakeBucket(bucket_name)

