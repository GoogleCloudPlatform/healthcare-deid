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

import collections
import sys

import apache_beam as beam
from apache_beam.coders.coders import ToStringCoder
from apache_beam.io import iobase
from common import testutil

_fake_bq_db = collections.defaultdict(list)


class FakeSource(iobase.BoundedSource):

  def __init__(self):
    self._records = []

  def get_range_tracker(self, unused_a, unused_b):
    return None

  def read(self, unused_range_tracker):
    for record in self._records:
      yield record


class DummyWriteTransform(beam.PTransform):
  """A transform that replaces iobase.WriteToText in tests."""

  def __init__(self, filename=None):
    gs_prefix = 'gs://'
    if filename.startswith(gs_prefix):
      filename = filename[len(gs_prefix):]
    self.filename = filename

  class WriteDoFn(beam.DoFn):
    """DoFn to write to fake GCS."""

    def __init__(self, filename):
      self.filename = filename
      self.file_obj = None
      self.coder = ToStringCoder()

    def start_bundle(self):
      pass

    def process(self, element):
      e = element
      if sys.version < '3':
        e = self.coder.encode(element)
      testutil.append_to_gcs_file(self.filename, e + '\n')

    def finish_bundle(self):
      pass

  def expand(self, pcoll):
    return pcoll | 'DummyWriteForTesting' >> beam.ParDo(
        DummyWriteTransform.WriteDoFn(self.filename))


class _FakeBqWriter(iobase.Writer):

  def __init__(self, table_name):
    self._table_name = table_name
    _fake_bq_db[table_name] = []

  def write(self, value):
    _fake_bq_db[self._table_name].append(value)

  def close(self):
    pass


class FakeSink(iobase.Sink):
  """Fake BigQuery sink object."""

  def __init__(self, table_name):
    self._writer = _FakeBqWriter(table_name)

  def initialize_write(self):
    pass

  def open_writer(self, unused_init_result, unused_uid):
    return self._writer

  def pre_finalize(self, unused_init_result, unused_writer_results):
    pass

  def finalize_write(self, unused_access_token, unused_table_names,
                     unused_pre_finalize_results=None):
    pass


def get_table(table_name):
  return _fake_bq_db[table_name]
