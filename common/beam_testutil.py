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

import apache_beam as beam
from apache_beam.coders.coders import ToStringCoder
from common import testutil


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
      testutil.append_to_gcs_file(self.filename,
                                  self.coder.encode(element) + '\n')

    def finish_bundle(self):
      pass

  def expand(self, pcoll):
    return pcoll | 'DummyWriteForTesting' >> beam.ParDo(
        DummyWriteTransform.WriteDoFn(self.filename))
