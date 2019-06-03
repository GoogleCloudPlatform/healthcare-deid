# Copyright 2018 Google Inc. All rights reserved.
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

"""CSV Reader and Writer that support unicode."""

from __future__ import absolute_import

import codecs
import csv
import io
import sys
try:
  import cStringIO  # pylint: disable=g-import-not-at-top
except ImportError:
  pass


class UTF8Recoder(object):
  """Iterator that reads an encoded stream and reencodes the input to UTF-8."""

  def __init__(self, f, encoding):
    if sys.version > '3':
      self.reader = f
    else:
      self.reader = codecs.getreader(encoding)(f)

  def __iter__(self):
    return self

  def __next__(self):
    return self.next()

  def next(self):
    if sys.version > '3':
      return next(self.reader)
    return next(self.reader).encode('utf-8')


class UnicodeReader(object):
  """A Unicode CSV reader.

  Iterates over lines in the CSV file, which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwds):
    f = UTF8Recoder(f, encoding)
    self.reader = csv.reader(f, dialect=dialect, **kwds)

  def next(self):
    row = next(self.reader)
    if sys.version > '3':
      return row
    return [unicode(s, 'utf-8') for s in row]

  def __next__(self):
    return self.next()

  def __iter__(self):
    return self


class UnicodeWriter(object):
  """A Unicode CSV writer."""

  def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwds):
    try:
      self.queue = cStringIO.StringIO()
    except NameError:
      self.queue = io.StringIO()
    self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
    self.stream = f
    self.encoder = codecs.getincrementalencoder(encoding)()

  def writerow(self, row):
    """Writes a row."""
    if sys.version < '3':
      self.writer.writerow([s.encode('utf-8') for s in row])
    else:
      self.writer.writerow(row)
    data = self.queue.getvalue()
    if sys.version < '3':
      data = data.decode('utf-8')
      data = self.encoder.encode(data)
    self.stream.write(data)
    self.queue.truncate(0)

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)


class DictWriter(csv.DictWriter):
  """A CSV DictWriter that supports unicode."""

  def __init__(self, f, fieldnames, restval='', extrasaction='raise',
               dialect='excel', encoding='utf-8', **kwds):
    if sys.version > '3':
      csv.DictWriter.__init__(self, f, fieldnames, restval, extrasaction,
                              dialect, **kwds)
      return
    self.encoding = encoding
    csv.DictWriter.__init__(self, f, fieldnames, restval,
                            extrasaction, dialect, **kwds)
    self.writer = UnicodeWriter(f, dialect, encoding, **kwds)

  def writerheader(self):
    header = dict(zip(self.fieldnames, self.fieldnames))
    self.writerow(header)
