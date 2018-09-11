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
import cStringIO
import csv


class UTF8Recoder(object):
  """
  Iterator that reads an encoded stream and reencodes the input to UTF-8.
  """

  def __init__(self, f, encoding):
    self.reader = codecs.getreader(encoding)(f)

  def __iter__(self):
    return self

  def next(self):
    return self.reader.next().encode('utf-8')


class UnicodeReader(object):
  """
  A CSV reader which will iterate over lines in the CSV file 'f',
  which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwds):
    f = UTF8Recoder(f, encoding)
    self.reader = csv.reader(f, dialect=dialect, **kwds)

  def next(self):
    row = self.reader.next()
    return [unicode(s, 'utf-8') for s in row]

  def __iter__(self):
    return self


class UnicodeWriter(object):
  """
  A CSV writer which will write rows to CSV file 'f',
  which is encoded in the given encoding
  """

  def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwds):
    self.queue = cStringIO.StringIO()
    self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
    self.stream = f
    self.encoder = codecs.getincrementalencoder(encoding)()

  def writerow(self, row):
    self.writer.writerow([s.encode('utf-8') for s in row])
    data = self.queue.getvalue().decode('utf-8')
    data = self.encoder.encode(data)
    self.stream.write(data)
    self.queue.truncate(0)

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)


class DictWriter(csv.DictWriter):
  """
  A CSV DictWriter that supports unicode.
  """

  def __init__(self, f, fieldnames, restval='', extrasaction='raise',
               dialect='excel', encoding='utf-8', **kwds):
    self.encoding = encoding
    csv.DictWriter.__init__(self, f, fieldnames, restval,
                            extrasaction, dialect, **kwds)
    self.writer = UnicodeWriter(f, dialect, encoding, **kwds)

  def writerheader(self):
    header = dict(zip(self.fieldnames, self.fieldnames))
    self.writerow(header)

