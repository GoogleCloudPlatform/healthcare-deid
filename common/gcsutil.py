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


"""Get files from a GCS bucket matching a pattern."""

from __future__ import absolute_import

import logging
import re


class GcsFileName(object):
  """Holds a gs:// filename with bucket and blob components."""

  def __init__(self, bucket, blob):
    self.bucket = bucket
    self.blob = blob

  @classmethod
  def from_path(cls, path):
    # Split the input path to get the bucket name and path within the bucket.
    re_match = re.match(r'gs://([\w-]+)/(.*)', path)
    if not re_match or len(re_match.groups()) != 2:
      err = ('Failed to parse input path: "{0}". Expected: '
             'gs://bucket-name/path/to/file'.format(path))
      logging.error(err)
      raise Exception(err)
    return cls(re_match.group(1), re_match.group(2))

  def string(self):
    return 'gs://{0}/{1}'.format(self.bucket, self.blob)

  def __str__(self):
    return self.string()


def find_files(pattern, storage_client):
  """Find files on GCS matching the given pattern."""
  f = GcsFileName.from_path(pattern)
  bucket_name = f.bucket
  file_pattern = f.blob

  # The storage client doesn't take a pattern, just a prefix, so we presume here
  # that the only special/regex-like characters used are '?' and '*', and take
  # the longest prefix that doesn't contain either of those.
  file_prefix = file_pattern
  re_result = re.search(r'(.*?)[\?|\*]', file_pattern)
  if re_result:
    file_prefix = re_result.group(1)

  # Convert file_pattern to a regex by escaping the string, explicitly
  # converting the characters we want to treat specially (* and ?), and
  # appending '\Z' to the end of the pattern so we match only the full string.
  file_pattern_as_regex = (
      re.escape(file_pattern).replace('\\*', '.*').replace('\\?', '.') + r'\Z')

  bucket = storage_client.lookup_bucket(bucket_name)
  if not bucket:
    raise Exception('Could not find bucket: "{}"'.format(bucket_name))
  for blob in bucket.list_blobs(prefix=file_prefix):
    if not re.match(file_pattern_as_regex, blob.name):
      continue
    yield GcsFileName(bucket_name, blob.name)
