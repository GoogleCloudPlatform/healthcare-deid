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

"""Stand-alone executable version of physionet_to_mae_lib."""

from __future__ import absolute_import

import argparse
import logging
import sys

from physionet import physionet_to_mae_lib


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description=('Convert files from PhysioNet to MAE.'))
  physionet_to_mae_lib.add_all_args(parser)
  args, pipeline_args = parser.parse_known_args(sys.argv[1:])
  # --project is used both as a local arg and a pipeline arg, so parse it, then
  # add it to pipeline_args as well.
  pipeline_args += ['--project', args.project]

  physionet_to_mae_lib.run_pipeline(args.input_pattern, args.mae_output_dir,
                                    args.mae_task_name, args.project,
                                    pipeline_args)


if __name__ == '__main__':
  main()
