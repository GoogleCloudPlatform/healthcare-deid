"""Setup module for the healthcare_deid DLP pipeline.

All of the code necessary to run the pipeline is packaged into a source
distribution that is uploaded to the --staging_location specified on the command
line.  The source distribution is then installed on the workers before they
start running.

When remotely executing the pipeline, `--setup_file path/to/setup.py` must be
added to the pipeline's command line.
"""

import os
import setuptools


# Add required python packages that should be installed over and above the
# standard DataFlow worker environment.  Version restrictions are supported if
# necessary.
REQUIRED_PACKAGES = [
    'apache_beam[gcp]',
    'google-api-python-client',
    'google-cloud-storage',
    'six==1.10.0',
]

packages = ['common', 'dlp', 'physionet']
package_dir = {p: p for p in packages}
# Use eval from bazel-bin so we get the generated results_pb2.py file.
# If it doesn't exist, then the job is another pipeline that doesn't need eval.
eval_bazel_path = 'bazel-bin/eval/run_pipeline.runfiles/__main__/eval'
if os.path.exists(eval_bazel_path):
  packages.append('eval')
  package_dir['eval'] = eval_bazel_path

setuptools.setup(
    name='healthcare_deid',
    version='0.0.1',
    package_dir=package_dir,
    description='Healthcare Deid pipeline package.',
    install_requires=REQUIRED_PACKAGES,
    packages=packages)
