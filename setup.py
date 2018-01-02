"""Setup module for the healthcare_deid DLP pipeline.

All of the code necessary to run the pipeline is packaged into a source
distribution that is uploaded to the --staging_location specified on the command
line.  The source distribution is then installed on the workers before they
start running.

When remotely executing the pipeline, `--setup_file path/to/setup.py` must be
added to the pipeline's command line.
"""

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

setuptools.setup(
    name='healthcare_deid',
    version='0.0.1',
    description='Healthcare Deid pipeline package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages())
