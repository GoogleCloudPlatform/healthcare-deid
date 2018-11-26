"""Provides configuration for running the flask server.

helps with separating the configuration from the actual app.
"""

from __future__ import absolute_import

import os

import google.auth

APP_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'
if APP_CREDENTIALS not in os.environ or not os.environ[APP_CREDENTIALS]:
  raise Exception('You must specify service account credentials in the '
                  'GOOGLE_APPLICATION_CREDENTIALS environment variable.')
_, default_project = google.auth.default()


class Config(object):
  """Config parameters to run the Deid App server and DB."""
  SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev'
  SERVER_NAME = 'localhost:5000'
  PROJECT_ID = os.environ.get('PROJECT_ID') or default_project
  DLP_API_NAME = 'dlp'
  DEID_CONFIG_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                  'sample_deid_config.json')
  CLOUDSQL_USER = os.environ.get('CLOUDSQL_USER')
  CLOUDSQL_PASSWORD = os.environ.get('CLOUDSQL_PASSWORD')
  CLOUDSQL_DATABASE = os.environ.get('CLOUDSQL_DATABASE')
  CLOUDSQL_CONNECTION_NAME = os.environ.get('CLOUDSQL_CONNECTION_NAME')
  SQLALCHEMY_TRACK_MODIFICATIONS = False
  LOCAL_SQLALCHEMY_DATABASE_URI = (
      'mysql+pymysql://{user}:{password}@127.0.0.1:3306/{database}').format(
          user=CLOUDSQL_USER, password=CLOUDSQL_PASSWORD,
          database=CLOUDSQL_DATABASE)
  LIVE_SQLALCHEMY_DATABASE_URI = (
      'mysql+pymysql://{user}:{password}@localhost/{database}'
      '?unix_socket=/cloudsql/{connection_name}').format(
          user=CLOUDSQL_USER, password=CLOUDSQL_PASSWORD,
          database=CLOUDSQL_DATABASE, connection_name=CLOUDSQL_CONNECTION_NAME)
  if os.environ.get('GAE_INSTANCE'):
    SQLALCHEMY_DATABASE_URI = LIVE_SQLALCHEMY_DATABASE_URI
  else:
    SQLALCHEMY_DATABASE_URI = LOCAL_SQLALCHEMY_DATABASE_URI
