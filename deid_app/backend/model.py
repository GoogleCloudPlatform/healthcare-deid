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

"""defines database access for deid app in Cloud SQL."""

from __future__ import absolute_import

import logging

import flask
from flask_sqlalchemy import SQLAlchemy
from deid_app.backend import config

db = SQLAlchemy()


def init_app(app):
  """initialize the database model and write the table to the DB."""
  # Disable track modifications, as it unnecessarily uses memory.
  app.config.setdefault('SQLALCHEMY_TRACK_MODIFICATIONS', False)
  db.init_app(app)


def from_sql(row):
  """Translates an SQLAlchemy model instance into a dictionary."""
  if not row:
    return None
  data = row.__dict__.copy()
  data['id'] = row.id
  data.pop('_sa_instance_state')
  return data


class DeidJobTable(db.Model):
  """Stores jobs created by running DLP deid pipeline."""
  __tablename__ = 'deid_jobs'

  id = db.Column(db.Integer, primary_key=True)
  name = db.Column(db.String(255))
  original_query = db.Column(db.String(255))
  deid_table = db.Column(db.String(255))
  findings_table = db.Column(db.String(255))
  status = db.Column(db.String(255))
  log_trace = db.Column(db.Text)
  timestamp = db.Column(db.TIMESTAMP)

  def __repr__(self):
    return ('<DeidJobTable(id={}, name={}, original={}, deidentified={}, '
            'status={}, log_trace={}, timestamp={}').format(
                self.id, self.name, self.original_query, self.deid_table,
                self.status, self.log_trace, self.timestamp)

  def update(self, **data):
    for k, v in data.items():
      setattr(self, k, v)
    db.session.commit()
    return from_sql(self)


class EvalJobTable(db.Model):
  """Stores jobs created by running the evaluation pipeline."""
  __tablename__ = 'eval_jobs'

  id = db.Column(db.Integer, primary_key=True)
  name = db.Column(db.String(255))
  findings = db.Column(db.String(255))
  goldens = db.Column(db.String(255))
  stats = db.Column(db.String(255))
  debug = db.Column(db.String(255))
  status = db.Column(db.Integer)
  log_trace = db.Column(db.Text)
  timestamp = db.Column(db.DateTime)

  def __repr__(self):
    return ('<EvalJobTable(id={}, name={}, findings={}, goldens={}, stats={}, '
            'debug={}>').format(self.id, self.name, self.findings, self.goldens,
                                self.stats, self.debug)

  def update(self, **data):
    for k, v in data.items():
      setattr(self, k, v)
    db.session.commit()


def get_list(table, limit=None, cursor=0):
  """Lists rows in a table up to a limit.

  Args:
    table: The target class to list the rows from. Must have an id column as a
      primary key.
    limit: The maximum number of rows to return.
    cursor: An integer with the offset from the start of the table.
  Returns:
    A tuple of the rows and the offset for the next call to list.
  """
  if not limit:
    query = (table.query
             .order_by(table.id)
             .offset(cursor))
  else:
    query = (table.query
             .order_by(table.id)
             .limit(limit)
             .offset(cursor))
  rows = [from_sql(row) for row in query.all()]
  next_page = cursor + limit if len(rows) == limit else 0
  return (rows, next_page)


def create(table, data):
  """Creates an object of type table and writes it to that table.

  Args:
    table: A class type that inherits from db.Model.
    data: A dictionary of column:value pairs to be stored inside the created
      object.
  Returns:
    A reference to the created object inside the table.
  """
  job = table(**data)
  db.session.add(job)
  db.session.commit()
  return job


def _create_database():
  """create all the tables necessary to run the application."""
  app = flask.Flask(__name__)
  app.config.from_object(config.Config)
  init_app(app)
  with app.app_context():
    db.drop_all()
    db.create_all()
  logging.info('All tables created')

if __name__ == '__main__':
  _create_database()
