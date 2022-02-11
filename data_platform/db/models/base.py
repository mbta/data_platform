
import datetime
from sqlalchemy import event, Column
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP


@as_declarative()
class Base(object):

  id = Column(INTEGER, primary_key=True)
  inserted_at = Column(TIMESTAMP, nullable=False)
  updated_at = Column(TIMESTAMP, nullable=False)
  deleted_at = Column(TIMESTAMP, nullable=True)

  # before an insert
  @staticmethod
  def before_insert(mapper, connection, target):
    now = datetime.datetime.utcnow()
    # set inserted_at, if not specifically set
    if not target.inserted_at:
      target.inserted_at = now
    # set updated_at, if not specifically set
    if not target.updated_at:
      target.updated_at = now

  # before an update
  @staticmethod
  def before_update(mapper, connection, target):
    # set updated_at
    target.updated_at = datetime.datetime.utcnow()

  @classmethod
  def __declare_last__(cls):
    # get called after mappings are completed
    # http://docs.sqlalchemy.org/en/rel_0_7/orm/extensions/declarative.html#declare-last
    event.listen(cls, 'before_insert', cls.before_insert)
    event.listen(cls, 'before_update', cls.before_update)
