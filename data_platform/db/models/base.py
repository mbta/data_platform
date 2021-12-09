
import datetime
from sqlalchemy import event, Column
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP


@as_declarative()
class Base(object):

  id = Column(INTEGER, primary_key=True)
  created = Column(TIMESTAMP, nullable=False)
  modified = Column(TIMESTAMP, nullable=False)
  deleted = Column(TIMESTAMP, nullable=True)

  # before an insert
  @staticmethod
  def before_insert(mapper, connection, target):
    now = datetime.datetime.utcnow()
    # set created, if not specifically set
    if not target.created:
      target.created = now
    # set modified, if not specifically set
    if not target.modified:
      target.modified = now

  # before an update
  @staticmethod
  def before_update(mapper, connection, target):
    # set modified
    target.modified = datetime.datetime.utcnow()

  @classmethod
  def __declare_last__(cls):
    # get called after mappings are completed
    # http://docs.sqlalchemy.org/en/rel_0_7/orm/extensions/declarative.html#declare-last
    event.listen(cls, 'before_insert', cls.before_insert)
    event.listen(cls, 'before_update', cls.before_update)
