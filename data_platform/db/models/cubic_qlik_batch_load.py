
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, VARCHAR, DATE

from db.models.base import Base



# create table metadata__full_loads (
#   id bigint,
#   table_id bigint,
#   created datetime,
#   modified datetime,

#   -- load
#   object_key varchar,
#   size long bigint,
#   status enum('available', 'processing', 'processed')

#   -- process
#   started datetime,
#   completed datetime,

#   -- info on rows
#   rows_processed bigint,
#   rows_max_modified datetime, -- the max modified from all the rows. if modified_field null, S3 object timestamp is used

# );



class CubicQlikBatchLoad(Base):

  __tablename__ = 'cubic_qlik_batch_loads'

  table_id = Column(INTEGER, nullable=False)

  s3_key = Column(VARCHAR(1000), nullable=False)
  status = Column(VARCHAR(100), nullable=False)

def get():
  pass
