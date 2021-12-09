
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, VARCHAR, DATE

from data_platform.db.models.base import Base




# create table metadata__ct_loads (
#   id,
#   table_id,
#   created datetime,
#   modified datetime,

#   -- load
#   object_key,
#   size bigint,
#   status enum('available', 'processing', 'processed')

#   -- process
#   started datetime,
#   completed datetime,

#   -- info on rows
#   rows_processed bigint,
#   rows_inserted bigint,
#   rows_updated bigint,
#   rows_deleted bigint,
#   rows_max_modified datetime, -- the max modified from all the rows

# );





class CubicQlikCDCLoad(Base):

  __tablename__ = 'cubic_qlik_cdc_loads'

  table_id = Column(INTEGER, nullable=False)

  s3_key = Column(VARCHAR(1000), nullable=False) # @todo maybe unique
  status = Column(VARCHAR(100), nullable=False)

def get():
  pass
