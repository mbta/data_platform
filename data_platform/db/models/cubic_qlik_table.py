
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, VARCHAR, DATE

from db.models.base import Base



# create table metadata__tables (
#   id bigint,
#   name varchar,
#   modified_field varchar,
#   partition_field varchar,
# )



class CubicQlikTable(Base):

  __tablename__ = 'cubic_qlik_tables'

  name = Column(VARCHAR(500), nullable=False)
  s3_prefix = Column(VARCHAR(1000), nullable=False)

def get():
  pass
