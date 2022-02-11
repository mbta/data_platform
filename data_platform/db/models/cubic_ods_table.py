
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, VARCHAR, DATE

from data_platform.db.models.base import Base


class CubicODSTable(Base):

  __tablename__ = 'cubic_ods_tables'

  name = Column(VARCHAR(500), nullable=False)
  snapshot = Column(TIMESTAMP, nullable=True)
  snapshot_s3_key = Column(VARCHAR(1000), nullable=False)

  s3_prefix = Column(VARCHAR(1000), nullable=False)
