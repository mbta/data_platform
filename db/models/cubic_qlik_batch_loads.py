
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, VARCHAR, DATE

from db.models.base import Base


class CubicQlikBatchLoad(Base):

  __tablename__ = 'cubic_qlik_batch_loads'

  table_id = Column(INTEGER, nullable=False)

  s3_key = Column(VARCHAR(1000), nullable=False)
  status = Column(VARCHAR(100), nullable=False)
