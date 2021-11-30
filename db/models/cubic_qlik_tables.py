
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, VARCHAR, DATE

from db.models.base import Base


class CubicQlikTable(Base):

  __tablename__ = 'cubic_qlik_tables'

  name = Column(VARCHAR(500), nullable=False)
  s3_prefix = Column(VARCHAR(1000), nullable=False)
