from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON

Base = declarative_base()


class ProcessResult(Base):
    __tablename__ = 'process_results'

    id = Column(Integer, primary_key=True)
    process_id = Column(Integer)
    json_data = Column(JSON)
