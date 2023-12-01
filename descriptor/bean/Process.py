import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()

class ProcessResult(Base):
    __tablename__ = 'process_status'

    id = Column(Integer, primary_key=True)
    process_id = Column(Integer)
    status = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)

class Process:
    end_time = 0.0
    result_path: str
    def __init__(self, process_id, future, status):
        self.process_id = process_id
        self.future = future
        self.status = status
        self.start_time = time.time()

    def to_dict(self):
        return {
            "process_id": self.process_id,
            "start_time": datetime.fromtimestamp(self.start_time).strftime('%F %T.%f')[:-3],
            "end_time": datetime.fromtimestamp(self.end_time).strftime('%F %T.%f')[:-3],
            "time_taken": str(timedelta(seconds=self.end_time - self.start_time))
        }


    def to_dict_result(self):
        return {
            "process_id": self.process_id,
            "start_time": datetime.fromtimestamp(self.start_time).strftime('%F %T.%f')[:-3],
            "end_time": datetime.fromtimestamp(self.end_time).strftime('%F %T.%f')[:-3],
            "time_taken": str(timedelta(seconds=self.end_time - self.start_time)),
            "result": self.result_path
        }

    def update_status(self):

        print(f"Updating status in the database for process_id {self.process_id} to {self.status}")

        user = 'postgres'
        password = 'postgres'
        host = 'localhost'
        port = '5434'
        database = 'postgres'

        connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'

        engine = create_engine(connection_str)
        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        process_result = session.query(ProcessResult).filter_by(process_id=self.process_id).first()

        if process_result:
            process_result.status = self.status
            process_result.end_time = datetime.fromtimestamp(self.end_time)
            session.commit()
            print(f"Updated status in the database for process_id {self.process_id} to {self.status}")
        else:
            print(f"Process with ID {self.process_id} not found in the database.")

        session.close()


    def create_record(self):

        print(f"Creating a new record in the database for process_id {self.process_id} with status {self.status}")

        user = 'postgres'
        password = 'postgres'
        host = 'localhost'
        port = '5434'
        database = 'postgres'

        connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'

        engine = create_engine(connection_str)
        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        process_result = ProcessResult(
            process_id=self.process_id,
            status=self.status,
            start_time=datetime.fromtimestamp(self.start_time),
            end_time=datetime.fromtimestamp(self.end_time)
        )

        session.add(process_result)
        session.commit()
        print(f"Record created in the database for process_id {self.process_id} with status {self.status}")

        session.close()

