import json
import os
import time
import dask.bag as db
import dask
import dask.dataframe as dd
import psycopg2
import sqlalchemy as sql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from src.main.a360_data_compute.descriptor.service.MetadataProcessor import MetadataProcessor
from src.main.a360_data_compute.persistence.ProcessResults import ProcessResult
from src.main.a360_data_compute.utils.LogUtility import LogUtility

dask.config.set(scheduler='threads')
Base = declarative_base()


class Processor:
    log_utility = LogUtility()

    def __init__(self, file_path, process_id):
        self.file_path = file_path
        self.process_id = process_id
        self.engine = None

    def execute_process(self, remote_files, request_dto, ftp):
        Processor.log_utility.log_info(f"Starting process {self.process_id}")
        Processor.log_utility.log_info("Starting to read CSV file")

        chunk_size = 100000000
        updated_metadata = self.read_csv(remote_files, chunk_size, request_dto.get('connection_type'), ftp)

        Processor.log_utility.log_info("Completed")
        return updated_metadata

    def end_process(self, json_list):
        output_format = "{time}_{process_id}.json"
        output_path = output_format.format(time=time.time(), process_id=self.process_id)
        user = 'postgres'
        password = 'postgres'
        host = 'localhost'
        port = '5434'
        database = 'postgres'
        with open(output_path, 'w') as json_file:
            json.dump(json_list, json_file, indent=4)

        connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        try:
            self.engine = sql.create_engine(connection_str, module=psycopg2)
            with self.engine.connect() as connection_str:
                Processor.log_utility.log_info('Successfully connected to the PostgreSQL database')
        except Exception as ex:
            Processor.log_utility.log_error(f'Failed to connect: {ex}')

        Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        session = Session()

        process_result = ProcessResult(process_id=self.process_id, json_data=json_list)
        session.add(process_result)
        session.commit()
        return output_path

    def read_csv(self, files, chunk_size, connection_type, ftp):
        files_bag = db.from_sequence(files)
        delayed_tasks = files_bag.map(
            lambda file: dask.delayed(self._process_file)(file, chunk_size, connection_type, ftp)
        )
        combined_metadata = dask.compute(*delayed_tasks)

        return Processor.merge_metadata(combined_metadata)

    @staticmethod
    @dask.delayed
    def _process_file(file, chunk_size, connection_type, ftp):
        try:
            block_size_gb = 1
            block_size_bytes = block_size_gb * 1024 ** 3

            if connection_type == 'SFTP':
                file_size = file.stat().st_size
            elif connection_type == 'Local Storage':
                file_size = os.path.getsize(file)
            elif connection_type == 'FTP':
                file_size = ftp.size(file)
            else:
                Processor.log_utility.log_warning(f"Unsupported connection type: {connection_type}")
                return None

            data_frame = dd.read_csv(file, assume_missing=True, dtype='object', blocksize=block_size_bytes)
            Processor.log_utility.log_info("File reading completed")

            metadata = MetadataProcessor.generate_metadata(data_frame, 'schema_name', 'table_name')

            return metadata

        except UnicodeDecodeError as e:
            Processor.log_utility.log_error(f"Error decoding file: {file}. {e}")
            return None
        except Exception as e:
            Processor.log_utility.log_error(f"Error processing file: {file}. {e}")
            return None

    def calculate_partitions(self, file_size, chunk_size):
        return (file_size + chunk_size - 1) // chunk_size

    @staticmethod
    def merge_metadata(updated_metadata_list):
        if not updated_metadata_list:
            Processor.log_utility.log_warning("List of updated metadata is empty. Unable to combine.")
            return None
        combined_metadata = [updated_metadata_list[0].to_dict()]

        for metadata in updated_metadata_list[1:]:
            if metadata is not None:
                metadata_dict = metadata.to_dict()
                combined_metadata.append(metadata_dict)

        return combined_metadata
