import json
import logging
import os
import sys
import time
import dask.bag as db
import dask
import dask.dataframe as dd
import psycopg2
import sqlalchemy as sql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from descriptor.service.MetadataProcessor import MetadataProcessor
from persistence.ProcessResults import ProcessResult

dask.config.set(scheduler='threads')

sys.setrecursionlimit(10 ** 6)
logging.basicConfig(level=logging.INFO)

Base = declarative_base()


class Processor:
    def __init__(self, file_path, process_id):
        self.file_path = file_path
        self.process_id = process_id
        self.engine = None  # Remove global variable

    def execute_process(self, remote_files, request_dto, ftp):
        logging.info("starting process %s", self.process_id)
        logging.info("starting to read csv file")

        chunk_size = 100000000

        updated_metadata = self.read_csv(remote_files, chunk_size, request_dto.get('connection_type'), ftp)

        logging.info("Completed")
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
                logging.info('Successfully connected to the PostgreSQL database')
        except Exception as ex:
            logging.error(f'Failed to connect: {ex}')

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
            lambda file: dask.delayed(self._process_file)(file, chunk_size, connection_type, ftp).compute()
        )
        combined_metadata = dask.compute(*delayed_tasks)

        return Processor.combine_metadata(combined_metadata)

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
                logging.warning("Unsupported connection type: %s", connection_type)
                return None

            data_frame = dd.read_csv(file, assume_missing=True, dtype='object', blocksize=block_size_bytes)
            logging.info("File reading completed")

            metadata = MetadataProcessor.generate_metadata(data_frame, 'schema_name', 'table_name')

            return metadata

        except UnicodeDecodeError as e:
            logging.error(f"Error decoding file: {file}. {e}")
            return None
        except Exception as e:
            logging.error(f"Error processing file: {file}. {e}", exc_info=True)
            return None

    def calculate_partitions(self, file_size, chunk_size):
        return (file_size + chunk_size - 1) // chunk_size

    @staticmethod
    def combine_metadata(updated_metadata):
        if updated_metadata is None:
            logging.warning("Updated metadata is None. Unable to combine.")
            return None

        combined_metadata = []
        for metadata in updated_metadata:
            if metadata is not None:
                metadata_dict = metadata.to_dict()
                combined_metadata.append(metadata_dict)

        return combined_metadata

    def updateMetadata(self, updated_metadata, metadata):
        if updated_metadata is None or metadata is None:
            logging.warning("Updated metadata or metadata is None. Unable to update.")
            return

        updated_metadata.row_count += metadata.row_count
        for column_name, column_metadata in metadata.columns.items():
            if column_name in updated_metadata.columns:
                updated_metadata.columns[column_name].distinct_row_count += column_metadata.distinct_row_count
                updated_metadata.columns[column_name].null_row_count += column_metadata.null_row_count
                updated_metadata.columns[column_name].type_length = max(
                    updated_metadata.columns[column_name].type_length,
                    column_metadata.type_length
                )
