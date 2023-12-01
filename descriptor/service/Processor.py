import json
import logging
import os
import time
import psycopg2
import sqlalchemy as sql
import dask.dataframe as dd
import duckdb as duckdb
import pandas as pd
import dask.bag as db
import dask
import sys

from sqlalchemy.orm import sessionmaker

from persistence.ProcessResults import ProcessResult
from descriptor.bean.TableBean import TableBean
from descriptor.bean.ColumnBean import ColumnBean
from sqlalchemy.ext.declarative import declarative_base

dask.config.set(scheduler='threads')

sys.setrecursionlimit(10**6)
logging.basicConfig(level=logging.INFO)

Base = declarative_base()

class Processor:

    def __init__(self, file_path, process_id):
        self.file_path = file_path
        self.process_id = process_id
        self.engine = None  # Remove global variable

    def execute_process(self, remote_files, request_dto):
        logging.info("starting process %s", self.process_id)
        logging.info("starting to read csv file")

        chunk_size = 100000000

        dask_chunks, updated_metadata = self.read_csv(remote_files, chunk_size, request_dto.get('connection_type'))

        combined_metadata = self.combine_metadata(updated_metadata)

        logging.info("Completed")
        return combined_metadata

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

    @staticmethod
    def generate_metadata(csv_data, schema_name, table_name):
        if csv_data is None:
            logging.error("CSV data is None. Unable to generate metadata.")
            return None

        column_beans = []
        is_all_alphabet: bool

        try:
            for column in csv_data.columns:
                distinct_row_count = len(csv_data[column].unique())
                null_row_count = csv_data[column].isnull().sum().item()
                all_numeric = all(pd.to_numeric(csv_data[column], errors='coerce').notna())
                is_all_alphabet = all(isinstance(s, str) and s.isalpha() for s in csv_data[column].dropna())

                unique_count = len(csv_data[column].unique())
                is_primary_key = unique_count == len(csv_data)

                is_date_column = False

                data_type = str(csv_data[column].dtypes)
                if data_type in ['object', 'string']:
                    df = duckdb.from_df(csv_data)
                    data_type = str(df[column].dtypes)
                    if data_type in ['DATE', 'DATE_TIME', 'TIME']:
                        is_date_column = True

                is_length_uniform = csv_data[column].str.len().nunique() == 1
                type_length = csv_data[column].astype(str).apply(len).max()
                column_beans.append(
                    ColumnBean(column, data_type, distinct_row_count, null_row_count, all_numeric, is_all_alphabet,
                               is_primary_key, is_date_column, is_length_uniform, int(type_length)))
        except Exception as e:
            logging.error(f"Error generating metadata: {e}", exc_info=True)
            return None

        column_count = len(csv_data.columns)
        row_count = len(csv_data)

        table_bean = TableBean(column_count, row_count,
                               {col: col_bean for col, col_bean in zip(csv_data.columns, column_beans)}, schema_name,
                               table_name)

        return table_bean

    def read_csv(self, files, chunk_size, connection_type):
        dask_bag_files = db.from_sequence(files)
        dask_chunks, updated_metadata = dask_bag_files.map(self._process_file, chunk_size=chunk_size, connection_type=connection_type).compute(scheduler='threads')
        return dask_chunks, updated_metadata

    def _process_file(self, file, chunk_size, connection_type):
        try:
            if connection_type == 'SFTP':
                file_size = file.stat().st_size
            elif connection_type == 'Local Storage':
                file_size = os.path.getsize(file)
            else:
                logging.warning("Unsupported connection type: %s", connection_type)
                return None, None

            num_chunks = file_size // chunk_size + 1

            delayed_dask_chunks = dask.delayed(dd.read_csv)(file, blocksize=chunk_size, assume_missing=True, dtype='object')
            logging.info("File reading completed")

            dask_chunks = delayed_dask_chunks.compute(scheduler='threads')

            bag = db.from_sequence(range(num_chunks), npartitions=num_chunks)

            delayed_metadata_list = bag.map(lambda i: self._process_chunk(i, dask_chunks))

            metadata_list = delayed_metadata_list.compute(scheduler='threads')

            updated_metadata = None
            for metadata in metadata_list:
                if metadata is not None:
                    if updated_metadata is None:
                        updated_metadata = metadata
                    else:
                        self.updateMetadata(updated_metadata, metadata)

            return dask_chunks, updated_metadata
        except UnicodeDecodeError as e:
            logging.error(f"Error decoding file: {file}. {e}")
            return None, None
        except Exception as e:
            logging.error(f"Error processing file: {file}. {e}", exc_info=True)
            return None, None




    def _process_chunk(self, i, delayed_dask_chunks):
        delayed_chunk_data = delayed_dask_chunks.get_partition(i)
        return dask.delayed(self.generate_metadata)(delayed_chunk_data, 'schema_name', 'table_name')

    def combine_metadata(self, updated_metadata):
        if updated_metadata is None:
            logging.warning("Updated metadata is None. Unable to combine.")
            return None

        combined_metadata = []
        for metadata in updated_metadata:
            if metadata is not None:
                metadata_dict = metadata.to_dict()
                combined_metadata.append(metadata_dict)

        return combined_metadata

    @staticmethod
    def updateMetadata(updated_metadata, metadata):
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
