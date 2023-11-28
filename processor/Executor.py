import json
import logging
import os
import time

import pyodbc
import sqlalchemy as sql
import dask.dataframe as dd
import duckdb as duckdb
import pandas as pd
from sqlalchemy import Column, Integer, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from bean.TableBean import TableBean
from bean.ColumnBean import ColumnBean

Base = declarative_base()

class ProcessResult(Base):
    __tablename__ = 'process_results'

    id = Column(Integer, primary_key=True)
    process_id = Column(Integer)
    json_data = Column(JSON)

class Execute:
    table: TableBean

    def __init__(self, file_path, process_id):
        self.file_path = file_path
        self.process_id = process_id
        self.table = TableBean

    def executeProcess(self, remote_files, request_dto):
        logging.info("starting process %s", self.process_id)
        logging.info("starting to read csv file")
        metadata_list = []
        dask_chunks = None
        if request_dto.get('connection_type') == 'SFTP':
            if remote_files:
                for remote_file in remote_files:
                    try:
                        file_size = remote_file.stat().st_size
                        chunk_size = 100000000
                        num_chunks = file_size // chunk_size + 1
                        dask_chunks = dd.read_csv(remote_file, blocksize=chunk_size, assume_missing=True)
                        for i in range(num_chunks):
                            chunk_data = dask_chunks.get_partition(i).compute()
                            metadata = self.generate_metadata(chunk_data, 'schema_name', 'table_name')
                            metadata_list.append(metadata)
                    except UnicodeDecodeError as e:
                        print(f"Error decoding file: {remote_file}. {e}")
                    except Exception as e:
                        print(f"Error processing file: {remote_file}. {e}")

        elif request_dto.get('connection_type') == 'LocalStorage':

            for remote_file in remote_files:
                try:
                    file_size = os.path.getsize(remote_file)
                    chunk_size = 100000000
                    num_chunks = file_size // chunk_size + 1
                    dask_chunks = dd.read_csv(remote_file, blocksize=chunk_size, dtype={'BIRTH_DATE': 'object',
                                                                                        'DATE_HIRED': 'object',
                                                                                        'TERMINATION_DATE': 'object'}, assume_missing=True)
                    for i in range(num_chunks):
                        chunk_data = dask_chunks.get_partition(i).compute()
                        metadata = self.generate_metadata(chunk_data, 'schema_name', 'table_name')
                        metadata_list.append(metadata)
                except UnicodeDecodeError as e:
                    print(f"Error decoding local file: {remote_file}. {e}")
                except Exception as e:
                    print(f"Error processing local file: {remote_file}. {e}")


        combined_metadata = self.combine_metadata(metadata_list)

        logging.info("File reading completed")
        logging.info("Total partition count is %s", dask_chunks.npartitions)
        logging.info("fetching record count, column count")
        logging.info("Completed")
        return combined_metadata

    def endProcess(self, json_list):
        output_format = "{time}_{process_id}.json"
        output_path = output_format.format(time=time.time(), process_id=self.process_id)
        user = 'postgres'
        password = 'postgres'
        host = 'localhost'
        port = '5434'
        database = 'postgres'
        schema = 'python'
        with open(output_path, 'w') as json_file:
            json.dump(json_list, json_file, indent=4)

        connection_str = f'postgresql:// {user}:{password}@{host}:{port}/{database}'
        engine = sql.create_engine(connection_str)
        try:
            with engine.connect() as connection_str:
                print('Successfully connected to the PostgreSQL database')
        except Exception as ex:
            print(f'Sorry failed to connect: {ex}')

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        process_result = ProcessResult(process_id=self.process_id, json_data=json_list)
        session.add(process_result)
        session.commit()

        print(self.table.to_dict())
        return output_path

    def generate_metadata(self, csv_data, schema_name, table_name):
        column_beans = []
        is_all_alphabet: bool

        for column in csv_data.columns:

            distinct_row_count = len(csv_data[column].unique())
            null_row_count = csv_data[column].isnull().sum().item()
            all_numeric = all(pd.to_numeric(csv_data[column], errors='coerce').notna())
            try:
                is_all_alphabet = all(isinstance(s, str) and s.isalpha() for s in csv_data[column].dropna())
            except AttributeError:
                is_all_alphabet = False

            unique_count = len(csv_data[column].unique())
            is_primary_key = unique_count == len(csv_data)

            is_date_column = False

            data_type = str(csv_data[column].dtypes)
            if data_type in ['object', 'string']:
                df = duckdb.from_df(csv_data)
                data_type = str(df[column].dtypes)
                if data_type in ['DATE', 'DATE_TIME', 'TIME']:
                    is_date_column = True
            try:
                is_length_uniform = csv_data[column].str.len().nunique() == 1
            except AttributeError:
                is_length_uniform = False
            type_length = csv_data[column].astype(str).apply(len).max()
            column_beans.append(
                ColumnBean(column, data_type, distinct_row_count, null_row_count, all_numeric, is_all_alphabet,
                           is_primary_key, is_date_column, is_length_uniform, int(type_length)))

        column_count = len(csv_data.columns)
        row_count = len(csv_data)

        table_bean = TableBean(column_count, row_count,
                               {col: col_bean for col, col_bean in zip(csv_data.columns, column_beans)}, schema_name,
                               table_name)

        return table_bean

    def combine_metadata(self, metadata_list):
        combined_metadata = {}

        for metadata in metadata_list:
            metadata = metadata.to_dict()
            combined_metadata.update(metadata)

        return combined_metadata

