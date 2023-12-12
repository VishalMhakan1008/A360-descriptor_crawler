import dask
import dask.bag as db
import dask.dataframe as dd
import json
import os
import psycopg2
import sqlalchemy as sql
import time
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

    def execute_process(self, csv_files, request_dto, ftp):
        metadata = []
        Processor.log_utility.log_info(f"Starting process {self.process_id}")
        Processor.log_utility.log_info("Starting to read CSV file")
        delayed_tasks = []

        try:
            files_bag = db.from_sequence(csv_files)
            delayed_tasks = files_bag.map(
                lambda file: dask.delayed(self.read_csv_file)(file, request_dto, ftp)
            )
        except Exception as e:
            Processor.log_utility.log_error(f"An error occurred in read_csv: {str(e)}")
            raise

        if delayed_tasks:
            try:
                computed_data_frame = dask.compute(*delayed_tasks)
                data_frames_list = list(computed_data_frame)
                combined_data_frame = dd.concat(data_frames_list, axis=0)
                Processor.log_utility.log_info("File reading completed")
                table_bean = request_dto.table_bean
                metadata.append(MetadataProcessor.generate_metadata(combined_data_frame, request_dto, table_bean).to_dict())

                #merge_metadata = Processor.merge_metadata(computed_data_list)
            except Exception as e:
                Processor.log_utility.log_error(f"An error occurred while computing delayed tasks: {str(e)}")
                raise
        else:
            Processor.log_utility.log_error("Delayed tasks list is empty")

        Processor.log_utility.log_info("Completed")
        return metadata

    @staticmethod
    def read_csv_file(file, request_dto, ftp):
        connection_type = request_dto.connection_dto.connection_type
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
            return data_frame
        except Exception as e:
            Processor.log_utility.log_error(f"Error processing file: {file}. {e}")
            return None


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


    def calculate_partitions(self, file_size, chunk_size):
        return (file_size + chunk_size - 1) // chunk_size

    @staticmethod
    def merge_metadata(computed_data_list):
        try:
            if not computed_data_list:
                Processor.log_utility.log_warning("List of updated metadata is empty. Unable to combine.")
                return None

            combined_metadata = [computed_data_list[0].to_dict()]

            for table_bean in computed_data_list[1:]:
                try:
                    if table_bean is not None:
                        for column in table_bean.columns.values():
                            column_name = column.column_name
                            column_found = False

                            for existing_table in combined_metadata:
                                for existing_column in existing_table["columns"]:
                                    if existing_column["column_name"] == column_name:
                                        existing_column["distinct_row_count"] += column.distinct_row_count
                                        existing_column["null_row_count"] += column.null_row_count
                                        existing_column["type_length"] = max(existing_column["type_length"], column.type_length)
                                        existing_column["max_whitespace_count"] += column.max_whitespace_count
                                        column_found = True
                                        break

                                if not column_found:
                                    new_column = {
                                        'column_name': column.column_name,
                                        'distinct_row_count': column.distinct_row_count,
                                        'null_row_count': column.null_row_count,
                                        'all_numeric': column.all_numeric,
                                        'all_alphabet': column.all_alphabet,
                                        'primary_key': column.primary_key,
                                        'is_date_column': column.is_date_column,
                                        'is_length_uniform': column.is_length_uniform,
                                        'type_length': column.type_length,
                                        'is_unstructured': column.is_unstructured,
                                        'is_unique_key': column.is_unique_key,
                                        'probable_primary': column.probable_primary,
                                        'contains_digit': column.contains_digit,
                                        'max_whitespace_count': column.max_whitespace_count,
                                        'probable_primary_for_crawl': column.probable_primary_for_crawl
                                    }

                                    existing_table["columns"].append(new_column)
                                    existing_table["column_count"] += 1

                        # Update other metadata for the table
                        existing_table["row_count"] = table_bean.row_count
                        existing_table["probable_primary_key_size"] = table_bean.probable_primary_key_size
                        existing_table["primary_key_size"] = table_bean.primary_key_size

                except Exception as table_bean_error:
                    Processor.log_utility.log_error(f"An error occurred while processing TableBean: {str(table_bean_error)}")

        except Exception as merge_metadata_error:
            Processor.log_utility.log_error(f"An error occurred in merge_metadata: {str(merge_metadata_error)}")
            return None

        return_data = combined_metadata
        return return_data


